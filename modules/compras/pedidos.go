package compras

import (
	"MGFSiga/connection"
	"MGFSiga/modules"
	"fmt"
	"time"

	"github.com/vbauerster/mpb"
)

func Cadped(p *mpb.Progress) {
	modules.LimpaTabela("cadped")
	cnxFdb, err := connection.ConexaoDestino()
	if err != nil {
		panic("Falha ao conectar com o banco de destino: " + err.Error())
	}
	defer cnxFdb.Close()

	cnxSqls, err := connection.ConexaoOrigem()
	if err != nil {
		panic("Falha ao conectar com o banco de origem: " + err.Error())
	}
	defer cnxSqls.Close()

	tx, err := cnxFdb.Begin()
	if err != nil {
		fmt.Printf("erro ao iniciar transação: %v", err)
	}
	defer tx.Commit()

	insert, err := tx.Prepare(`insert into cadped (numped, num, ano, datped, codif, entrou, id_cadped, empresa, numlic, obs, codccusto) values (?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		fmt.Printf("erro ao preparar insert: %v", err)
	}

	query := `select
		right(replicate('0',
		5)+ cast(numeroAutorizacao as varchar),
		5)+ '/' + cast(anoAutorizacao%2000 as varchar) numped,
		right(replicate('0',
		5)+ cast(numeroAutorizacao as varchar),
		5) num,
		anoAutorizacao,
		ade.dataAutorizacao,
		cgc_cpf,
		concat(numeroAutorizacao, anoAutorizacao%2000) id_cadped,
		'N' entrou,
		right(replicate('0',
			5) + cast(numeroProcessoDeCompra as varchar),
			5) + '/' + cast(anoProcessoDeCompra % 2000 as varchar) as numorc,
		'Autorização nº ' + right(replicate('0',
		5)+ cast(numeroAutorizacao as varchar),
		5)+ '/' + cast(anoAutorizacao%2000 as varchar) obs
	from
		MGFSiga.dbo.AutorizacaoDeEmpenho ade`

	rows, err := cnxSqls.Query(query)
	if err != nil {
		fmt.Printf("erro ao executar query: %v", err)
	}

	totalLinhas, err := modules.CountRows(query)
	if err != nil {
		fmt.Printf("erro ao contar linhas: %v", err)
	}

	barCadped := modules.NewProgressBar(p, totalLinhas, "CADPED")

	for rows.Next() {
		var (
			numped, num, insmf, id_cadped, entrou, obs, dataAutorizacao, numorc string
			anoAutorizacao int
		)
		empresa := modules.Cache.Empresa
		
		err = rows.Scan(&numped, &num, &anoAutorizacao, &dataAutorizacao, &insmf, &id_cadped, &entrou, &numorc, &obs)
		if err != nil {
			fmt.Printf("erro ao fazer scan: %v", err)
		}

		dataParseada, err := time.Parse(time.RFC3339, dataAutorizacao)
		if err != nil {
			fmt.Printf("erro ao parsear string para data: %v", err)
		}

		dataFormatada := dataParseada.Format("02.01.2006")
		codif := modules.Cache.Codif[insmf]
		numlic := modules.Cache.NumlicAtravesDaNumorc[numorc]
		obsConvertidoWin1252, err := modules.DecodeToWin1252(obs) 
		if err != nil {
			fmt.Printf("erro ao converter obs para win1252: %v", err)
		}

		_, err = insert.Exec(numped, num, anoAutorizacao, dataFormatada, codif, entrou, id_cadped, empresa, numlic, obsConvertidoWin1252, 0)
		if err != nil {
			fmt.Printf("erro ao executar insert: %v", err)
		}
		barCadped.Increment()
	}
}

func Icadped(p *mpb.Progress) {
	modules.LimpaTabela("Icadped")
	cnxFdb, err := connection.ConexaoDestino()
	if err != nil {
		panic("Falha ao conectar com o banco de destino: " + err.Error())
	}
	defer cnxFdb.Close()

	cnxSqls, err := connection.ConexaoOrigem()
	if err != nil {
		panic("Falha ao conectar com o banco de origem: " + err.Error())
	}
	defer cnxSqls.Close()

	tx, err := cnxFdb.Begin()
	if err != nil {
		fmt.Printf("erro ao iniciar transação: %v", err)
	}
	defer tx.Commit()

	insert, err := tx.Prepare("insert into icadped (numped, item, cadpro, qtd, prcunt, prctot, codccusto, id_cadped) values (?,?,?,?,?,?,?,?)")
	if err != nil {
		fmt.Printf("erro ao preparar insert: %v", err)
	}

	query := `select
		right(replicate('0',
			5)+ cast(numeroAutorizacao as varchar),
			5)+ '/' + cast(anoAutorizacao%2000 as varchar) numped,
			idItem,
			idEspecificacao,
			idade.quantidadeAFornecer,
			idade.valorUnitario,
			idade.quantidadeAFornecer * idade.valorUnitario prctot,
			0 codccusto,
			concat(numeroAutorizacao, anoAutorizacao%2000) id_cadped
	from
		MGFSiga.dbo.ItemDeAutorizacaoDeEmpenho idade`

	rows, err := cnxSqls.Query(query)
	if err != nil {
		fmt.Printf("erro ao executar query: %v", err)
	}

	totalLinhas, err := modules.CountRows(query)
	if err != nil {
		fmt.Printf("erro ao contar linhas: %v", err)
	}

	barIcadped := modules.NewProgressBar(p, totalLinhas, "ICADPED")

	for rows.Next() {
		var (
			numped, idEspecificacao, id_cadped string
			item, codccusto int
			qtd, prcunt, prctot float64
		)

		err = rows.Scan(&numped, &item, &idEspecificacao, &qtd, &prcunt, &prctot, &codccusto, &id_cadped)
		if err != nil {
			fmt.Printf("erro ao fazer scan: %v", err)
		}

		cadpro := modules.Cache.Itens[idEspecificacao]

		_, err = insert.Exec(numped, item, cadpro, qtd, prcunt, prctot, codccusto, id_cadped)
		if err != nil {
			fmt.Printf("erro ao executar insert: %v", err)
		}
		barIcadped.Increment()
	}
}