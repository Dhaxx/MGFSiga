package compras

import (
	"MGFSiga/connection"
	"MGFSiga/modules"
	"fmt"
	// "time"

	"github.com/vbauerster/mpb"
)

func Requi(p *mpb.Progress) {
	modules.LimpaTabela("icadreq where id_requi = 0")
	modules.LimpaTabela("requi where id_requi = 0")
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

	insertRequi, err := tx.Prepare(`INSERT
			INTO
			requi (empresa,
			id_requi,
			requi,
			num,
			ano,
			destino,
			codccusto,
			datae,
			dtlan,
			entr,
			said,
			comp,
			codif)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		fmt.Printf("erro ao preparar insert: %v", err)
	}

	insertIcadreq, err := tx.Prepare(`insert into icadreq (id_requi, requi, codccusto, empresa, item, quan1, quan2, vaun1, vaun2, vato1, vato2, cadpro, destino) values (?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		fmt.Printf("erro ao preparar insert: %v", err)
	}

	query := `select
		*,
		row_number() over (order by idMaterial) item,
		case when quan1 <> 0 then valorSaldo / quan1 else 0 end valUni,
		1280 codif
	from
		(
		select
			0 idRequi,
			'000000/24' requi,
			'000000' num,
			2024 anorequi,
			right(replicate('0',
			9)+ IdCCusto,
			9) destino,
			cast(IdCCusto as int) codccusto,
			'2024-01-01' data,
			'S' entr,
			'S' said,
			'P' comp,
			idMaterial,
			(e.QuantidadeAnterior + e.QuantidadeEntradas + e.QuantidadeEntradasTransferencia) quan1,
			(e.QuantidadeSaidas + e.QuantidadeSaidasTransferencia) quan2,
			e.ValorSaldo
		from
			MGFEstoq.dbo.Estoque e) as subquery`
	
	totalLinhas, err := modules.CountRows(query)
	if err != nil {
		fmt.Printf("erro ao contar linhas: %v", err)
	}

	barRequi := modules.NewProgressBar(p, totalLinhas, "REQUI")

	cabecalhos, err := cnxSqls.Query(fmt.Sprintf("select distinct idRequi, requi, num, anorequi, '000000000' destino, 0 codccusto, data, entr, said, comp, codif from (%v) subquery", query))
	if err != nil {
		fmt.Printf("erro ao obter cabecalhos: %v", err)
	}
	
	for cabecalhos.Next() {
		var(
			requi, num, destino, entr, said, comp, dtlan string
			idRequi, ano, codccusto, codif int
		)

		if err := cabecalhos.Scan(&idRequi, &requi, &num, &ano, &destino, &codccusto, &dtlan, &entr, &said, &comp, &codif); err != nil {
			fmt.Printf("erro ao scanear valores: %v", err)
		}

		// dataParseada, err := time.Parse(time.RFC3339, dtlan)
		// if err != nil {
		// 	fmt.Printf("erro ao parsear string para data: %v", err)
		// }
		// dtlanFormatada := dataParseada.Format("2006-01-02")

		if _, err := insertRequi.Exec(modules.Cache.Empresa, idRequi, requi, num, ano, destino, codccusto, dtlan, dtlan, entr, said, comp, codif); err != nil {
			fmt.Printf("erro ao executar insert: %v", err)
		}
		barRequi.Increment()
	}

	itens, err := cnxSqls.Query(fmt.Sprintf("select distinct idRequi, requi, codccusto, item, quan1, quan2, valUni, quan1 * valUni vato1, quan2*valUni vato2, idMaterial, destino from (%v) subquery",query))
	if err != nil {
		fmt.Printf("erro ao executar query: %v", err)
	}

	for itens.Next() {
		var (
			idRequisicao, codccusto, item int
			requi, destino, idMaterial string
			quan1, quan2, vaun, vato1, vato2 float64
		)

		err = itens.Scan(&idRequisicao, &requi, &codccusto, &item, &quan1, &quan2, &vaun, &vato1, &vato2, &idMaterial, &destino)
		if err != nil {
			fmt.Printf("erro ao fazer scan: %v", err)
		}

		cadpro := modules.Cache.Itens[idMaterial]

		if _, err := insertIcadreq.Exec(idRequisicao, requi, codccusto, modules.Cache.Empresa, item, quan1, quan2, vaun, vaun, vato1, vato2, cadpro, destino); err != nil {
			fmt.Printf("erro ao executar insert: %v", err)
		}
	}
	barRequi.Completed()
}