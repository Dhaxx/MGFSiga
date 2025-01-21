package compras

import (
	"MGFSiga/connection"
	"MGFSiga/modules"
	"fmt"
	"time"

	"github.com/gobuffalo/nulls"
	"github.com/vbauerster/mpb"
)

func Cadorc(p *mpb.Progress) {
	modules.LimpaTabela("cadorc")
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

	insert, err := tx.Prepare(`insert
		into
		cadorc (id_cadorc,
		num,
		ano,
		numorc,    
		dtorc,
		descr,  
		prioridade,
		status,
		liberado,
		codccusto,
		liberado_tela,
		empresa,
		solicitante,
		numlic) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		fmt.Printf("erro ao preparar insert: %v", err)
	}

	query := `select 
		row_number() over (
		order by anoProcesso) id_cadorc,
		*
	from
		(
		select
			distinct
		*
		from
			(
			select
				distinct
			c.numeroProcesso,
				c.anoProcesso,
				right(replicate('0',
				5)+ cast(c.numeroProcesso as varchar),
				5)+ '/' + cast(c.anoProcesso%2000 as varchar) as numorc,
				c.dataHoraConsulta dtorc,
				cast(pdc.objeto as nvarchar(MAX)) descr,
				'NORMAL' prioridade,
				case
					when pdc.numeroLicitacao <> 0 then 'LC'
					else 'AP'
				end status,
				case
					when pdc.numeroLicitacao <> 0 then 'S'
					else 'N'
				end liberado,
				0 as codccusto,
				case
					when pdc.numeroLicitacao <> 0 then 'L'
				end liberado_tela,
				rtrim(pf.nome) solicitante,
				concat(pdc.numeroLicitacao, pdc.anoLicitacao) numlic
			from
				dbo.Cotacao c
			join ProcessoDeCompra pdc on
				c.numeroProcesso = pdc.numero
				and c.anoProcesso = pdc.ano
				--and pdc.tipoDeProcesso <> 2
			left join MGFSiga.dbo.PessoaFisica pf on
				pdc.cpfResponsavel = pf.cpf
		union all
			select
				distinct
			crdp.numeroProcessoDeCompra,
				crdp.anoProcessoDeCompra,
				right(replicate('0',
				5)+ cast(crdp.numeroProcessoDeCompra as varchar),
				5)+ '/' + cast(crdp.anoProcessoDeCompra%2000 as varchar) as numorc,
				crdp.dataDaPesquisa dtorc,
				cast(pdc.objeto as nvarchar(MAX)),
				'NORMAL' prioridade,
				case
					when pdc.numeroLicitacao <> 0 then 'LC'
					else 'AP'
				end status,
				case
					when pdc.numeroLicitacao <> 0 then 'S'
					else 'N'
				end liberado,
				0 as codccusto,
				case
					when pdc.numeroLicitacao <> 0 then 'L'
				end liberado_tela,
				rtrim(pf.nome) solicitante,
				concat(pdc.idModalidade, pdc.numeroLicitacao, pdc.anoLicitacao) numlic
			from
				dbo.CotacaoRegistroDePreco crdp
			join ProcessoDeCompra pdc on
				crdp.numeroProcessoDeCompra = pdc.numero
				and crdp.anoProcessoDeCompra = pdc.ano 
				and pdc.tipoDeProcesso = 2
			left join MGFSiga.dbo.PessoaFisica pf on
				pdc.cpfResponsavel = pf.cpf) as rn) as subquery`
	
	rows, err := cnxSqls.Query(query)
	if err != nil {
		fmt.Printf("erro ao executar consulta: %v", err)
	}

	totalLinhas, err := modules.CountRows(query)
	if err != nil {
		fmt.Printf("erro ao obter total de linhas: %v", err)
	}

	barCadorc := modules.NewProgressBar(p, totalLinhas, "CADORC")

	for rows.Next() {
		var (
			idCadorc, codccusto, numlic nulls.Int
			num, ano, numorc, dtorc, descr, prioridade, status, liberado, liberadoTela, solicitante nulls.String
			empresa = modules.Cache.Empresa
		)

		if err := rows.Scan(&idCadorc, &num, &ano, &numorc, &dtorc, &descr, 
		&prioridade, &status, &liberado, &codccusto, &liberadoTela, &solicitante, &numlic); err != nil {
			fmt.Printf("erro ao scanear valores: %v", err)
		}

		descricaoConvertidoWin1252, err := modules.DecodeToWin1252(descr.String)
		if err != nil {
			fmt.Printf("erro ao decodificar: %v", err)
		}

		dataParseada, err := time.Parse(time.RFC3339, dtorc.String)
		if err != nil {
			fmt.Printf("erro ao parsear string para data: %v", err)
		}

		dataFormatada := dataParseada.Format("02.01.2006")

		if _, err := insert.Exec(idCadorc, num, ano, numorc, dataFormatada, descricaoConvertidoWin1252, prioridade, status, liberado, codccusto, liberadoTela, empresa,
		solicitante, numlic); err != nil {
			fmt.Printf("erro ao inserir registro: %v", err)
		}

		barCadorc.Increment()
	}
}

func Icadorc(p *mpb.Progress) {
	modules.LimpaTabela("icadorc")

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

	insert, err := tx.Prepare(`insert into icadorc (numorc, item, cadpro, qtd, valor, itemorc, codccusto, itemorc_ag, id_cadorc) values (?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		fmt.Printf("erro ao preparar insert: %v", err)
	}

	query := `select
		right(replicate('0',
		5)+ cast(idc.numeroProcesso as varchar),
		5)+ '/' + cast(idc.anoProcesso%2000 as varchar) as numorc,
		row_number() over (partition by numeroProcesso, anoProcesso order by numeroProcesso, anoProcesso, idEspecificacao) item, 
		idc.idEspecificacao,
		idc.quantidadeAComprar,
		idc.precoUnitario,
		0 codccusto
	from
		MGFSiga.dbo.ItemDeCotacao idc`

	rows, err := cnxSqls.Query(query)
	if err != nil {
		fmt.Printf("erro ao executar query: %v", err)
	}

	totalLinhas, err := modules.CountRows(query)
	if err != nil {
		fmt.Printf("erro ao contar total de linhas: %v", err)
	}

	barIcadorc := modules.NewProgressBar(p, totalLinhas, "ICADORC")

	for rows.Next() {
		var (
			numorc, idEspecificacao string
			item, codccusto int
			quantidade, precoUnitario float32
		)

		if err := rows.Scan(&numorc, &item, &idEspecificacao, &quantidade, &precoUnitario, &codccusto); err != nil {
			fmt.Printf("erro ao scanear valores: %v", err)
		}

		cadpro := modules.Cache.Itens[idEspecificacao]
		idCadorc := modules.Cache.IdCadorc[numorc]

		if _, err := insert.Exec(numorc, item, cadpro, quantidade, precoUnitario, item, codccusto, item, idCadorc); err != nil {
			fmt.Printf("erro ao inserir registros: %v", err)
		}
		barIcadorc.Increment()
	}
}

func Fcadorc(p *mpb.Progress) {
	modules.LimpaTabela("Fcadorc")

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

	insert, err := tx.Prepare("insert into fcadorc(numorc,codif, nome, valorc, id_cadorc) values (?,?,?,?,?)")
	if err != nil {
		fmt.Printf("erro ao preparar insert: %v", err)
	}

	query := `select
		right(replicate('0',
		5) + cast(idc.numeroProcesso as varchar),
		5) + '/' + cast(idc.anoProcesso % 2000 as varchar) as numorc,
		cgc_cpfFornecedor,
		sum(quantidadeAComprar * precoUnitario) as total
	from
		itemDeCotacao idc
	group by
		right(replicate('0',
		5) + cast(idc.numeroProcesso as varchar),
		5) + '/' + cast(idc.anoProcesso % 2000 as varchar),
		cgc_cpfFornecedor`

	rows, err := cnxSqls.Query(query)
	if err != nil {
		fmt.Printf("erro ao executar consulta: %v", err)
	}

	totalLinhas, err := modules.CountRows(query)
	if err != nil {
		fmt.Printf("erro ao obter total de linhas")
	}

	barFcadorc := modules.NewProgressBar(p, totalLinhas, "FCADORC")

	for rows.Next() {
		var (
			numorc, insmf string
			valorc float32
		)
		
		if err := rows.Scan(&numorc, &insmf, &valorc); err != nil {
			fmt.Printf("erro ao scanear valores: %v", err)
		}

		nome := modules.Cache.NomeForn[insmf]
		codif := modules.Cache.Codif[insmf]
		idCadorc := modules.Cache.IdCadorc[numorc]

		if _, err := insert.Exec(numorc, codif, nome, valorc, idCadorc); err != nil {
			fmt.Printf("erro ao inserir registro: %v", err)
		}

		barFcadorc.Increment()
	}
}

func Vcadorc(p *mpb.Progress) {
	modules.LimpaTabela("Vcadorc")

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

	insert, err := tx.Prepare("insert into vcadorc(numorc, item, codif, vlruni, vlrtot, id_cadorc) values (?,?,?,?,?,?)")
	if err != nil {
		fmt.Printf("erro ao preparar insert: %v", err)
	}

	query := `select
			right(replicate('0',
			5)+ cast(idc.numeroProcesso as varchar),
			5)+ '/' + cast(idc.anoProcesso%2000 as varchar) as numorc,
			row_number() over (partition by numeroProcesso,
		anoProcesso
	order by
		numeroProcesso,
		anoProcesso,
		idEspecificacao) item, 
			idc.CGC_CPFFornecedor,
			idc.precoUnitario,
			idc.quantidadeAComprar * idc.precoUnitario valorTotal
	from
			MGFSiga.dbo.ItemDeCotacao idc`

	rows, err := cnxSqls.Query(query)
	if err != nil {
		fmt.Printf("erro ao executar query: %v", err)
	}

	totalLinhas, err := modules.CountRows(query)
	if err != nil {
		fmt.Printf("erro ao obter total de registros: %v", err)
	}

	barVcadorc := modules.NewProgressBar(p, totalLinhas, "VCADORC")
	
	for rows.Next() {
		var (
			numorc, insmf string
			item int
			vlrUni, vlrTot float32
		)

		if err := rows.Scan(&numorc, &item, &insmf, &vlrUni, &vlrTot); err != nil {
			fmt.Printf("erro ao scanear valores: %v", err)
		}

		codif := modules.Cache.Codif[insmf]
		idCadorc := modules.Cache.IdCadorc[numorc]

		if _, err = insert.Exec(numorc, item, codif, vlrUni, vlrTot, idCadorc); err != nil {
			fmt.Printf("erro ao inserir registro: %v", err)
		}
		barVcadorc.Increment()
	}
	tx.Commit()

	cnxFdb.Exec(`UPDATE VCADORC SET GANHOU = CODIF, VLRGANHOU = VLRUNI`)
}