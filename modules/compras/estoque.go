package compras

import (
	"MGFSiga/connection"
	"MGFSiga/modules"
	"fmt"
	"time"

	"github.com/vbauerster/mpb"
)

func Requi(p *mpb.Progress) {
	modules.LimpaTabela("icadreq")
	modules.LimpaTabela("requi")
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
			obs,
			codif)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		fmt.Printf("erro ao preparar insert: %v", err)
	}

	insertIcadreq, err := tx.Prepare(`insert into icadreq (id_requi, requi, codccusto, empresa, item, quan1, quan2, vaun1, vaun2, vato1, vato2, cadpro, destino) values (?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		fmt.Printf("erro ao preparar insert: %v", err)
	}

	query := `select
			idRequisicao,
			format(sea.IdRequisicao,
			'000000')+ '/' + cast(anoRequisicao%2000 as varchar) requi,
			format(sea.IdRequisicao,
			'000000') num,
			AnoRequisicao ano,
			right(replicate('0',
			9)+ sea.IdCCusto,
			9) destino,
			cast(sea.IdCCusto as int) codccusto,
			sea.DataBMDA,
			'S' entr,
			'N' said,
			'P' comp,
			descricaoNatureza obs,
			sea.CGCFornecedor,
			row_number() over (partition by idRequisicao,
				anoRequisicao
		order by
				idRequisicao,
				anoRequisicao, 
				IdMaterial) item,
			sea.QuantidadeMovimentada quan1,
			0 quan2,
			sea.ValorMovimentado / sea.QuantidadeMovimentada vaun1,
			0 vaun2,
			sea.ValorMovimentado vato1,
			0 vato2,
			IdMaterial
		from
			MGFEstoq.dbo.SQL_EntAnt sea
		union all
		select
			idRequisicao,
			format(se.IdRequisicao,
			'000000')+ '/' + cast(anoRequisicao%2000 as varchar) requi,
			format(se.IdRequisicao,
			'000000') num,
			AnoRequisicao,
			right(replicate('0',
			9)+ se.IdCCusto,
			9) destino,
			cast(se.IdCCusto as int) codccusto,
			se.DataBMDA,
			'S' entr,
			'N' said,
			'P' comp,
			descricaoNatureza obs,
			se.CGCFornecedor,
			row_number() over (partition by idRequisicao,
				anoRequisicao
		order by
				idRequisicao,
				anoRequisicao, 
				IdMaterial) item,
			se.QuantidadeMovimentada quan1,
			0 quan2,
			se.ValorMovimentado / se.QuantidadeMovimentada vaun1,
			0 vaun2,
			se.ValorMovimentado vato1,
			0 vato2,
			IdMaterial
		from
			MGFEstoq.dbo.SQL_Ent se
		union all
		select
			idRequisicao,
			format(IdRequisicao,
			'000000')+ '/' + cast(anoRequisicao%2000 as varchar) requi,
			format(IdRequisicao,
			'000000') num,
			AnoRequisicao,
			right(replicate('0',
			9)+ DescricaoCCustoDestino,
			9) destino,
			cast(DescricaoCCustoDestino as int) codccusto,
			DataBMDA,
			'N' entr,
			'S' said,
			'P' comp,
			descricaoNatureza obs,
			'0' codif,
			row_number() over (partition by idRequisicao,
				anoRequisicao
		order by
				idRequisicao,
				anoRequisicao, 
				IdMaterial) item,
			0 quan1,
			ssa.QuantidadeMovimentada quan2,
			0 vaun1,
			ssa.ValorMovimentado / ssa.QuantidadeMovimentada vaun2,
			0 vato1,
			ssa.ValorMovimentado vato2,
			ssa.IdMaterial
		from
			MGFEstoq.dbo.SQL_SaiAnt ssa
		union all
		select
			idRequisicao,
			format(IdRequisicao,
			'000000')+ '/' + cast(anoRequisicao%2000 as varchar) requi,
			format(IdRequisicao,
			'000000') num,
			AnoRequisicao,
			right(replicate('0',
			9)+ DescricaoCCustoDestino,
			9) destino,
			cast(DescricaoCCustoDestino as int) codccusto,
			DataBMDA,
			'N' entr,
			'S' said,
			'P' comp,
			descricaoNatureza obs,
			'0' codif,
			row_number() over (partition by idRequisicao,
				anoRequisicao
		order by
				idRequisicao,
				anoRequisicao,
				IdMaterial) item,
			0 quan1,
			QuantidadeMovimentada quan2,
			0 vaun1,
			ValorMovimentado / QuantidadeMovimentada vaun2,
			0 vato1,
			ValorMovimentado vato2,
			IdMaterial
		from
			MGFEstoq.dbo.SQL_Sai`
	
	totalLinhas, err := modules.CountRows(query)
	if err != nil {
		fmt.Printf("erro ao contar linhas: %v", err)
	}

	barRequi := modules.NewProgressBar(p, totalLinhas, "REQUI")

	cabecalhos, err := cnxSqls.Query(fmt.Sprintf("select distinct idRequisicao, requi, num, ano, destino, codccusto, databmda, entr, said, comp, obs, cgcfornecedor from (%v) subquery", query))
	if err != nil {
		fmt.Printf("erro ao obter cabecalhos: %v", err)
	}
	
	for cabecalhos.Next() {
		var(
			requi, num, destino, entr, said, comp, obs, insmf, dtlan string
			idRequi, ano, codccusto, codif int
		)

		if err := cabecalhos.Scan(&idRequi, &requi, &num, &ano, &destino, &codccusto, &dtlan, &entr, &said, &comp, &obs, &insmf); err != nil {
			fmt.Printf("erro ao scanear valores: %v", err)
		}

		codif = modules.Cache.Codif[insmf]

		dataParseada, err := time.Parse(time.RFC3339, dtlan)
		if err != nil {
			fmt.Printf("erro ao parsear string para data: %v", err)
		}
		dtlanFormatada := dataParseada.Format("2006-01-02")

		if _, err := insertRequi.Exec(modules.Cache.Empresa, idRequi, requi, num, ano, destino, codccusto, dtlanFormatada, dtlanFormatada, entr, said, comp, obs, codif); err != nil {
			fmt.Printf("erro ao executar insert: %v", err)
		}
		barRequi.Increment()
	}

	itens, err := cnxSqls.Query(fmt.Sprintf("select distinct idRequisicao, requi, codccusto, item, quan1, quan2, vaun1, vaun2, vato1, vato2, idMaterial, destino from (%v) subquery",query))
	if err != nil {
		fmt.Printf("erro ao executar query: %v", err)
	}

	for itens.Next() {
		var (
			idRequisicao, codccusto, item int
			requi, destino, idMaterial string
			quan1, quan2, vaun1, vaun2, vato1, vato2 float64
		)

		err = itens.Scan(&idRequisicao, &requi, &codccusto, &item, &quan1, &quan2, &vaun1, &vaun2, &vato1, &vato2, &idMaterial, &destino)
		if err != nil {
			fmt.Printf("erro ao fazer scan: %v", err)
		}

		cadpro := modules.Cache.Itens[idMaterial]

		if _, err := insertIcadreq.Exec(idRequisicao, requi, codccusto, modules.Cache.Empresa, item, quan1, quan2, vaun1, vaun2, vato1, vato2, cadpro, destino); err != nil {
			fmt.Printf("erro ao executar insert: %v", err)
		}
	}
	barRequi.Completed()
}