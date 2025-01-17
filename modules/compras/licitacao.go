package compras

import (
	"MGFSiga/connection"
	"MGFSiga/modules"
	"fmt"
	"time"

	"github.com/vbauerster/mpb"
)

func Cadlic(p *mpb.Progress) {
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

	insert, err := tx.Prepare(`insert into cadlic (numpro,
		datae,
		dtpub,
		dtenc,
		horabe,
		discr,
		discr7,
		modlic,
		dthom,
		dtadj,
		comp,
		numero,
		registropreco, 
		ctlance,
		obra,
		proclic,
		numlic,
		microempresa,
		ano,
		licnova,
		tlance,
		mult_entidade,
		processo_ano,
		lei_invertfasetce,
		valor1,
		detalhe,
		discr9,
		liberacompra,
		numorc) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		fmt.Printf("erro ao preparar insert: %v", err)
	}

	query := `select
		pdl.numeroLicitacao,
		pdc.dataDoProcesso datae,
		pdc.dataDoProcesso dtpub,
		pdl.dataDoTermoDeRatificacao dtenc,
		'09:00' horabe,
		cast(pdc.objeto as nvarchar(MAX)) discr,
		cast(cdj.descricao as nvarchar(MAX)) discr7,
		case
			when pdl.idModalidade in (0, 1, 2, 7, 9, 10) then 'DI01'
			--DISPENSA	
			when pdl.idModalidade = 3 then 'IN01'
			--INEXIGIBILIDADE
			when pdl.idModalidade = 4 then 'CONV'
			--CONVITE
			when pdl.idModalidade = 5 then 'TOM3'
			--TOMADA DE PREÇOS
			when pdl.idModalidade = 6 then 'CON4'
			--CONCORRÊNCIA
			when pdl.idModalidade = 8 then 'PP01'
			--PREGÃO PRESENCIAL
			when pdl.idModalidade = 11 then 'LEIL'
			--PREGÃO PRESENCIAL
			when pdl.idModalidade = 12 then 'PE01'
			--PREGÃO PRESENCIAL
		end modlic,
		pdl.dataDoTermoDeRatificacao dthom,
		pdl.dataDoTermoDeRatificacao dtadj,
		3 comp,
		right(replicate('0',
		6)+ cast(pdc.numeroLicitacao as varchar),6) numero,
		'N' registropreco,
		'U' ctlance,
		'N' obra,
		right(replicate('0',
		6)+ cast(pdl.numeroLicitacao as varchar),
		6)+ '/' + cast(pdl.anoLicitacao%2000 as varchar) proclic,
		concat(pdl.idModalidade, pdl.numeroLicitacao, pdl.anoLicitacao) numlic,
		2 microempresa,
		pdl.anoLicitacao,
		1 licnova,
		'$' tlance,
		'N' mult_entidade,
		pdl.anoProcessoDeCompra,
		'N' lei_invertfasestce,
		pdc.valorEstimado,
		pdl.justificativaDaModalidade detalhe,
		pdc.formaDePagamento discr9,
		'S' liberacompra,
			right(replicate('0',
			5)+ cast(pdl.numeroProcessoDeCompra as varchar),
			5)+ '/' + cast(pdl.anoProcessoDeCompra%2000 as varchar) as numorc
	from
		MGFSiga.dbo.ProcessoDeLicitacao pdl
	join MGFSiga.dbo.ProcessoDeCompra pdc on
		pdl.numeroProcessoDeCompra = pdc.numero
		and pdl.anoProcessoDeCompra = pdc.ano and pdc.tipoDeProcesso <> 2
	left join CriterioDeJulgamento cdj on
		cdj.idCriterio = pdc.idCriterio 
	union	
	select
		pdc.numeroLicitacao,
		pdc.dataDoProcesso datae,
		pdc.dataDoProcesso dtpub,
		rdp.dataDaAta dtenc,
		'09:00' horabe,
		cast(pdc.objeto as nvarchar(MAX)) discr,
		cast(cdj.descricao as nvarchar(MAX)) discr7,
		case
			when pdc.idModalidade in (0, 1, 2, 7, 9, 10) then 'DI01'
			--DISPENSA	
			when pdc.idModalidade = 3 then 'IN01'
			--INEXIGIBILIDADE
			when pdc.idModalidade = 4 then 'CONV'
			--CONVITE
			when pdc.idModalidade = 5 then 'TOM3'
			--TOMADA DE PREÇOS
			when pdc.idModalidade = 6 then 'CON4'
			--CONCORRÊNCIA
			when pdc.idModalidade = 8 then 'PP01'
			--PREGÃO PRESENCIAL
			when pdc.idModalidade = 11 then 'LEIL'
			--PREGÃO PRESENCIAL
			when pdc.idModalidade = 12 then 'PE01'
			--PREGÃO PRESENCIAL
		end modlic,
		rdp.dataDaAta dthom,
		rdp.dataDaAta dtadj,
		3 comp,
		right(replicate('0',
		6)+ cast(pdc.numeroLicitacao as varchar),6),
		'S' registropreco,
		'U' ctlance,
		'N' obra,
		right(replicate('0',
		6)+ cast(pdc.numeroLicitacao as varchar),
		6)+ '/' + cast(pdc.anoLicitacao%2000 as varchar) proclic,
		concat(pdc.idModalidade, pdc.numeroLicitacao, pdc.anoLicitacao) numlic,
		2 microempresa,
		pdc.anoLicitacao,
		1 licnova,
		'$' tlance,
		'N' mult_entidade,
		pdc.ano,
		'N' lei_invertfasestce,
		pdc.valorEstimado,
		rdp.objetoDoRegistro detalhe,
		pdc.formaDePagamento discr9,
		'S' liberacompra,
			right(replicate('0',
			5)+ cast(pdc.numero as varchar),
			5)+ '/' + cast(pdc.ano%2000 as varchar) as numorc
	from
		MGFSiga.dbo.RegistroDePrecoNoOrgaoGestor rdp
	join MGFSiga.dbo.ProcessoDeCompra pdc on
		rdp.numeroDoProcesso = pdc.numero
		and rdp.anoDoProcesso = pdc.ano
		and pdc.tipoDeProcesso = 2
	join CriterioDeJulgamento cdj on
		cdj.idCriterio = pdc.idCriterio`

	rows, err := cnxSqls.Query(query)
	if err != nil {
		fmt.Printf("erro ao executar query: %v", err)
	}

	totalLinhas, err := modules.CountRows(query)
	if err != nil {
		fmt.Printf("erro ao obter total de linhas: %v", err)
	}

	barCadlic := modules.NewProgressBar(p, totalLinhas, "CADLIC")

	for rows.Next() {
		var (
			numpro, comp, numlic, ano, licnova, processoAno int
			datae, dtpub, dtenc, horabe, discr, discr7, modlic, dthom, dtadj, registropreco, ctlance, 
			obra, proclic, microempresa, tlance, mult_entidade, leiInvertfasestce, detalhe, liberacompra, numorc, discr9, numero  string
			valor1 float32
		)

		if err := rows.Scan(&numpro, &datae, &dtpub, &dtenc, &horabe, &discr, &discr7, &modlic, &dthom, &dtadj, &comp, &numero, &registropreco, &ctlance, &obra, &proclic,
		&numlic, &microempresa, &ano, &licnova, &tlance, &mult_entidade, &processoAno, &leiInvertfasestce, &valor1, &detalhe, &discr9, &liberacompra, &numorc); err != nil {
			fmt.Printf("erro ao scanear valores: %v", err)
		}

		dataeParseada, err := time.Parse(time.RFC3339, datae)
		if err != nil {
			fmt.Printf("erro ao parsear string para data: %v", err)
		}
		dtpubParseada, err := time.Parse(time.RFC3339, dtpub)
		if err != nil {
			fmt.Printf("erro ao parsear string para data: %v", err)
		}
		dtencParseada, err := time.Parse(time.RFC3339, dtenc)
		if err != nil {
			fmt.Printf("erro ao parsear string para data: %v", err)
		}
		dthomParseada, err := time.Parse(time.RFC3339, dthom)
		if err != nil {
			fmt.Printf("erro ao parsear string para data: %v", err)
		}
		dtadjParseada, err := time.Parse(time.RFC3339, dtadj)
		if err != nil {
			fmt.Printf("erro ao parsear string para data: %v", err)
		}

		format := "02.01.2006"

		dataeFormatada := dataeParseada.Format(format)
		dtpubFormatada := dtpubParseada.Format(format)
		dtencFormatada := dtencParseada.Format(format)
		dthomFormatada := dthomParseada.Format(format)
		dtadjFormatada := dtadjParseada.Format(format)

		if _, err = insert.Exec(numpro, dataeFormatada, dtpubFormatada, dtencFormatada, horabe, discr, discr7, modlic,
		dthomFormatada, dtadjFormatada, comp, numero, registropreco, ctlance, obra, proclic, numlic, microempresa, ano, licnova,
		tlance, mult_entidade, processoAno, leiInvertfasestce, valor1, detalhe, discr9, liberacompra, numorc); err != nil {
			fmt.Printf("erro ao inserir registro: %v", err)
		}

		barCadlic.Increment()
	}
}