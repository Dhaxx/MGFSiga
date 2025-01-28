package compras

import (
	"MGFSiga/connection"
	"MGFSiga/modules"
	"fmt"
	"time"

	"github.com/gobuffalo/nulls"
	"github.com/vbauerster/mpb"
)

func Cadlic(p *mpb.Progress) {
	modules.LimpaTabela("cadlic")
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
		lei_invertfasestce,
		valor1,
		detalhe,
		discr9,
		liberacompra,
		numorc,
		codmod,
		empresa) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		fmt.Printf("erro ao preparar insert: %v", err)
	}

	query := `select
	distinct *
from
	(
	select
					pdc.numero,
					pdc.dataDoProcesso datae,
					pdc.dataDoProcesso dtpub,
					pdl.dataDoTermoDeRatificacao dtenc,
					'09:00' horabe,
					cast(pdc.objeto as nvarchar(1024)) discr,
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
					'N' registropreco,
					'U' ctlance,
					'N' obra,
					case
						when pdc.idModalidade in (0, 1, 2, 7, 9, 10) then '1'
			--DISPENSA	
			when pdc.idModalidade = 3 then '5'
			--INEXIGIBILIDADE
			when pdc.idModalidade = 4 then '2'
			--CONVITE
			when pdc.idModalidade = 5 then '3'
			--TOMADA DE PREÇOS
			when pdc.idModalidade = 6 then '4'
			--CONCORRÊNCIA
			when pdc.idModalidade = 8 then '8'
			--PREGÃO PRESENCIAL
			when pdc.idModalidade = 11 then '6'
			--PREGÃO PRESENCIAL
			when pdc.idModalidade = 12 then '9'
			--PREGÃO PRESENCIAL
		end + right(replicate('0',
						5)+ cast(pdc.numero as varchar),
						5)+ '/' + cast(pdc.ano%2000 as varchar) as proclic,
					concat(pdl.idModalidade, pdc.numero, pdc.numeroLicitacao, pdc.ano%2000) numlic,
					2 microempresa,
					pdl.anoLicitacao,
					1 licnova,
					'$' tlance,
					'N' mult_entidade,
					pdl.anoProcessoDeCompra,
					'N' lei_invertfasestce,
					pdc.valorEstimado,
					cast(pdl.justificativaDaModalidade as nvarchar(max)) detalhe,
					pdc.formaDePagamento discr9,
					'S' liberacompra,
						right(replicate('0',
						5)+ cast(pdl.numeroProcessoDeCompra as varchar),
						5)+ '/' + cast(pdl.anoProcessoDeCompra%2000 as varchar) as numorc,
						case
						when pdc.idModalidade in (0, 1, 2, 7, 9, 10) then 1
			--DISPENSA	
			when pdc.idModalidade = 3 then 5
			--INEXIGIBILIDADE
			when pdc.idModalidade = 4 then 2
			--CONVITE
			when pdc.idModalidade = 5 then 3
			--TOMADA DE PREÇOS
			when pdc.idModalidade = 6 then 4
			--CONCORRÊNCIA
			when pdc.idModalidade = 8 then 8
			--PREGÃO PRESENCIAL
			when pdc.idModalidade = 11 then 6
			--PREGÃO PRESENCIAL
			when pdc.idModalidade = 12 then 9
			--PREGÃO PRESENCIAL
		end codmod
	from
					MGFSiga.dbo.ProcessoDeLicitacao pdl
	join MGFSiga.dbo.ProcessoDeCompra pdc on
					pdl.numeroProcessoDeCompra = pdc.numero
		and pdl.anoProcessoDeCompra = pdc.ano
		and pdc.tipoDeProcesso <> 2
	left join CriterioDeJulgamento cdj on
					cdj.idCriterio = pdc.idCriterio
union
	select
					pdc.numeroLicitacao,
					pdc.dataDoProcesso datae,
					pdc.dataDoProcesso dtpub,
					rdp.dataDaAta dtenc,
					'09:00' horabe,
					cast(pdc.objeto as nvarchar(1024)) discr,
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
					'S' registropreco,
					'U' ctlance,
					'N' obra,
					case
						when pdc.idModalidade in (0, 1, 2, 7, 9, 10) then '1'
			--DISPENSA	
			when pdc.idModalidade = 3 then '5'
			--INEXIGIBILIDADE
			when pdc.idModalidade = 4 then '2'
			--CONVITE
			when pdc.idModalidade = 5 then '3'
			--TOMADA DE PREÇOS
			when pdc.idModalidade = 6 then '4'
			--CONCORRÊNCIA
			when pdc.idModalidade = 8 then '8'
			--PREGÃO PRESENCIAL
			when pdc.idModalidade = 11 then '6'
			--PREGÃO PRESENCIAL
			when pdc.idModalidade = 12 then '9'
			--PREGÃO PRESENCIAL
		end + right(replicate('0',
						5)+ cast(pdc.numero as varchar),
						5)+ '/' + cast(pdc.ano%2000 as varchar) as proclic,
					concat(pdc.idModalidade, pdc.numero, pdc.numeroLicitacao, pdc.ano%2000) numlic,
					2 microempresa,
					pdc.anoLicitacao,
					1 licnova,
					'$' tlance,
					'N' mult_entidade,
					pdc.ano,
					'N' lei_invertfasestce,
					pdc.valorEstimado,
					cast(rdp.objetoDoRegistro as nvarchar(max)) detalhe,
					pdc.formaDePagamento discr9,
					'S' liberacompra,
						right(replicate('0',
						5)+ cast(pdc.numero as varchar),
						5)+ '/' + cast(pdc.ano%2000 as varchar) as numorc,
					case
						when pdc.idModalidade in (0, 1, 2, 7, 9, 10) then 1
			--DISPENSA	
			when pdc.idModalidade = 3 then 5
			--INEXIGIBILIDADE
			when pdc.idModalidade = 4 then 2
			--CONVITE
			when pdc.idModalidade = 5 then 3
			--TOMADA DE PREÇOS
			when pdc.idModalidade = 6 then 4
			--CONCORRÊNCIA
			when pdc.idModalidade = 8 then 8
			--PREGÃO PRESENCIAL
			when pdc.idModalidade = 11 then 6
			--PREGÃO PRESENCIAL
			when pdc.idModalidade = 12 then 9
			--PREGÃO PRESENCIAL
		end codmod
	from
					MGFSiga.dbo.RegistroDePrecoNoOrgaoGestor rdp
	join MGFSiga.dbo.ProcessoDeCompra pdc on
					rdp.numeroDoProcesso = pdc.numero
		and rdp.anoDoProcesso = pdc.ano
		and pdc.tipoDeProcesso = 2
	join CriterioDeJulgamento cdj on
					cdj.idCriterio = pdc.idCriterio
union all
	select
		rdp.numeroProcesso,	
		dataDoProcesso,
		dataDoProcesso,
		rdp.dataDoRegistro,
		'09:00' horabe,
		cast(pdc.objeto as nvarchar(1024)) discr,
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
		dataDoProcesso,
		dataDoProcesso,
		3 comp,
		'S' registropreco,
					'U' ctlance,
					'N' obra,
					case
						when pdc.idModalidade in (0, 1, 2, 7, 9, 10) then '1'
			--DISPENSA	
			when pdc.idModalidade = 3 then '5'
			--INEXIGIBILIDADE
			when pdc.idModalidade = 4 then '2'
			--CONVITE
			when pdc.idModalidade = 5 then '3'
			--TOMADA DE PREÇOS
			when pdc.idModalidade = 6 then '4'
			--CONCORRÊNCIA
			when pdc.idModalidade = 8 then '8'
			--PREGÃO PRESENCIAL
			when pdc.idModalidade = 11 then '6'
			--PREGÃO PRESENCIAL
			when pdc.idModalidade = 12 then '9'
			--PREGÃO PRESENCIAL
		end + right(replicate('0',
						5)+ cast(pdc.numero as varchar),
						5)+ '/' + cast(pdc.ano%2000 as varchar) as proclic,
					concat(pdc.idModalidade, pdc.numero, pdc.numeroLicitacao, pdc.ano%2000) numlic,
					2 microempresa,
					pdc.anoLicitacao,
					1 licnova,
					'$' tlance,
					'N' mult_entidade,
					pdc.ano,
					'N' lei_invertfasestce,
					pdc.valorEstimado,
					cast(pdc.objeto as nvarchar(max)) detalhe,
					pdc.formaDePagamento discr9,
					'S' liberacompra,
						right(replicate('0',
						5)+ cast(pdc.numero as varchar),
						5)+ '/' + cast(pdc.ano%2000 as varchar) as numorc,
					case
						when pdc.idModalidade in (0, 1, 2, 7, 9, 10) then 1
			--DISPENSA	
			when pdc.idModalidade = 3 then 5
			--INEXIGIBILIDADE
			when pdc.idModalidade = 4 then 2
			--CONVITE
			when pdc.idModalidade = 5 then 3
			--TOMADA DE PREÇOS
			when pdc.idModalidade = 6 then 4
			--CONCORRÊNCIA
			when pdc.idModalidade = 8 then 8
			--PREGÃO PRESENCIAL
			when pdc.idModalidade = 11 then 6
			--PREGÃO PRESENCIAL
			when pdc.idModalidade = 12 then 9
			--PREGÃO PRESENCIAL
		end codmod
	from
		MGFSiga.dbo.RegistroDePreco rdp
	join MGFSiga.dbo.ProcessoDeCompra pdc on
		rdp.numeroProcesso = pdc.numero
		and rdp.anoProcesso = pdc.ano
	join MGFSiga.dbo.CriterioDeJulgamento cdj ON
		cdj.idCriterio = pdc.idCriterio) as subquery`

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
			numpro, comp, numlic, ano, licnova, processoAno, codmod int
			datae, dtpub, dtenc, horabe, discr, discr7, modlic, dthom, dtadj, registropreco, ctlance, 
			obra, proclic, microempresa, tlance, mult_entidade, leiInvertfasestce, detalhe, liberacompra, numorc, discr9, numero string
			valor1 float32
		)

		if err := rows.Scan(&numpro, &datae, &dtpub, &dtenc, &horabe, &discr, &discr7, &modlic, &dthom, &dtadj, &comp, &registropreco, &ctlance, &obra, &proclic,
		&numlic, &microempresa, &ano, &licnova, &tlance, &mult_entidade, &processoAno, &leiInvertfasestce, &valor1, &detalhe, &discr9, &liberacompra, &numorc, &codmod); err != nil {
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

		discrConvertidoWin1252, err := modules.DecodeToWin1252(discr)
		if err != nil {
			fmt.Printf("erro ao decodificar: %v", err)
		}
		discr7ConvertidoWin1252, err := modules.DecodeToWin1252(discr7)
		if err != nil {
			fmt.Printf("erro ao decodificar: %v", err)
		}

		numero = proclic[:6]

		if _, err = insert.Exec(numpro, dataeFormatada, dtpubFormatada, dtencFormatada, horabe, discrConvertidoWin1252, discr7ConvertidoWin1252, modlic,
		dthomFormatada, dtadjFormatada, comp, numero, registropreco, ctlance, obra, proclic, numlic, microempresa, ano, licnova,
		tlance, mult_entidade, processoAno, leiInvertfasestce, valor1, detalhe, discr9, liberacompra, numorc, codmod, modules.Cache.Empresa); err != nil {
			fmt.Printf("erro ao inserir registro: %v", err)
		}

		barCadlic.Increment()
	}
	tx.Commit()

	tx1, err := cnxFdb.Begin()
	if err != nil {
		fmt.Printf("erro ao iniciar transação: %v", err)
	}

	tx2, err := cnxFdb.Begin()
	if err != nil {
		fmt.Printf("erro ao iniciar transação: %v", err)
	}
	
	tx3, err := cnxFdb.Begin()
	if err != nil {
		fmt.Printf("erro ao iniciar transação: %v", err)
	}

	tx4, err := cnxFdb.Begin()
	if err != nil {
		fmt.Printf("erro ao iniciar transação: %v", err)
	}

	tx5, err := cnxFdb.Begin()
	if err != nil {
		fmt.Printf("erro ao iniciar transação: %v", err)
	}

	_, err = tx1.Exec(`EXECUTE BLOCK AS
					DECLARE VARIABLE NUMLIC INTEGER;
					DECLARE VARIABLE NUMORC VARCHAR(8);
					DECLARE VARIABLE PROCLIC VARCHAR(9);
					BEGIN
						FOR 
							SELECT NUMLIC, PROCLIC, NUMORC FROM CADLIC WHERE NUMORC IS NOT NULL INTO :NUMLIC, :PROCLIC, :NUMORC
						DO
						BEGIN
							UPDATE CADORC SET PROCLIC = :PROCLIC, NUMLIC = :NUMLIC WHERE NUMORC = :NUMORC;
						END
						UPDATE CADLIC SET NUMORC = NULL;
					END`)
	if err != nil {
		fmt.Printf("erro ao executar query: %v", err)
	}
	tx1.Commit()

	_, err = tx2.Exec(`EXECUTE BLOCK AS
					DECLARE VARIABLE DESCMOD VARCHAR(1024);
					DECLARE VARIABLE CODMOD INTEGER;
					BEGIN
						FOR
							SELECT CODMOD, DESCMOD FROM MODLIC INTO :CODMOD, :DESCMOD
						DO
						BEGIN
							UPDATE CADLIC SET LICIT = :DESCMOD where CODMOD = :CODMOD;
						END
					END`)
	if err != nil {
		fmt.Printf("erro ao executar query: %v", err)
	}
	tx2.Commit()

	_, err = tx3.Exec(`INSERT
			INTO
			MODLICANO (ULTNUMPRO,
			CODMOD,
			ANOMOD,
			EMPRESA)
		SELECT
			COALESCE(MAX(NUMPRO), 0),
			CODMOD,
			COALESCE(ANO, 0) ANO,
			EMPRESA
		FROM
			CADLIC c
		WHERE
			CODMOD IS NOT NULL
		GROUP BY
			2,
			3,
			4
		ORDER BY
			ano,
			codmod`)
	if err != nil {
		fmt.Printf("erro ao executar query: %v", err)
	}
	tx3.Commit()

	_, err = tx4.Exec(`INSERT INTO CADLICITACAO (modlic, licitacao, obs, empresa, proclic, numlic, nlicitacao) SELECT MODLIC, numpro, discr, empresa, PROCLIC, NUMLIC, NUMLIC FROM CADLIC b 
	WHERE NOT EXISTS (SELECT 1 FROM cadlicitacao x WHERE x.numlic = b.numlic)`)
	if err != nil {
		fmt.Printf("erro ao executar query: %v", err)
	}
	tx4.Commit()

	_, err = tx5.Exec(`update cadlic set processo = CAST(substring(numero FROM 3 for 6) AS int), ano = processo_ano`)
	if err != nil {
		fmt.Printf("erro ao executar query: %v", err)
	}
	tx5.Commit()
}

func Cadprolic(p *mpb.Progress) {
	modules.LimpaTabela("CADLOTELIC")
	modules.LimpaTabela("CADPROLIC")
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

	insert, err := tx.Prepare(`insert into cadprolic (numorc, lotelic, item, item_mask, itemorc, cadpro, codccusto, quan1, vamed1, vatomed1, reduz, microempresa, tlance, item_ag, numlic, id_cadorc, item_lote, item_lc147) 
									values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		panic("Erro ao preparar insert: " + err.Error())
	}

	query := `select * from (select
		right(replicate('0',
		5)+ cast(idc.numero as varchar),
		5)+ '/' + cast(idc.ano%2000 as varchar) as numorc,
		right(replicate('0', 8)+cast(idlote as varchar), 8) as lotelic,
		row_number() over (partition by idc.numero, idc.ano order by idc.numero, idc.ano, idc.idEspecificacao) item,
		idEspecificacao,
		0 codccusto,
		quantidade,
		idc.valorestimado,
		quantidade * idc.valorestimado vato,
		'N' reduz,
		'N' microempresa,
		'$' tlance,
		case
						when pdc.idModalidade in (0, 1, 2, 7, 9, 10) then '1'
				--DISPENSA	
				when pdc.idModalidade = 3 then '5'
				--INEXIGIBILIDADE
				when pdc.idModalidade = 4 then '2'
				--CONVITE
				when pdc.idModalidade = 5 then '3'
				--TOMADA DE PREÇOS
				when pdc.idModalidade = 6 then '4'
				--CONCORRÊNCIA
				when pdc.idModalidade = 8 then '8'
				--PREGÃO PRESENCIAL
				when pdc.idModalidade = 11 then '6'
				--PREGÃO PRESENCIAL
				when pdc.idModalidade = 12 then '9'
				--PREGÃO PRESENCIAL
			end + right(replicate('0',
					6)+ cast(pdc.numeroLicitacao as varchar),
					5)+ '/' + cast(pdc.anoLicitacao%2000 as varchar) proclic,
					concat(pdc.idModalidade, pdc.numero, pdc.numeroLicitacao, pdc.ano%2000) numlic
	from
		MGFSiga.dbo.ItemDeProcessoDeCompra idc
	join MGFSiga.dbo.ProcessoDeCompra pdc 
	on pdc.numero = idc.numero and pdc.ano = idc.ano
	join MGFSiga.dbo.ProcessoDeLicitacao pdl on 
	pdl.numeroProcessoDeCompra = pdc.numero
	and pdl.anoProcessoDeCompra = pdc.ano
	union 
	select
		right(replicate('0',
		5)+ cast(idc.numero as varchar),
		5)+ '/' + cast(idc.ano%2000 as varchar) as numorc,
		right(replicate('0', 8)+cast(idlote as varchar), 8) as lotelic,
		row_number() over (partition by idc.numero, idc.ano order by idc.numero, idc.ano, idc.idEspecificacao) item,
		idEspecificacao,
		0 codccusto,
		quantidade,
		idc.valorestimado,
		quantidade * idc.valorestimado vato,
		'N' reduz,
		'N' microempresa,
		'$' tlance,
		case
						when pdc.idModalidade in (0, 1, 2, 7, 9, 10) then '1'
				--DISPENSA	
				when pdc.idModalidade = 3 then '5'
				--INEXIGIBILIDADE
				when pdc.idModalidade = 4 then '2'
				--CONVITE
				when pdc.idModalidade = 5 then '3'
				--TOMADA DE PREÇOS
				when pdc.idModalidade = 6 then '4'
				--CONCORRÊNCIA
				when pdc.idModalidade = 8 then '8'
				--PREGÃO PRESENCIAL
				when pdc.idModalidade = 11 then '6'
				--PREGÃO PRESENCIAL
				when pdc.idModalidade = 12 then '9'
				--PREGÃO PRESENCIAL
			end + right(replicate('0',
					6)+ cast(pdc.numeroLicitacao as varchar),
					5)+ '/' + cast(pdc.anoLicitacao%2000 as varchar) proclic,
					concat(pdc.idModalidade, pdc.numero, pdc.numeroLicitacao, pdc.ano%2000) numlic
	from
		MGFSiga.dbo.ItemDeProcessoDeCompra idc
	join MGFSiga.dbo.ProcessoDeCompra pdc 
	on pdc.numero = idc.numero and pdc.ano = idc.ano
	join MGFSiga.dbo.RegistroDePreco rdp on 
	rdp.numeroProcesso  = pdc.numero 
	and rdp.anoProcesso = pdc.ano) as subquery`

	rows, err := cnxSqls.Query(query)
	if err != nil {
		fmt.Printf("erro ao obter linhas: %v", err)
	}

	totalLinhas, err := modules.CountRows(query)
	if err != nil {
		fmt.Printf("erro ao obter total de linhas: %v", err)
	}

	barCadprolic := modules.NewProgressBar(p, totalLinhas, "CADPROLIC")

	for rows.Next() {
		var (
			numorc, idEspecificacao, reduz, microempresa, tlance, proclic, numlic string
			item, codccusto int
			quantidade, valorEstimado, vato float32
			lotelic, itemLote nulls.String
		)
		
		if  err := rows.Scan(&numorc, &lotelic, &item, &idEspecificacao, &codccusto, &quantidade, &valorEstimado, &vato, &reduz, &microempresa, &tlance, &proclic, &numlic); err != nil {
			fmt.Printf("erro ao scanear valores: %v", err)
		}

		idCadorc := modules.Cache.IdCadorc[numorc]
		cadpro := modules.Cache.Itens[idEspecificacao]
		if lotelic.String == "00000000" {
			lotelic.String = ""
			lotelic.Valid = false
			itemLote.String = ""
			itemLote.Valid = false
		}

		if _, err := insert.Exec(numorc, lotelic, item, item, item, cadpro, codccusto, quantidade, valorEstimado, vato, reduz, microempresa, tlance, item, numlic, idCadorc, itemLote, item); err != nil {
			fmt.Printf("erro ao inserir registro: %v", err)
		}

		barCadprolic.Increment()
	}
	tx.Commit()

	cnxFdb.Exec(`INSERT
		INTO
		cadlotelic (descr,
		lotelic,
		numlic) 
	SELECT
		'Lote ' || lotelic,
		lotelic,
		numlic
	FROM
		cadprolic a
	WHERE
		lotelic IS NOT NULL
		AND NOT EXISTS (
		SELECT
			1
		FROM
			CADLOTELIC c
		WHERE
			c.numlic = a.numlic
			AND a.lotelic = c.lotelic)`)
}

func CadprolicDetalhe(p *mpb.Progress) {
	cnxFdb, err := connection.ConexaoDestino()
	if err != nil {
		panic("Falha ao conectar com o banco de destino: " + err.Error())
	}
	defer cnxFdb.Close()

	barCadprolicDetalhe := modules.NewProgressBar(p, 1, "CADPROLIC DETALHE")
	cnxFdb.Exec("ALTER TRIGGER TBI_CADPROLIC_DETALHE_BLOQUEIO INACTIVE")
	cnxFdb.Exec(`INSERT INTO CADPROLIC_DETALHE (NUMLIC,item,CADPRO,quan1,VAMED1,VATOMED1,marca,CODCCUSTO,ITEM_CADPROLIC)
					select numlic, item, cadpro, quan1, vamed1, vatomed1, marca, codccusto, item from cadprolic b where
					not exists (select 1 from cadprolic_detalhe c where b.numlic = c.numlic and b.item = c.item);`)
	cnxFdb.Exec("ALTER TRIGGER TBI_CADPROLIC_DETALHE_BLOQUEIO ACTIVE;`)")
	barCadprolicDetalhe.Increment()
}

func ProlicProlics(p *mpb.Progress) {
	// modules.LimpaTabela("prolic")
	// modules.LimpaTabela("prolics")
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

	insertProlic, err := tx.Prepare("insert into prolic (codif, nome, status, numlic, obs) values (?,?,?,?,?)")
	if err != nil {
		fmt.Printf("erro ao preparar insert: %v", err)
	}

	insertProlics, err := cnxFdb.Prepare(`insert into prolics (sessao, codif, status, representante, numlic, usa_preferencia) select 1, codif, status, obs, numlic, 'N' from prolic p
	WHERE NOT EXISTS (SELECT 1 FROM prolics x WHERE x.numlic = p.numlic AND x.codif = p.codif)`)
	if err != nil {
		fmt.Printf("erro ao preparar insert: %v", err)
	}

	query := `select
		distinct
			'A' status,
			trim(idp.cgc_cpf) cgc,
			substring(razao, 1, 40) razao,
			substring(nomeRepresentante, 1, 100) nome,
			concat(pdc.idModalidade, pdc.numero, pdc.numeroLicitacao, pdc.ano%2000) numlic
	from
			MGFSiga.dbo.ItemDeProposta idp
	join proposta p on
		idp.anoProcesso = p.anoProcesso
		and 
		idp.numeroProcesso = p.numeroProcesso
		and idp.cgc_cpf = p.cgc_cpf
	join empresa e on
		e.cgc_cpf = p.cgc_cpf
	join processoDeCompra pdc on
		pdc.numero = idp.numeroProcesso
		and pdc.ano = idp.anoProcesso
	join MGFSiga.dbo.ProcessoDeLicitacao pdl on
		pdl.numeroProcessoDeCompra = pdc.numero
		and pdl.anoProcessoDeCompra = pdc.ano
	union 
		select
		distinct
			'A' status,
			trim(idp.cgc_cpf) cgc,
			substring(razao, 1, 40) razao,
			substring(nomeRepresentante, 1, 100) nome,
			concat(pdc.idModalidade, pdc.numero, pdc.numeroLicitacao, pdc.ano%2000) numlic
	from
			MGFSiga.dbo.ItemDeProposta idp
	join proposta p on
		idp.anoProcesso = p.anoProcesso
		and 
		idp.numeroProcesso = p.numeroProcesso
		and idp.cgc_cpf = p.cgc_cpf
	join empresa e on
		e.cgc_cpf = p.cgc_cpf
	join processoDeCompra pdc on
		pdc.numero = idp.numeroProcesso
		and pdc.ano = idp.anoProcesso
	join MGFSiga.dbo.RegistroDePreco rdp on
		rdp.numeroProcesso = pdc.numero
		and rdp.anoProcesso = pdc.ano`

	rows, err := cnxSqls.Query(query)
	if err != nil {
		fmt.Printf("erro ao executar query: %v", err)
	}

	totalLinhas, err := modules.CountRows(query)
	if err != nil {
		fmt.Printf("erro ao obter total de linhas: %v", err)
	}

	barProlic := modules.NewProgressBar(p, totalLinhas, "PROLIC")

	for rows.Next() {
		var (
			insmf, status, nome, representante, numlic, usaPreferencial string
		)

		if err := rows.Scan(&status, &insmf, &nome, &representante, &numlic); err != nil {
			fmt.Printf("erro ao scanear valores: %v", err)
		}

		codif := modules.Cache.Codif[insmf]
		nomeConvertido1252, err := modules.DecodeToWin1252(nome)
		if err != nil {
			fmt.Printf("erro ao decodificar: %v", err)
		}
		representanteConvertido1252, err := modules.DecodeToWin1252(representante)
		if err != nil {
			fmt.Printf("erro ao decodificar: %v", err)
		}

		if _, err := insertProlic.Exec(codif, nomeConvertido1252, status, numlic, nome); err != nil {
			fmt.Printf("erro ao inserir registro: %v", err)
		}

		if _, err := insertProlics.Exec("1", codif, status, representanteConvertido1252, numlic, usaPreferencial); err != nil {
			fmt.Printf("erro ao inserir registro: %v", err)
		}

		barProlic.Increment()
	}
	tx.Commit()

	if _, err := insertProlics.Exec(); err != nil {
		fmt.Printf("erro ao inserir registro: %v", err)
	}
}

func CadlicSessao(p *mpb.Progress) {
	modules.LimpaTabela("cadlic_sessao")
	modules.LimpaTabela("cadpro_status")

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

	barCadlicsessao := modules.NewProgressBar(p, 1, "CADLICSESSAO")

	cnxFdb.Exec(`INSERT INTO cadpro_status (numlic, sessao, itemp, item, telafinal)
					SELECT b.NUMLIC, 1 AS sessao, a.item, a.item, 'I_ENCERRAMENTO'
					FROM CADPROLIC a
					JOIN cadlic b ON a.NUMLIC = b.NUMLIC
					WHERE NOT EXISTS (
						SELECT 1
						FROM cadpro_status c
						WHERE a.numlic = c.numlic and a.item = c.item);`)

	cnxFdb.Exec(`INSERT INTO CADLIC_SESSAO (NUMLIC, SESSAO, DTREAL, HORREAL, COMP, DTENC, HORENC, SESSAOPARA, MOTIVO) 
	SELECT L.NUMLIC, CAST(1 AS INTEGER), L.DTREAL, L.HORREAL, L.COMP, L.DTENC, L.HORENC, CAST('T' AS VARCHAR(1)), CAST('O' AS VARCHAR(1)) FROM CADLIC L 
	WHERE numlic not in (SELECT FIRST 1 S.NUMLIC FROM CADLIC_SESSAO S WHERE S.NUMLIC = L.NUMLIC)`)

	barCadlicsessao.Increment()
}

func CadproProposta(p *mpb.Progress) {
	modules.LimpaTabela("cadpro_proposta")

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

	insert, err := tx.Prepare(`insert into cadpro_proposta (codif, sessao, numlic, lotelic, itemp, item, quan1, vaun1, vato1, status, subem) values (?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		fmt.Printf("erro ao preparar insert: %v", err)
	}

	query := `select
				trim(idp.cgc_cpf) cgc,
				1 sessao,
				concat(pdc.idModalidade, pdc.numero, pdc.numeroLicitacao, pdc.ano%2000) numlic,
				case when idLote = 0 then null else right(replicate(0, '8')+ cast(idlote as varchar), 8) end lotelic,
				DENSE_RANK() over (partition by pdc.numero, pdc.ano order by idp.idEspecificacao) item,
				idp.quantidadeVencedora,
				idp.precoUnitarioFinal,
				idp.quantidadeVencedora * idp.precoUnitarioFinal vato1,
				case when documentacaoAceita = 1 then 'C' else 'D' end status,
				case when idp.status = 0 then 1 else 0 end subem
		from
				MGFSiga.dbo.ItemDeProposta idp
		join ProcessoDeCompra pdc on
			idp.anoProcesso = pdc.ano
			and idp.numeroProcesso = pdc.numero
		join MGFSiga.dbo.ProcessoDeLicitacao pdl on
			pdl.numeroProcessoDeCompra = pdc.numero
			and pdl.anoProcessoDeCompra = pdc.ano
		join MGFSiga.dbo.Proposta p on
			idp.numeroProcesso = p.numeroProcesso 
			and idp.anoProcesso = p.anoProcesso
			and idp.CGC_CPF = p.CGC_CPF
	UNION 
	select
				trim(rdp.cgc_cpf) cgc,
				1 sessao,
				concat(pdc.idModalidade, pdc.numero, pdc.numeroLicitacao, pdc.ano%2000) numlic,
				case when idLote = 0 then null else right(replicate(0, '8')+ cast(idlote as varchar), 8) end lotelic,
				DENSE_RANK() over (partition by pdc.numero, pdc.ano order by idp.idEspecificacao) item,
				idp.quantidadeVencedora,
				idp.precoUnitarioFinal,
				idp.quantidadeVencedora * idp.precoUnitarioFinal vato1,
				case when documentacaoAceita = 1 then 'C' else 'D' end status,
				case when idp.status = 0 then 1 else 0 end subem
		from
				MGFSiga.dbo.ItemDeProposta idp
		join ProcessoDeCompra pdc on
			idp.anoProcesso = pdc.ano
			and idp.numeroProcesso = pdc.numero
		join MGFSiga.dbo.RegistroDePreco rdp on
			rdp.numeroProcesso = pdc.numero
			and rdp.anoProcesso = pdc.ano
		join MGFSiga.dbo.Proposta p on
			idp.numeroProcesso = p.numeroProcesso 
			and idp.anoProcesso = p.anoProcesso
			and idp.CGC_CPF = p.CGC_CPF`

	rows, err := cnxSqls.Query(query)
	if err != nil {
		fmt.Printf("erro ao executar query: %v", err)
	}

	totalLinhas, err := modules.CountRows(query)
	if err != nil {
		fmt.Printf("erro ao obter total de linhas: %v", err)
	}

	barCadproproposta := modules.NewProgressBar(p, totalLinhas, "CADPROPOSTA")

	for rows.Next() {
		var (
			insmf, numlic, status, subem string
			sessao, item int
			quan1, vaun1, vato1 float32
			lotelic nulls.String
		)

		if err := rows.Scan(&insmf, &sessao, &numlic, &lotelic, &item, &quan1, &vaun1, &vato1, &status, &subem); err != nil {
			fmt.Printf("erro ao scanear valores: %v", err)
		}

		codif := modules.Cache.Codif[insmf]

		if _, err := insert.Exec(codif, sessao, numlic, lotelic, item, item, quan1, vaun1, vato1, status, subem); err != nil {
			fmt.Printf("erro ao inserir registro: %v", err)
		}
		barCadproproposta.Increment()
	}
}

func CadproLance(p *mpb.Progress) {
	modules.LimpaTabela("cadpro_lance")

	cnxFdb, err := connection.ConexaoDestino()
	if err != nil {
		panic("Falha ao conectar com o banco de destino: " + err.Error())
	}
	defer cnxFdb.Close()

	barCadprolance := modules.NewProgressBar(p, 1, "CADPROLANCE")

	cnxFdb.Exec(`insert into cadpro_lance (sessao, rodada, codif, itemp, vaunl, vatol, status, subem, numlic)
					SELECT sessao, 1 rodada, CODIF, ITEMP, VAUN1, VATO1, 'F' status, SUBEM, numlic FROM CADPRO_PROPOSTA cp where subem = 1 and not exists
					(select 1 from cadpro_lance cl where cp.codif = cl.codif and cl.itemp = cp.itemp and cl.numlic = cp.numlic)`)
	barCadprolance.Increment()
}

func CadproFinal(p *mpb.Progress) {
	modules.LimpaTabela("cadpro_final")

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

	barCadproFinal := modules.NewProgressBar(p, 1, "CADPROFINAL")

	tx.Exec("alter table cadpro_final add CQTDADT double precision")
	tx.Exec("alter table cadpro_final add ccadpro varchar(20)")
	tx.Exec("alter table cadpro_final add CCODCCUSTO integer;")
	tx.Commit()
	cnxFdb.Exec(`EXECUTE BLOCK
	AS
	BEGIN	                          
		INSERT INTO CADPRO_FINAL (NUMLIC, ULT_SESSAO, CODIF, ITEMP, VAUNF, VATOF, STATUS, SUBEM, PERCF) 
							SELECT A.NUMLIC, A.SESSAO, A.CODIF, A.ITEMP, A.VAUN1, A.VATO1, 'C', 1, NULL  
							FROM CADPRO_PROPOSTA A 
							WHERE NOT EXISTS(SELECT 1 FROM CADPRO_FINAL B WHERE A.NUMLIC = B.NUMLIC AND A.SESSAO = B.ULT_SESSAO AND A.ITEMP = B.ITEMP) 
							AND A.STATUS = 'C' AND A.SUBEM = 1 AND A.NUMLIC IN (SELECT NUMLIC FROM CADLIC);
		
		MERGE INTO cadpro_final f
		using(
			SELECT quan1,cadpro,codccusto,numlic,item FROM cadprolic
		) a ON a.numlic = f.numlic AND a.item = f.itemp
		WHEN MATCHED THEN UPDATE SET f.CQTDADT = a.quan1, f.ccadpro = a.cadpro, f.ccodccusto = a.codccusto;
	END`)
	barCadproFinal.Increment()
}

func Cadpro(p *mpb.Progress) {
	modules.LimpaTabela("cadpro")

	cnxFdb, err := connection.ConexaoDestino()
	if err != nil {
		panic("Falha ao conectar com o banco de destino: " + err.Error())
	}
	defer cnxFdb.Close()

	tx, err := cnxFdb.Begin()
	if err != nil {
		fmt.Printf("erro ao iniciar transação: %v", err)
	}

	tx.Exec(`INSERT INTO CADPRO (
					CODIF,
					CADPRO,
					QUAN1,
					VAUN1,
					VATO1,
					SUBEM,
					STATUS,
					ITEM,
					NUMORC,
					ITEMORCPED,
					CODCCUSTO,
					FICHA,
					ELEMENTO,
					DESDOBRO,
					NUMLIC,
					ULT_SESSAO,
					ITEMP,
					QTDADT,
					QTDPED,
					VAUNADT,
					VATOADT,
					PERC,
					QTDSOL,
					ID_CADORC,
					VATOPED,
					VATOSOL,
					TPCONTROLE_SALDO,
					QTDPED_FORNECEDOR_ANT,
					VATOPED_FORNECEDOR_ANT
				)
				SELECT
					a.CODIF,
					c.CADPRO,
					CASE WHEN a.VAUNL <> 0 THEN ROUND((a.vatol / a.VAUNL), 2) ELSE 0 END qtdunit,
					a.VAUNL,
					CASE WHEN a.VAUNL <> 0 THEN ROUND((a.vatol / a.VAUNL), 2) * a.VAUNL ELSE 0 END VATOTAL,
					1,
					'C',
					c.ITEM,
					c.NUMORC,
					c.ITEM,
					c.CODCCUSTO,
					c.FICHA,
					c.ELEMENTO,
					c.DESDOBRO,
					a.NUMLIC,
					1,
					b.ITEMP,
					CASE WHEN a.VAUNL <> 0 THEN ROUND((a.vatol / a.VAUNL), 2) ELSE 0 END qtdunit_adit,
					0,
					a.VAUNL,
					CASE WHEN a.VAUNL <> 0 THEN ROUND((a.vatol / a.VAUNL), 2) * a.VAUNL ELSE 0 END VATOTAL,
					0,
					0,
					c.ID_CADORC,
					0,
					0,
					'Q',
					0,
					0
				FROM
					CADPRO_LANCE a
				INNER JOIN CADPRO_STATUS b ON
					b.NUMLIC = a.NUMLIC AND a.ITEMP = b.ITEMP AND a.SESSAO = b.SESSAO
				INNER JOIN CADPROLIC_DETALHE c ON
					c.NUMLIC = a.NUMLIC AND b.ITEM = c.ITEM_CADPROLIC
				INNER JOIN CADLIC D ON
					D.NUMLIC = A.NUMLIC
				WHERE
					a.SUBEM = 1 AND a.STATUS = 'F'
					AND NOT EXISTS (
						SELECT 1 
						FROM CADPRO cp
						WHERE cp.NUMLIC = a.NUMLIC 
						AND cp.ITEM = c.ITEM 
						AND cp.CODIF = a.CODIF
					);`)
	tx.Commit()

	cnxFdb.Exec(`insert into cadprolic_detalhe_fic (numlic, item, codigo, qtd, valor, qtdadt, valoradt, codccusto, qtdmed, valormed, tipo) 
		select numlic, item, '0', sum(quan1) qtd, sum(vato1) valor, sum(qtdadt) qtdadt, sum(vatoadt) valoradt, codccusto, sum(quan1) qtdmed, sum(vato1) valormed, 'C' from cadpro a where numlic in 
		(select numlic from cadlic where liberacompra='S' and registropreco = 'N') and subem = 1 AND NOT EXISTS (SELECT 1 FROM cadprolic_detalhe_fic x WHERE a.numlic = x.numlic and a.item = x.item)
		GROUP BY numlic, item, codccusto`)
}

func Regpreco(p *mpb.Progress) {
	modules.LimpaTabela("regpreco")
	modules.LimpaTabela("regprecohis")
	modules.LimpaTabela("regprecodoc")

	cnxFdb, err := connection.ConexaoDestino()
	if err != nil {
		panic("Falha ao conectar com o banco de destino: " + err.Error())
	}
	defer cnxFdb.Close()

	tx, err := cnxFdb.Begin()
	if err != nil {
		fmt.Printf("erro ao iniciar transação: %v", err)
	}

	tx.Exec(`EXECUTE BLOCK AS  
                        BEGIN  
                        INSERT INTO REGPRECODOC (NUMLIC, CODATUALIZACAO, DTPRAZO, ULTIMA)  
                        SELECT DISTINCT A.NUMLIC, 0, DATEADD(1 YEAR TO A.DTHOM), 'S'  
                        FROM CADLIC A WHERE A.REGISTROPRECO = 'S'
                        AND NOT EXISTS(SELECT 1 FROM REGPRECODOC X  
                        WHERE X.NUMLIC = A.NUMLIC);  

                        INSERT INTO REGPRECO (COD, DTPRAZO, NUMLIC, CODIF, CADPRO, CODCCUSTO, ITEM, CODATUALIZACAO, QUAN1, VAUN1, VATO1, QTDENT, SUBEM, STATUS, ULTIMA)  
                        SELECT B.ITEM, DATEADD(1 YEAR TO A.DTHOM), B.NUMLIC, B.CODIF, B.CADPRO, B.CODCCUSTO, B.ITEM, 0, B.QUAN1, B.VAUN1, B.VATO1, 0, B.SUBEM, B.STATUS, 'S'  
                        FROM CADLIC A INNER JOIN CADPRO B ON (A.NUMLIC = B.NUMLIC) WHERE A.REGISTROPRECO = 'S' AND NOT EXISTS(SELECT 1 FROM REGPRECO X  
                        WHERE X.NUMLIC = B.NUMLIC AND X.CODIF = B.CODIF AND X.CADPRO = B.CADPRO AND X.CODCCUSTO = B.CODCCUSTO AND X.ITEM = B.ITEM);  

                        INSERT INTO REGPRECOHIS (NUMLIC, CODIF, CADPRO, CODCCUSTO, ITEM, CODATUALIZACAO, QUAN1, VAUN1, VATO1, SUBEM, STATUS, MOTIVO, MARCA, NUMORC, ULTIMA)  
                        SELECT B.NUMLIC, B.CODIF, B.CADPRO, B.CODCCUSTO, B.ITEM, 0, B.QUAN1, B.VAUN1, B.VATO1, B.SUBEM, B.STATUS, B.MOTIVO, B.MARCA, B.NUMORC, 'S'  
                        FROM CADLIC A INNER JOIN CADPRO B ON (A.NUMLIC = B.NUMLIC) WHERE A.REGISTROPRECO = 'S' 
                        AND NOT EXISTS(SELECT 1 FROM REGPRECOHIS X  
                        WHERE X.NUMLIC = B.NUMLIC AND X.CODIF = B.CODIF AND X.CADPRO = B.CADPRO AND X.CODCCUSTO = B.CODCCUSTO AND X.ITEM = B.ITEM);  
                        END;`)
	tx.Commit()

	cnxFdb.Exec(`insert into cadprolic_detalhe_fic (numlic, item, codigo, qtd, valor, qtdadt, valoradt, codccusto, qtdmed, valormed, tipo) 
		select numlic, item, '0', sum(quan1) qtd, sum(vato1) valor, sum(qtdadt) qtdadt, sum(vatoadt) valoradt, codccusto, sum(quan1) qtdmed, sum(vato1) valormed, 'C' from cadpro a where numlic in 
		(select numlic from cadlic where liberacompra='S' and registropreco = 'S') and subem = 1 AND NOT EXISTS (SELECT 1 FROM cadprolic_detalhe_fic x WHERE a.numlic = x.numlic and a.item = x.item)
		GROUP BY numlic, item, codccusto`)
}