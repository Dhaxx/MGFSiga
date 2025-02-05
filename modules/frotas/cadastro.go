package frotas

import (
	"MGFSiga/connection"
	"MGFSiga/modules"
	"database/sql"
	"fmt"
	"time"

	"github.com/vbauerster/mpb"
)

func TipoVeiculo(p *mpb.Progress) {
	modules.LimpaTabela("VEICULO_TIPO")
	cnxFdb, err := connection.ConexaoDestino()
	if err != nil {
		panic(err)
	}
	defer cnxFdb.Close()

	cnxSqls, err := connection.ConexaoOrigem()
	if err != nil {
		panic(err)
	}
	defer cnxSqls.Close()

	insert, err := cnxFdb.Prepare(`insert into veiculo_tipo(codigo_tip, descricao_tip) values(?, ?)`)
	if err != nil {
		panic(err)
	}

	query := `select cast(idtipodeveiculo as int) codigo_tip, descricaotipodeveiculo descricao_tip from MGFFrota.dbo.tiposdeveiculos`

	rows, err := cnxSqls.Query(query)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	totalLinhas, err := modules.CountRows(query)
	if err != nil {
		panic(err)
	}

	barTipoVeiculos := modules.NewProgressBar(p, totalLinhas, "VEICULO_TIPO")

	for rows.Next() {
		var (
			codigo_tip int
			descricao_tip string
		)

		err := rows.Scan(&codigo_tip, &descricao_tip)
		if err != nil {
			panic(err)
		}

		_, err = insert.Exec(codigo_tip, descricao_tip)
		if err != nil {
			panic(err)
		}
		barTipoVeiculos.Increment()
	}
}

func Marcas(p *mpb.Progress) {
	modules.LimpaTabela("VEICULO_MARCA")
	cnxFdb, err := connection.ConexaoDestino()
	if err != nil {
		panic(err)
	}
	defer cnxFdb.Close()

	cnxSqls, err := connection.ConexaoOrigem()
	if err != nil {
		panic(err)
	}
	defer cnxSqls.Close()

	query := `select
		distinct concat('insert into VEICULO_MARCA (codigo_mar, descricao_mar) values (', DENSE_RANK() over (order by MarcaFabricante), ', ''', ve.MarcaFabricante, ''')')
	from
		MGFFrota.dbo.VeiculosEEquipamentos ve`

	rows, err := cnxSqls.Query(query)
	if err != nil {
		panic(err)
	}

	for rows.Next() {
		var insert string
		err := rows.Scan(&insert)
		if err != nil {
			panic(err)
		}
		cnxFdb.Exec(insert)
	}
}

func Veiculos(p *mpb.Progress) {
	modules.LimpaTabela("VEICULO")
	cnxFdb, err := connection.ConexaoDestino()
	if err != nil {
		panic(err)
	}
	defer cnxFdb.Close()

	cnxSqls, err := connection.ConexaoOrigem()
	if err != nil {
		panic(err)
	}
	defer cnxSqls.Close()

	query := `WITH PlacasNumeradas AS (
	SELECT
				CASE
					WHEN placa IN ('.', '0') THEN SUBSTRING(DescricaoVeiculo, 1, 7)
			ELSE SUBSTRING(REPLACE(placa, '-', ''), 1, 7)
		END AS placa_original,
				ve.DescricaoVeiculo AS modelo,
				CASE
					WHEN ve.IdTipoDeVeiculo IN ('0001', '0005', '0008') THEN 'D'
			WHEN ve.IdTipoDeVeiculo IN ('0002', '0006', '0007') THEN 'G'
			WHEN ve.IdTipoDeVeiculo IN ('0003') THEN 'E'
			ELSE 'F'
		END AS combustivel,
				ve.AnoFabricacao,
				ve.Renavan,
				NumeroDeSerie AS chassi,
				MarcaFabricante,
				tipoVeiculo,
				dataCompraVeiculo,
				format(cast(idBem as int),
			'000000') chapa,
				CapacidadeDoTanque,
				ROW_NUMBER() OVER (PARTITION BY 
						CASE
					WHEN placa IN ('.', '0') THEN SUBSTRING(DescricaoVeiculo, 1, 7)
			ELSE SUBSTRING(REPLACE(placa, '-', ''), 1, 7)
		END
	ORDER BY
				ve.DescricaoVeiculo) AS numero,
						case 
					when status = 1 then 0
			else 1
		end inativo,
			idVeiculo
	FROM
				MGFFrota.dbo.VeiculosEEquipamentos ve
			)
			SELECT
				SUBSTRING(
				CASE
					WHEN numero = 1 THEN placa_original
					ELSE CONCAT(SUBSTRING(placa_original, 1, 6), numero)
				END, 1, 7) AS placa,
				substring(modelo, 0, 45) as modelo,
				combustivel,
				AnoFabricacao,
				Renavan,
				chassi,
				MarcaFabricante,
				tipoVeiculo,
				dataCompraVeiculo,
				chapa,
				CapacidadeDoTanque,
				inativo,
				idVeiculo
	FROM
				PlacasNumeradas
	ORDER BY
				placa`

	rows, err := cnxSqls.Query(query)
	if err != nil {
		panic(err)
	}

	// totalLinhas, err := modules.CountRows(query)
	// if err != nil {
	// 	panic(err)
	// }

	// barVeiculos := modules.NewProgressBar(p, totalLinhas, "VEICULO")

	for rows.Next() {
		var (
			placa, modelo, combustivel, AnoFabricacao, Renavan, chassi, stringMarca, tipoVeiculo, dataAquisicao, chapa, sequencia string
			codigoMarca, capacidadeTanque, inativoStatus int
		)

		err := rows.Scan(&placa, &modelo, &combustivel, &AnoFabricacao, &Renavan, &chassi, &stringMarca, &tipoVeiculo, &dataAquisicao, &chapa, &capacidadeTanque, &inativoStatus, &sequencia)
		if err != nil {
			panic(err)
		}

		dataAquisicaoParseada, err := time.Parse(time.RFC3339, dataAquisicao)
		if err != nil {
			fmt.Printf("erro ao parsear string para data: %v", err)
		}

		dataAquisicaoFormatada := dataAquisicaoParseada.Format("02.01.2006")

		cnxFdb.QueryRow(`select codigo_mar from veiculo_marca where descricao_mar containing ?`, stringMarca).Scan(&codigoMarca)	

		_, err = cnxFdb.Exec(`insert into veiculo(placa, modelo, combustivel, anomod, renavam, chassi, codigo_marca_vei, codigo_tipo_vei, aquisicao, codigo_bem_par, tanque, inativo, sequencia) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, placa, modelo, combustivel, AnoFabricacao, Renavan, chassi, codigoMarca, tipoVeiculo, dataAquisicaoFormatada, chapa, capacidadeTanque, inativoStatus, sequencia)
		if err != nil {
			panic(err)
		}
		// barVeiculos.Increment()
	}
}

func Abastecimento(p *mpb.Progress) {
	modules.LimpaTabela("icadreq where id_requi <> 0")
	modules.LimpaTabela("requi where id_requi <> 0")

	tiposCombustiveis := map[string]string{
		"GASOLINA": "026.001.331",
		"DIESEL": "026.001.997",
		"DIESEL S10": "026.004.876",
	}

	cnxFdb, err := connection.ConexaoDestino()
	if err != nil {
		panic(err)
	}
	defer cnxFdb.Close()

	cnxSqls, err := connection.ConexaoOrigem()
	if err != nil {
		panic(err)
	}

	tx, err := cnxFdb.Begin()
	if err != nil {
		panic(err)
	}

	modules.CriaCentroDeCustoDoFrotas()

	query := `select
		idveiculo,
		concat(IdAbastecimento,anoAbastecimento%2000) idRequi,
		format(cast(a.IdAbastecimento as int), '000000')+'/'+cast(a.anoAbastecimento%2000 as varchar) requi,
		format(cast(a.IdAbastecimento as int), '000000') num,
		a.AnoAbastecimento ano,
		'000000999' destino,
		999 codccusto,
		a.DataDoAbastecimento as data,
		'S' entr,
		'S' said,
		'P' comp,
		'ABASTECIMENTO Nº '+a.IdAbastecimento obs,
		b.NomeFornecedor,
		case 
			when idtipodecombustivel = 32 then 'DIESEL S10'
			when idtipodecombustivel = 1 then 'DIESEL'
			when idtipodecombustivel = 2 then 'GASOLINA'		
		end as combust,
		a.Quantidade,
		a.Valor,
		a.NumeroNotaFiscal,
		a.Tacometro
	from
		MGFFrota.dbo.Abastecimentos a 
	join MGFFrota.dbo.FornecedoresDeCombustivel b on
		a.idFornecedor = b.IdFornecedor`

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
		dtpag,
		entr,
		said,
		comp,
		obs,
		codif,
		docum)
	VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		fmt.Printf("erro ao preparar insert: %v", err)
	}

	cabecalhos, err := cnxSqls.Query(fmt.Sprintf("select distinct idRequi, requi, num, ano, destino, codccusto, data, entr, said, comp, obs, nomeFornecedor, NumeroNotaFiscal from (%v) subquery", query))
	if err != nil {
		fmt.Printf("erro ao obter cabecalhos: %v", err)
	}

	for cabecalhos.Next() {
		var (
			requi, num, ano, destino, data, entr, said, comp, obs, nomeFornecedor, idRequi, nf string
			codif, codccusto int
		)

		if err := cabecalhos.Scan(&idRequi, &requi, &num, &ano, &destino, &codccusto, &data, &entr, &said, &comp, &obs, &nomeFornecedor, &nf); err != nil {
			fmt.Printf("erro ao scanear cabecalhos: %v", err)
		}

		dataParseada, err := time.Parse(time.RFC3339, data)
		if err != nil {
			fmt.Printf("erro ao parsear string para data: %v", err)
		}
		dataFormatada := dataParseada.Format("2006-01-02")

		nomeFornecedorConvertido1252, err := modules.DecodeToWin1252(nomeFornecedor)
		if err != nil {
			fmt.Printf("Erro ao converter descrição para Win1252: %v", err)
		}

		err = tx.QueryRow(fmt.Sprintf("select codif from desfor where nome containing '%v'", nomeFornecedorConvertido1252)).Scan(&codif)
		if err != nil {
			if err == sql.ErrNoRows {
				codif = modules.Cache.Codif["0"]
			} else {
				panic("Erro ao buscar codif "+ err.Error())
			}
		}

		obsConvertida1252, err := modules.DecodeToWin1252(obs)
		if err != nil {
			fmt.Printf("Erro ao converter descrição para Win1252: %v", err)
		}

		_, err = insertRequi.Exec(modules.Cache.Empresa, idRequi, requi, num, ano, destino, codccusto, dataFormatada, dataFormatada, dataFormatada, entr, said, comp, obsConvertida1252, codif, nf)
		if err != nil {
			fmt.Printf("erro ao executar insert: %v", err)
		}
	}
	tx.Commit()

	insertIcadreq, err := cnxFdb.Prepare(`insert into icadreq (id_requi, requi, codccusto, empresa, placa, item, quan1, quan2, vaun1, vaun2, vato1, vato2, cadpro, destino, km) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		fmt.Printf("erro ao preparar insert: %v", err)
	}

	itens, err := cnxSqls.Query(fmt.Sprintf("select distinct cast(idveiculo as int) idveiculo, idRequi, requi, codccusto, combust, quantidade, valor, Tacometro from (%v) subquery", query))
	if err != nil {
		fmt.Printf("erro ao executar query: %v", err)
	}

	for itens.Next() {
		var (
			requi, combust, cadpro, placa string
			codccusto, idVeiculo, idRequi int
			quantidade, valor, kilometragem float64
		)

		err = itens.Scan(&idVeiculo, &idRequi, &requi, &codccusto, &combust, &quantidade, &valor, &kilometragem)
		if err != nil {
			fmt.Printf("erro ao fazer scan: %v", err)
		}

		cadpro = tiposCombustiveis[combust]
		placa = modules.Cache.Placa[idVeiculo]
		precoUnitario := valor/quantidade

		_, err = insertIcadreq.Exec(idRequi, requi, codccusto, modules.Cache.Empresa, placa, 1, quantidade, quantidade, precoUnitario, precoUnitario, valor, valor, cadpro, "000000999", kilometragem)
		if err != nil {
			fmt.Printf("erro ao executar insert: %v", err)
		}
	}
}

func Portaria(p *mpb.Progress) {
	modules.LimpaTabela("PORTARIA")
	cnxFdb, err := connection.ConexaoDestino()
	if err != nil {
		panic(err)
	}
	defer cnxFdb.Close()

	cnxSqls, err := connection.ConexaoOrigem()
	if err != nil {
		panic(err)
	}

	tx, err := cnxFdb.Begin()
	if err != nil {
		panic(err)
	}
	defer tx.Commit()

	query := `select
		idOperacao,
		cast(idVeiculo as int) idVeiculo,
		o.DataOperacao,
		o.InicioOperacao,
		o.FimOperacao,
		descricaoServico,
		o.DetalhesDaOperacao
	from
		MGFFrota.dbo.Operacoes o
	join MGFFrota.dbo.servicos s on
		o.IdServico = s.idServico`

	insert, err := tx.Prepare(`INSERT
	INTO
	portaria (
	codmotor,
	codigo,
	placa,
	saida,
	entrada,
	kmini,
	kmfim,
	obs,
	obs_chegada) VALUES (?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		panic(err)
	}

	rows, err := cnxSqls.Query(query)
	if err != nil {
		panic(err)
	}

	totalLinhas, err := modules.CountRows(query)
	if err != nil {
		panic(err)
	}

	barPortaria := modules.NewProgressBar(p, totalLinhas, "PORTARIA")

	for rows.Next() {
		var (
			idOperacao, descricaoServico, dataOperacao, detalhes string
			idVeiculo int
			kmInicio, kmFim float32
		)

		err := rows.Scan(&idOperacao, &idVeiculo, &dataOperacao, &kmInicio, &kmFim, &descricaoServico, &detalhes)
		if err != nil {
			panic(err)
		}

		placa := modules.Cache.Placa[idVeiculo]
		
		dataParseada, err := time.Parse(time.RFC3339, dataOperacao)
		if err != nil {
			fmt.Printf("erro ao parsear string para data: %v", err)
		}
		dataFormatada := dataParseada.Format("2006-01-02")
		
		descricaoServicoConvertida1252, err := modules.DecodeToWin1252(descricaoServico)
		if err != nil {
			fmt.Printf("Erro ao converter descrição para Win1252: %v", err)
		}
		
		_, err = insert.Exec(0, idOperacao, placa, dataFormatada, dataFormatada, kmInicio, kmFim, descricaoServicoConvertida1252, detalhes)
		if err != nil {
			panic(err)
		}

		barPortaria.Increment()
	}
}