package patrimonio

import (
	"MGFSiga/connection"
	"MGFSiga/modules"
	"database/sql"
	"fmt"
	"time"

	"github.com/vbauerster/mpb"
)

func Cadpat(p *mpb.Progress) {
	modules.LimpaTabela("pt_cadpat")

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
		fmt.Printf("Erro ao iniciar transação: %v", err)
	}
	defer tx.Commit()

	insert, err := tx.Prepare(`INSERT
		INTO
		pt_cadpat (codigo_pat,
		empresa_pat,
		codigo_gru_pat,
		chapa_pat,
		codigo_cpl_pat,
		codigo_set_pat,
		codigo_set_atu_pat,
		orig_pat,
		codigo_tip_pat,
		codigo_sit_pat,
		discr_pat,
		obs_pat,
		datae_pat,
		dtlan_pat,
		valaqu_pat,
		valatu_pat,
		codigo_for_pat,
		percenqtd_pat,
		dae_pat,
		valres_pat,
		percentemp_pat,
		nempg_pat,
		anoemp_pat,
		hash_sinc,
		codigo_bai_pat)
	VALUES(?,?,?,?,
	?,?,?,?,?,?,?,?,
	?,?,?,?,?,?,?,?,
	?,?,?,?,?);`)
	if err != nil { 
		fmt.Printf("Erro ao preparar insert: %v", err)
	}

	query := `select
		cast(idBem as int) codigo_pat,
		case
			when b.idClasse like '1231%' then 1
			when b.idClasse like '1232%' then 2
			else 3
		end codigo_gru_pat,
		right(idBem,
		6) chapa_pat,
		substring(b.idclasse, 1, 9) codigo_cpl_pat,
		idLocal codigo_set_pat,
		substring(tde.Descricao, 1, 1) orig_pat,
		substring(c.descricao, 0, 60) tip_pat,
		conservacao codigo_sit_pat,
		DescricaoResumida discr_pat,
		b.DescricaoTecnica obs_pat,
		DataReferencia datae_pat,
		DataIncorporacao dtlan_pat,
		ValorOriginal valaqu_pat,
		ValorAtual valatu_pat,
		Fornecedor,
		'V' dae_pat,
		valorResidual,
		numeroEmpenho,
		anoEmpenho,
		case 
			when situacao = 1 then 1
		end codigo_bai_pat
	from
		MGFPatri.dbo.bens b
	join MGFPatri.dbo.TipoDocumentoEntrada tde 
	on
		tde.IdTipoEntrada = b.idtipoEntrada
	left join MGFPatri.dbo.Classes c on
		c.idClasse = b.idClasse`

	totalLinhas, err := modules.CountRows(query)
	if err != nil {
		fmt.Printf("Erro ao contar linhas: %v", err)
	}

	barCadpat := modules.NewProgressBar(p, totalLinhas, "CADPAT")

	rows, err := cnxSqls.Query(query)
	if err != nil {
		fmt.Printf("Erro ao executar query: %v", err)
	}

	for rows.Next() {
		var (
			codigoPat, codigoGrupat, codigoSetPat, codigoSitPat, nempg, anoemp, codif, codTip int
			chapaPat, codigoCplPat, origPat, discrPat, obsPat, dataePat, dtlanPat, fornecedor, daePat, tipPat string
			valaquPat, valatuPat, valresPat float32 
			codigoBaiPat sql.NullInt64
		)

		err := rows.Scan(&codigoPat, &codigoGrupat, &chapaPat, &codigoCplPat, &codigoSetPat, &origPat, &tipPat, &codigoSitPat, &discrPat, &obsPat, &dataePat, &dtlanPat, &valaquPat, &valatuPat, &fornecedor, &daePat, &valresPat, &nempg, &anoemp, &codigoBaiPat)
		if err != nil {
			fmt.Printf("Erro ao escanear valores: %v", err)
		}

		descricaoConvertido1252, err := modules.DecodeToWin1252(discrPat)
		if err != nil {
			fmt.Printf("Erro ao converter descrição para Win1252: %v", err)
		}	
		// obsPatConvertido1252, err := modules.DecodeToWin1252(obsPat)
		// if err != nil {
		// 	fmt.Printf("Erro ao converter descrição para Win1252: %v", err)
		// }

		fornecedorConvertido1252, err := modules.DecodeToWin1252(fornecedor)
		if err != nil {
			fmt.Printf("Erro ao converter descrição para Win1252: %v", err)
		}

		err = tx.QueryRow(fmt.Sprintf("select codif from desfor where nome containing '%v'", fornecedorConvertido1252)).Scan(&codif)
		if err != nil {
			if err == sql.ErrNoRows {
				codif = modules.Cache.Codif["0"]
			} else {
				panic("Erro ao buscar codif "+ err.Error())
			}
		}

		tipPatConvertido1252, err := modules.DecodeToWin1252(tipPat)
		if err != nil {
			fmt.Printf("Erro ao converter descrição para Win1252: %v", err)
		}

		err = cnxFdb.QueryRow(fmt.Sprintf("select codigo_tip from pt_cadtip where descricao_tip containing '%v'", tipPatConvertido1252)).Scan(&codTip)
		if err != nil {
			fmt.Printf("Erro ao buscar codigo_tip_pat: %v", err)
		}

		dtlanPatParseada, err := time.Parse(time.RFC3339, dtlanPat)
		if err != nil {
			fmt.Printf("erro ao parsear string para data: %v", err)
		}
		dtlanPatFormatada := dtlanPatParseada.Format("02.01.2006")

		dataePatParseada, err := time.Parse(time.RFC3339, dataePat)
		if err != nil {
			fmt.Printf("erro ao parsear string para data: %v", err)
		}
		dataePatFormatada := dataePatParseada.Format("02.01.2006")

		_, err = insert.Exec(codigoPat, modules.Cache.Empresa, codigoGrupat, chapaPat, codigoCplPat, codigoSetPat, codigoSetPat, origPat, codTip, codigoSitPat, descricaoConvertido1252, obsPat, dataePatFormatada, dtlanPatFormatada, valaquPat, valatuPat, codif, 0, daePat, valresPat, 0, nempg, anoemp, codigoPat, codigoBaiPat.Int64)
		if err != nil {
			fmt.Printf("Erro ao inserir valores: %v", err)
		}
		barCadpat.Increment()
	}
}

func Cadresponsavel(p *mpb.Progress) {
	modules.LimpaTabela("pt_cadresponsavel")

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
		fmt.Printf("Erro ao iniciar transação: %v", err)
	}
	defer tx.Commit()

	insert, err := tx.Prepare(`INSERT
		INTO
		pt_cadresponsavel (codigo_resp,
		nome_resp)
	VALUES(?,?);`)
	if err != nil {
		fmt.Printf("Erro ao preparar insert: %v", err)
	}

	query := `select * from MGFPatri.dbo.Responsaveis r `

	totalLinhas, err := modules.CountRows(query)
	if err != nil {
		fmt.Printf("Erro ao contar linhas: %v", err)
	}

	barCadresponsavel := modules.NewProgressBar(p, totalLinhas, "CADRESPONSAVEL")

	rows, err := cnxSqls.Query(query)
	if err != nil {
		fmt.Printf("Erro ao executar query: %v", err)
	}

	for rows.Next() {
		var (
			codigoRes int
			nomeRes string
		)

		err := rows.Scan(&codigoRes, &nomeRes)
		if err != nil {
			fmt.Printf("Erro ao escanear valores: %v", err)
		}

		nomeResConvertido1252, err := modules.DecodeToWin1252(nomeRes)
		if err != nil {
			fmt.Printf("Erro ao converter descrição para Win1252: %v", err)
		}

		_, err = insert.Exec(codigoRes, nomeResConvertido1252)
		if err != nil {
			fmt.Printf("Erro ao inserir valores: %v", err)
		}

		barCadresponsavel.Increment()
	}
}