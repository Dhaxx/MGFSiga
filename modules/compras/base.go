package compras

import (
	"MGFSiga/connection"
	"MGFSiga/modules"
	"fmt"
	"strings"

	"github.com/vbauerster/mpb"
)

func Cadunimedida(p *mpb.Progress) {
	modules.LimpaTabela("cadunimedida")

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

	insert, err := cnxFdb.Prepare("INSERT INTO CADUNIMEDIDA(sigla, descricao) VALUES(?,?)")
	if err != nil {
		fmt.Printf("Erro ao preparar insert: %v", err)
	}

	query := "SELECT DISTINCT rtrim(unidade) as unidade FROM EspecificacaoMaterialOuServico emos"
	rows, err := cnxSqls.Query(query)
	if err != nil {
		fmt.Printf("Erro ao obter linhas: %v", err)
	}
	defer rows.Close()

	totalLinhas, err := modules.CountRows(query)
	if err != nil {
		fmt.Printf("erro ao contar linhas: %v", err)
	}
	barCadunimedida := modules.NewProgressBar(p, totalLinhas, "CADUNIMEDIDA")
	
	for rows.Next() {
		var (
			unidade string
		)

		if err := rows.Scan(&unidade); err != nil {
			fmt.Printf("Erro ao scanear valores: %v", err)
		}

		unidadeConvertidaWin1252, err := modules.DecodeToWin1252(unidade)
		if err != nil {
			fmt.Printf("erro ao converter unidade para win1252: %v", err)
		}

		sigla := unidadeConvertidaWin1252
		if len(sigla) > 5 {
			sigla = sigla[:5]
		}

		_, err = insert.Exec(sigla, unidadeConvertidaWin1252)
		if err != nil {
			fmt.Printf("Erro ao inserir em CADUNIMEDIDA: %v", err)
		}
		barCadunimedida.Increment()
	}
}

func GrupoSubgrupo(p *mpb.Progress) {
	modules.LimpaTabela("cadsubgr")
	modules.LimpaTabela("cadgrupo")
	modules.NewCol("CADGRUPO", "ID_ANT", "varchar(6)")
	modules.NewCol("CADSUBGR", "ID_ANT", "varchar(6)")

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

	query := `select DISTINCT 
		case when IdGrupo = 0 then 'CONVERSÃO' ELSE substring(descricao,1,45) end nome,
		IdGrupo
	from
		grupo`

	insert, err := tx.Prepare("INSERT INTO CADGRUPO(grupo, nome, ocultar, id_ant) VALUES(?,?,?,?)")
	if err != nil {
		fmt.Printf("Erro ao preparar insert: %v", err)
	}

	rows, err := cnxSqls.Query(query)
	if err != nil {
		fmt.Printf("erro ao obter linhas: %v", err)
	}

	totalLinhas, err := modules.CountRows(query)
	if err != nil {
		fmt.Printf("erro ao contar linhas: %v", err)
	}
	barGrupo := modules.NewProgressBar(p, totalLinhas, "CADUNIMEDIDA")

	var i int

	for rows.Next() {
		var (
			descricao string
			id_ant string
		)

		i++

		if err := rows.Scan(&descricao, &id_ant); err != nil {
			fmt.Printf("Erro ao scanear valores: %v", err.Error())
		}

		grupo := fmt.Sprintf("%03d", i)
		
		descricaoConvertidoWin1252, err := modules.DecodeToWin1252(descricao)
		if err != nil {
			fmt.Printf("erro ao decodificar descricao para win1252: %v", err)
		}

		if _, err := insert.Exec(grupo, descricaoConvertidoWin1252, "N", id_ant); err != nil {
			fmt.Printf("Erro ao inserir em CADGRUPO: %v", err)
		}
		barGrupo.Increment()
	}
	tx.Commit()
	if _, err := cnxFdb.Exec("INSERT INTO cadsubgr (grupo, SUBGRUPO, nome, ocultar, id_ant) SELECT GRUPO, '000', nome, ocultar, id_ant FROM CADGRUPO"); err != nil {
		fmt.Printf("Erro ao inserir em CADSUBGR: %v", err)
	}
}

func Cadest(p *mpb.Progress) {
	modules.LimpaTabela("cadest")

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

	insert, err := tx.Prepare(`INSERT
								INTO
								Cadest(cadpro,
								grupo,
								subgrupo,
								codigo,
								disc1,
								tipopro,
								unid1,
								discr1,
								codreduz,
								ocultar,
								balco_tce,
								balco_tce_saida)
							VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		fmt.Printf("Erro ao preparar insert: %v", err)
	}
	
	query := `select
		cast(idEspecificacao as integer) id,
		idEspecificacao,
		idGrupo,
		descricao,
		especificacao,
		contaDoAtivo balcoTce,
		VPD balcoTceSaida,
		rtrim(unidade) unidade,
		case 
			when idSubTipoDeProduto = 5 then 'S'
			else 'P'
		end tipopro
	from
		dbo.EspecificacaoMaterialOuServico emos`
	
	rows, err := cnxSqls.Query(query)
	if err != nil {
		fmt.Printf("erro ao obter linhas: %v", err)
	}

	totalLinhas, err := modules.CountRows(query)
	if err != nil {
		fmt.Printf("erro ao contar linhas: %v", err)
	}
	barCadest := modules.NewProgressBar(p, totalLinhas, "CADEST")

	for rows.Next() {
		var idEspecificacao, idAnt, descricao, especificacao, balcoTce, balcoTceSaida, unidade, tipopro, codigoString string
		var codigo int

		if err = rows.Scan(&codigo, &idEspecificacao, &idAnt, &descricao, &especificacao, &balcoTce, &balcoTceSaida, &unidade, &tipopro); err != nil {
			fmt.Printf("Erro ao scanear valores: %v", err)
		}

		grupoSubgrupo := modules.Cache.Subgrupos[idAnt]
		if grupoSubgrupo == "" {
			grupoSubgrupo = modules.CriaGrupoSubgrupo(idAnt)
		}

		if codigo >= 1000 {
			grupoSubgrupo, err = modules.EstourouSubGrupo(codigo, grupoSubgrupo, idAnt)
			if err != nil {
				fmt.Printf("erro: %v", err)
			}
			codigoString = fmt.Sprintf("%03d", codigo%1000)
		} else {
			codigoString = fmt.Sprintf("%03d", codigo)
		}

		cadpro := fmt.Sprintf("%v.%v",grupoSubgrupo, codigoString)
		grupoSubgrupoSeparado := strings.Split(grupoSubgrupo, ".")

		descricaoConvertidoWin1252, err := modules.DecodeToWin1252(descricao)
		if err != nil {
			fmt.Printf("erro ao decodificar descricao: %v", err)
		}

		unidadeConvertidaWin1252, err := modules.DecodeToWin1252(unidade)
		if err != nil {
			fmt.Printf("erro ao decodificar descricao: %v", err)
		}

		unidadeMedida := modules.Cache.Medidas[unidadeConvertidaWin1252]

		if _, err := insert.Exec(cadpro, grupoSubgrupoSeparado[0], grupoSubgrupoSeparado[1], codigoString, descricaoConvertidoWin1252, tipopro, unidadeMedida, especificacao, idEspecificacao, "N", balcoTce, balcoTceSaida); err != nil {
			fmt.Printf("Erro ao inserir em CADEST: %v", err)
		}
		barCadest.Increment()
	}
	fmt.Print("Acabou")
}

func CentroCusto(p *mpb.Progress) {
	modules.LimpaTabela("centrocusto")

	modules.NewCol("centrocusto", "id_ant", "varchar(6)")

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
		centrocusto (poder,
		orgao,
		destino,
		ccusto,
		descr,
		codccusto,
		empresa,
		ocultar,
		id_ant)
	values (?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		fmt.Printf("erro ao preparar insert: %v", err)
	}

	query := `select
		'01' poder,
		'03' orgao,
		right(replicate('0', 9)+IdCCusto,9) destino,
		1 ccusto,
		substring(CASE
			WHEN c.DescricaoCCusto = '' THEN 'CONVERSAO'
			ELSE c.DescricaoCCusto 
		END, 0, 60) AS descricao,
		cast(c.IdCCusto as int) codccusto,
		c.IdCCusto
	from
		MGFEstoq.dbo.CCusto c`

	rows, err := cnxSqls.Query(query)
	if err != nil {
		fmt.Printf("erro ao obter linhas: %v", err)
	}
	
	totalLinhas, err := modules.CountRows(query)
	if err != nil {
		fmt.Printf("erro ao contar linhas: %v", err)
	}
	barCcusto := modules.NewProgressBar(p, totalLinhas, "CENTRO DE CUSTO")

	for rows.Next() {
		var (
			poder, orgao, destino, descricao, id_ant string
			ccusto, codccusto int
		)

		if err := rows.Scan(&poder, &orgao, &destino, &ccusto, &descricao, &codccusto, &id_ant); err != nil {
			fmt.Printf("erro ao scanear valores: %v", err)
		}

		descricaoConvertidoWin1252, err := modules.DecodeToWin1252(descricao)
		if err != nil {
			fmt.Printf("erro ao decodificar: %v", err)
		}

		if len(descricaoConvertidoWin1252) > 64 {
			descricaoConvertidoWin1252 = descricaoConvertidoWin1252[:64]
		}

		if _, err := insert.Exec(poder, orgao, destino, ccusto, descricaoConvertidoWin1252, codccusto, modules.Cache.Empresa, "N", id_ant); err != nil {
			fmt.Printf("erro ao inserir registro: %v", err)
		}

		if _, err := tx.Exec(fmt.Sprintf("INSERT INTO DESTINO(COD, DESTI, EMPRESA) VALUES('%v','%v',%v)", destino, descricaoConvertidoWin1252, modules.Cache.Empresa)); err != nil {
			fmt.Printf("erro ao inserir almoxarifado: %v", err)
		}
		
		barCcusto.Increment()
	}
}