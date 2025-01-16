package compras

import (
	"MGFSiga/connection"
	"MGFSiga/modules"
	"database/sql"
	"fmt"
	"strings"

	"github.com/vbauerster/mpb"
)

func Cadunimedida(cnxSqls *sql.DB, cnxFdb *sql.DB, p *mpb.Progress) {
	modules.LimpaTabela("cadunimedida")
	tx, err := cnxFdb.Begin()
	if err != nil {
		fmt.Printf("erro ao iniciar transação: %v", err)
	}
	defer tx.Commit()

	insert, err := tx.Prepare("INSERT INTO CADUNIMEDIDA(sigla, descricao) VALUES(?,?)")
	if err != nil {
		fmt.Printf("Erro ao preparar insert: %v", err)
	}
	

	query := "SELECT DISTINCT rtrim(SUBSTRING(unidade, 1, 5)) AS sigla, rtrim(unidade) as unidade FROM EspecificacaoMaterialOuServico emos"
	rows, err := cnxSqls.Query(query)
	if err != nil {
		fmt.Printf("Erro ao obter linhas: %v", err)
	}
	defer rows.Close()

	totalLinhas, err := modules.CountRows(query, cnxFdb)
	if err != nil {
		fmt.Printf("erro ao contar linhas: %v", err)
	}
	barCadunimedida := modules.NewProgressBar(p, totalLinhas, "CADUNIMEDIDA")
	
	for rows.Next() {
		var (
			sigla string
			unidade string
		)

		if err := rows.Scan(&sigla, &unidade); err != nil {
			fmt.Printf("Erro ao scanear valores: %v", err)
		}

		_, err := insert.Exec(sigla, unidade)
		if err != nil {
			fmt.Printf("Erro ao inserir em CADUNIMEDIDA: %v", err)
		}
		barCadunimedida.Increment()
	}
}

func GrupoSubgrupo(cnxSqls *sql.DB, cnxFdb *sql.DB, p *mpb.Progress) {
	modules.LimpaTabela("cadsubgr")
	modules.LimpaTabela("cadgrupo")

	modules.NewCol("CADGRUPO", "ID_ANT", "varchar(6)")
	modules.NewCol("CADSUBGR", "ID_ANT", "varchar(6)")

	tx, err := cnxFdb.Begin()
	if err != nil {
		fmt.Printf("erro ao iniciar transação: %v", err)
	}
	
	insert, err := tx.Prepare("INSERT INTO CADGRUPO(grupo, nome, ocultar, id_ant) VALUES(?,?,?,?)")
	if err != nil {
		fmt.Printf("Erro ao preparar insert: %v", err)
	}
	
	query := `select DISTINCT 
		FORMAT(CAST(REPLACE(dbo.Grupo.IdGrupo, '0', '') AS INT),
		'000') grupo,
		substring(descricao,1,45) nome,
		'N' ocultar,
		IdGrupo
	from
		grupo`

	rows, err := cnxSqls.Query(query)
	if err != nil {
		fmt.Printf("erro ao obter linhas: %v", err)
	}

	totalLinhas, err := modules.CountRows(query, cnxFdb)
	if err != nil {
		fmt.Printf("erro ao contar linhas: %v", err)
	}
	barGrupo := modules.NewProgressBar(p, totalLinhas, "CADUNIMEDIDA")

	for rows.Next() {
		var (
			grupo string
			descricao string
			ocultar string
			id_ant string
		)

		if err := rows.Scan(&grupo, &descricao, &ocultar, &id_ant); err != nil {
			fmt.Printf("Erro ao scanear valores: %v", err)
		}

		if _, err := insert.Exec(grupo, descricao, ocultar, id_ant); err != nil {
			fmt.Printf("Erro ao inserir em CADGRUPO: %v", err)
		}
		barGrupo.Increment()
	}
	tx.Commit()
	if _, err := cnxFdb.Exec("INSERT INTO cadsubgr (grupo, SUBGRUPO, nome, ocultar, id_ant) SELECT GRUPO, '000', nome, ocultar, id_ant FROM CADGRUPO"); err != nil {
		fmt.Printf("Erro ao inserir em CADSUBGR: %v", err)
	}
}

func Cadest(cnxSqls *sql.DB, cnxFdb *sql.DB, p *mpb.Progress) {
	modules.LimpaTabela("cadest")

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
		rtrim(substring(unidade, 1, 5)) unidade,
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

	totalLinhas, err := modules.CountRows(query, cnxSqls)
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

		if _, err := insert.Exec(cadpro, grupoSubgrupoSeparado[0], grupoSubgrupoSeparado[1], codigoString, descricao, tipopro, unidade, especificacao, idEspecificacao, "N", balcoTce, balcoTceSaida); err != nil {
			fmt.Printf("Erro ao inserir em CADEST: %v", err)
		}
		barCadest.Increment()
	}
	fmt.Print("Acabou")
}

func Destino(p *mpb.Progress) {
	modules.LimpaTabela("caddestino")

	tx, err := connection.ConexaoFdb.Begin()
	if err != nil {
		fmt.Printf("erro ao iniciar transação: %v", err)
	}
	defer tx.Commit()

	insert, err := tx.Prepare("INSERT INTO DESTINO(COD, DESTI, EMPRESA) VALUES(?,?,?)")
	if err != nil {
		fmt.Printf("Erro ao preparar insert: %v", err)
	}

	
}