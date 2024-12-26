package compras

import (
	"database/sql"
	"log"
	"sync"

	"github.com/vbauerster/mpb"
)

func Cadunimedida(cnxSqls *sql.DB, cnxFdb *sql.DB, p *mpb.Progress, wg *sync.WaitGroup) {
	defer wg.Done()
	defer cnxFdb.Close()

	insert, err := cnxFdb.Prepare("INSERT INTO CADUNIMEDIDA(sigla, descricao) VALUES(?,?)")
	if err != nil {
		log.Fatalf("Erro ao preparar insert: %v", err)
	}
	defer insert.Close()

	rows, err := cnxSqls.Query("SELECT DISTINCT SUBSTRING(unidade, 1, 5) AS sigla, unidade FROM EspecificacaoMaterialOuServico emos")
	if err != nil {
		log.Fatalf("Erro ao obter linhas: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var cadunimedida struct {
			sigla string
			unidade string
		}

		if err := rows.Scan(&cadunimedida.sigla, &cadunimedida.unidade); err != nil {
			log.Fatalf("Erro ao scanear valores: %v", err)
		}

		_, err := insert.Exec(cadunimedida)
		if err != nil {
			log.Fatalf("Erro ao inserir registros: %v", err)
		}
	}
}

func GrupoSubgrupo(cnxSqls *sql.DB, cnxFdb *sql.DB, p *mpb.Progress, wg *sync.WaitGroup) {
	defer wg.Done()
	defer cnxFdb.Close()
	
	inserts := map[string]string{
		"grupo":    "INSERT INTO CADGRUPO(grupo, nome, ocultar) VALUES(?,?,?)",
		"subgrupo": "INSERT INTO CADSUBGR(grupo, subgrupo, nome, ocultar) VALUES(?,?,?,?)",
	}

	rows, err := cnxFdb.Query(`SELECT
			FORMAT(CAST(REPLACE(dbo.Grupo.IdGrupo, '0', '') AS INT), '000') AS IdGrupoFormatado,
			dbo.Grupo.Descricao,
			FORMAT(CAST(REPLACE(SubGrupo.IdGrupo, '0', '') AS INT), '000') AS IdSubGrupoFormatado,
			SubGrupo.Descricao AS DescricaoSubGrupo
		FROM
			dbo.Grupo
		INNER JOIN
			dbo.Grupo AS SubGrupo
		ON
			LEFT(dbo.Grupo.IdGrupo, 3) = LEFT(SubGrupo.IdGrupo, 3);`)
	if err != nil {
		log.Fatalf("Erro ao obter linhas: %v", err)
	}
	defer rows.Close()

	grupoAnt := ""
	for rows.Next() {
		var cadgrupo struct{
			grupo string
			nome string
			ocultar string
		}
		var cadsubgr struct{
			grupo string
			subgrupo string
			nome string
			ocultar string
		}

		if cadgrupo.grupo != grupoAnt {
			if err := rows.Scan(&cadgrupo.grupo, &cadgrupo.nome, &cadgrupo.ocultar); err != nil {
				log.Fatalf("Erro ao scanear valores: %v", err)
			}
			cnxFdb.Exec(inserts["grupo"], cadgrupo.grupo, cadgrupo.nome, "N")
		}

		if err := rows.Scan(&cadsubgr.grupo, &cadsubgr.subgrupo, &cadsubgr.nome); err != nil {
			log.Fatalf("Erro ao scanear valores: %v", err)
		}

		cnxFdb.Exec(inserts["subgrupo"], cadsubgr.grupo, cadsubgr.subgrupo, cadsubgr.nome, "N")
	}
}