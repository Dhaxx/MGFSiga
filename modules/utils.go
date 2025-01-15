package modules

import (
	"MGFSiga/connection"
	"database/sql"
	"fmt"
	"strings"

	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"
)

var Cache struct {
	Subgrupos map[string]string
}

func init() {
	Cache.Subgrupos = make(map[string]string)
	NewCol("CADGRUPO", "ID_ANT", "varchar(6)")
	NewCol("CADSUBGR", "ID_ANT", "varchar(6)")

	rowsSubgrupos, err := connection.ConexaoFdb.Query("select id_ant, grupo||'.'||subgrupo from cadsubgr")
	if err != nil {
		if err == sql.ErrNoRows {
			fmt.Printf("Cadsubgr não possuem registros ainda: %v", err)
		}
		fmt.Printf("Erro ao obter subgrupos: %v", err)
	}
	defer rowsSubgrupos.Close()

	for rowsSubgrupos.Next() {
		var id_ant, grupoSubgrupo string
		if err := rowsSubgrupos.Scan(&id_ant, &grupoSubgrupo); err != nil {
			fmt.Printf("erro ao scanear valores: %v", err)
			continue
		}
		Cache.Subgrupos[id_ant] = grupoSubgrupo
	}
}

func LimpaTabela(tabela string) {
	tx, err := connection.ConexaoFdb.Begin()
	if err != nil {
		fmt.Printf("erro ao iniciar transação: %v", err)
	}
	defer tx.Commit()

	if _, err = tx.Exec(fmt.Sprintf("DELETE FROM %v", tabela)); err != nil {
		fmt.Printf("erro ao limpar tabela: %v", err)
		tx.Rollback()
	}
}

func CountRows(q string, cnx *sql.DB) (int64, error) {
	var count int64
	query := fmt.Sprintf("SELECT count(*) FROM (%v) as subquery", q)
	
	if err := connection.ConexaoSql.QueryRow(query).Scan(&count); err != nil {
		if err == sql.ErrNoRows {
			return 0, fmt.Errorf("nenhuma linha recuperada: %v", sql.ErrNoRows.Error())
		}
		return 0, fmt.Errorf("erro ao contar registros: %v", err)
	}
	return count, nil
}

func NewProgressBar(p *mpb.Progress, total int64, label string) *mpb.Bar {
	return p.AddBar(total, 
		mpb.BarWidth(60),
		mpb.BarStyle("[██████░░░░░░]"),
		mpb.PrependDecorators(
			decor.Name(label+": "),
			decor.CountersNoUnit("%d / %d"),
		),
		mpb.AppendDecorators(
			decor.Percentage(),
			decor.EwmaETA(decor.ET_STYLE_GO, 60),
		),
	)
}

func NewCol(table string, colName string, info string) {
	tx, err := connection.ConexaoFdb.Begin()
	if err != nil {
		fmt.Printf("erro ao iniciar transação: %v", err)
	}

	_, err = tx.Exec(fmt.Sprintf("ALTER TABLE %v ADD %v %v", table, colName, info))
	if err != nil {
		tx.Rollback()
		fmt.Printf("erro ao criar coluna %v: %v", colName, err)
	}

	tx.Commit()
}

func EstourouSubGrupo(codigo int, id_ant string) (string, error) {
	tx, err := connection.ConexaoFdb.Begin()
	if err != nil {
		fmt.Printf("erro ao iniciar transação: %v", err)
	}

	milhar := codigo / 1000
	aux1 := Cache.Subgrupos[id_ant]
	grupoSubgrupo := strings.Split(aux1, ".")
	novoSubgr := fmt.Sprintf("%03d", milhar)
	novoGrupoSubgrupo := grupoSubgrupo[0] + "." + novoSubgr

	if _, err = tx.Query(fmt.Sprintf("select 1 from cadsubgr where id_ant = %s", id_ant)); err != nil {
		if err == sql.ErrNoRows {
			tx.Exec(fmt.Sprintf("INSERT INTO cadsubgr (grupo, SUBGRUPO, nome, ocultar, id_ant) SELECT GRUPO, %v, nome, ocultar, id_ant FROM CADGRUPO WHERE GRUPO = %v", novoSubgr, grupoSubgrupo[0]))
			tx.Commit()
		} else {
			tx.Rollback()
			return "", fmt.Errorf("erro ao buscar subgrupos: %v", err)
		}
	}
	return novoGrupoSubgrupo, err
}

func CriaGrupoSubgrupo(id_ant string) string {
	tx1, err := connection.ConexaoFdb.Begin()
	if err != nil {
		fmt.Printf("erro ao iniciar transação: %v", err)
	}
	defer tx1.Commit()

	tx2, err := connection.ConexaoFdb.Begin()
	if err != nil {
		fmt.Printf("erro ao iniciar transação: %v", err)
	}
	defer tx1.Commit()

	grupo := id_ant[:3]

	_, err = tx1.Exec(`INSERT INTO CADGRUPO(grupo, nome, ocultar, id_ant) VALUES(?,?,?,?)`, grupo, "CONVERSÃO", "N", id_ant)
	if err != nil {
		fmt.Printf("erro ao executar bloco: %v", err)
	}

	_, err = tx2.Exec("INSERT INTO cadsubgr (grupo, SUBGRUPO, nome, ocultar, id_ant) SELECT GRUPO, '000', nome, ocultar, id_ant FROM CADGRUPO WHERE grupo = ?", grupo)
	if err != nil {
		fmt.Printf("erro ao executar bloco: %v", err)
	}
	
	novoGrupoSubgrupo := fmt.Sprintf("%v.000", grupo)
	Cache.Subgrupos[id_ant] = novoGrupoSubgrupo
	return novoGrupoSubgrupo
}