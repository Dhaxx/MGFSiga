package modules

import (
	"MGFSiga/connection"
	"bytes"
	"database/sql"
	"fmt"
	"strings"

	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
)

var Cache struct {
	Subgrupos map[string]string
	Medidas map[string]string
	Empresa int
	IdCadorc map[string]int
	Itens map[string]string
	NomeForn map[string]string
	Codif map[string]int
	NumlicAtravesDaNumorc map[string]int
}

func init() {
	cnxFdb, err := connection.ConexaoDestino()
	if err != nil {
		panic("Falha ao conectar com o banco de destino: " + err.Error())
	}
	defer cnxFdb.Close()

	cnxFdb.QueryRow("Select empresa from cadcli").Scan(&Cache.Empresa)
}

func ArmazenaGruposSubgrupos() {
	Cache.Subgrupos = make(map[string]string)
	cnxFdb, err := connection.ConexaoDestino()
	if err != nil {
		fmt.Printf("Falha ao conectar com o banco de destino: %v", err)
	}
	defer cnxFdb.Close()

	rowsSubgrupos, err := cnxFdb.Query("select id_ant, grupo||'.'||subgrupo from cadsubgr")
	if err != nil {
		if err == sql.ErrNoRows {
			fmt.Printf("Cadsubgr não possuem registros ainda: %v", err)
		}
		fmt.Printf("Erro ao obter subgrupos: %v", err)
	}

	for rowsSubgrupos.Next() {
		var id_ant, grupoSubgrupo string
		if err := rowsSubgrupos.Scan(&id_ant, &grupoSubgrupo); err != nil {
			fmt.Printf("erro ao scanear valores: %v", err)
			continue
		}
		Cache.Subgrupos[id_ant] = grupoSubgrupo
	}
}

func ArmazenaUnidadesMedidas() {
	Cache.Medidas = make(map[string]string)
	cnxFdb, err := connection.ConexaoDestino()
	if err != nil {
		fmt.Printf("Falha ao conectar com o banco de destino: %v", err)
	}
	defer cnxFdb.Close()

	rows, err := cnxFdb.Query("select descricao, sigla from cadunimedida")
	if err != nil {
		fmt.Printf("erro ao buscar unidades de medida: %v", err)
	}

	for rows.Next() {
		var descricao, sigla string
		if err := rows.Scan(&descricao, &sigla); err != nil {
			fmt.Printf("erro ao scanear valores: %v", err)
		}
		Cache.Medidas[descricao] = sigla
	}
}

func ArmazenaIdCadorc() {
	Cache.IdCadorc = make(map[string]int)
	cnxFdb, err := connection.ConexaoDestino()
	if err != nil {
		fmt.Printf("Falha ao conectar com o banco de destino: %v", err)
	}
	defer cnxFdb.Close()

	rows, err := cnxFdb.Query("select numorc, id_cadorc from cadorc")
	if err != nil {
		fmt.Printf("erro ao buscar unidades de medida: %v", err)
	}

	for rows.Next() {
		var numorc string
		var idCadorc int

		if err := rows.Scan(&numorc, &idCadorc); err != nil {
			fmt.Printf("erro ao scanear valores: %v", err)
		}

		Cache.IdCadorc[numorc] = idCadorc
	}
}

func ArmazenaItens() {
	Cache.Itens = make(map[string]string)
	cnxFdb, err := connection.ConexaoDestino()
	if err != nil {
		fmt.Printf("Falha ao conectar com o banco de destino: %v", err)
	}
	defer cnxFdb.Close()

	rows, err := cnxFdb.Query("select cadpro, codreduz from cadest")
	if err != nil {
		fmt.Printf("erro ao buscar unidades de medida: %v", err)
	}

	for rows.Next() {
		var cadpro, codreduz string

		if err := rows.Scan(&cadpro, &codreduz); err != nil {
			fmt.Printf("erro ao scanear valores: %v", err)
		}

		Cache.Itens[codreduz] = cadpro
	}
}

func ArmazenaFornecedor() {
	Cache.NomeForn = make(map[string]string)
	Cache.Codif = make(map[string]int)
	defaultCodif := new(int)
	cnxFdb, err := connection.ConexaoDestino()
	if err != nil {
		fmt.Printf("Falha ao conectar com o banco de destino: %v", err)
	}
	defer cnxFdb.Close()
	
	cnxFdb.QueryRow("SELECT codif FROM desfor WHERE insmf = (SELECT replace(replace(cgc,('/'),''),'-','') from cadcli)").Scan(defaultCodif)
	Cache.Codif["0"] = *defaultCodif

	rows, err := cnxFdb.Query("select nome, codif, trim(insmf) from desfor")
	if err != nil {
		fmt.Printf("erro ao obter informações: %v", err)
	}

	for rows.Next() {
		var (
			nome string
			codif int
			insmf string
		)

		if err := rows.Scan(&nome, &codif, &insmf); err != nil {
			fmt.Printf("erro ao scanear valores: %v", err)
		}

		Cache.NomeForn[insmf] = nome
		Cache.Codif[insmf] = codif 
	}
}

func ArmazenaNumlicAtravesDaNumorc() {
	Cache.NumlicAtravesDaNumorc = make(map[string]int)
	cnxFdb, err := connection.ConexaoDestino()
	if err != nil {
		fmt.Printf("Falha ao conectar com o banco de destino: %v", err)
	}
	defer cnxFdb.Close()

	rows, err := cnxFdb.Query("select numorc, numlic from cadorc where numlic is not null")
	if err != nil {
		fmt.Printf("erro ao buscar unidades de medida: %v", err)
	}

	for rows.Next() {
		var numorc string
		var numlic int

		if err := rows.Scan(&numorc, &numlic); err != nil {
			fmt.Printf("erro ao scanear valores: %v", err)
		}

		Cache.NumlicAtravesDaNumorc[numorc] = numlic
	}
}

func LimpaTabela(tabela string) {
	cnxFdb, err := connection.ConexaoDestino()
	if err != nil {
		fmt.Printf("Falha ao conectar com o banco de destino: %v", err)
	}
	defer cnxFdb.Close()

	tx, err := cnxFdb.Begin()
	if err != nil {
		fmt.Printf("erro ao iniciar transação: %v", err)
	}

	if _, err = tx.Exec(fmt.Sprintf("DELETE FROM %v", tabela)); err != nil {
		fmt.Printf("erro ao limpar tabela: %v", err)
		tx.Rollback()
	}
	tx.Commit()
}

func CountRows(q string) (int64, error) {
	cnxSqls, err := connection.ConexaoOrigem()
	if err != nil {
		fmt.Printf("Falha ao conectar com o banco de destino: %v", err)
	}
	defer cnxSqls.Close()

	var count int64
	query := fmt.Sprintf("SELECT count(*) FROM (%v) as subquery", q)
	
	if err := cnxSqls.QueryRow(query).Scan(&count); err != nil {
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
	cnxFdb, err := connection.ConexaoDestino()
	if err != nil {
		fmt.Printf("Falha ao conectar com o banco de destino: %v", err)
	}
	defer cnxFdb.Close()

	tx, err := cnxFdb.Begin()
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

func EstourouSubGrupo(codigo int, subgrupo string, id_ant string) (string, error) {
	cnxFdb, err := connection.ConexaoDestino()
	if err != nil {
		fmt.Printf("Falha ao conectar com o banco de destino: %v", err)
	}
	defer cnxFdb.Close()

	tx, err := cnxFdb.Begin()
	if err != nil {
		fmt.Printf("erro ao iniciar transação: %v", err)
	}
	defer tx.Commit()

	milhar := codigo / 1000
	grupoSubgrupo := strings.Split(subgrupo, ".")
	novoSubgr := fmt.Sprintf("%03d", milhar)
	novoGrupoSubgrupo := grupoSubgrupo[0] + "." + novoSubgr

	if _, err = tx.Query(fmt.Sprintf("select 1 from cadsubgr where id_ant = %v and subgrupo = %v", id_ant, novoSubgr)); err != nil {
		if err == sql.ErrNoRows {
			tx.Exec(fmt.Sprintf("INSERT INTO cadsubgr (grupo, SUBGRUPO, nome, ocultar, id_ant) SELECT GRUPO, %v, nome, ocultar, id_ant FROM CADGRUPO WHERE GRUPO = %v", novoSubgr, grupoSubgrupo[0]))
		} else {
			tx.Rollback()
			return "", fmt.Errorf("erro ao buscar subgrupos: %v", err)
		}
	}
	return novoGrupoSubgrupo, err
}

func CriaGrupoSubgrupo(id_ant string) string {
	cnxFdb, err := connection.ConexaoDestino()
	if err != nil {
		fmt.Printf("Falha ao conectar com o banco de destino: %v", err)
	}
	defer cnxFdb.Close()

	tx1, err := cnxFdb.Begin()
	if err != nil {
		fmt.Printf("erro ao iniciar transação: %v", err)
	}

	tx2, err := cnxFdb.Begin()
	if err != nil {
		fmt.Printf("erro ao iniciar transação: %v", err)
	}

	grupo := id_ant[:3]

	_, err = tx1.Exec(`INSERT INTO CADGRUPO(grupo, nome, ocultar, id_ant) VALUES(?,?,?,?)`, grupo, "CONVERSÃO", "N", id_ant)
	if err != nil {
		tx1.Rollback()
		fmt.Printf("erro ao tentar inserir grupo: %v", err)
	}
	tx1.Commit()

	_, err = tx2.Exec("INSERT INTO cadsubgr (grupo, SUBGRUPO, nome, ocultar, id_ant) SELECT GRUPO, '000', nome, ocultar, id_ant FROM CADGRUPO WHERE grupo = ?", grupo)
	if err != nil {
		tx2.Rollback()
		fmt.Printf("erro ao tentar inserir subgrupo: %v", err)
	}
	tx2.Commit()
	
	novoGrupoSubgrupo := fmt.Sprintf("%v.000", grupo)
	Cache.Subgrupos[id_ant] = novoGrupoSubgrupo
	return novoGrupoSubgrupo
}

func DecodeToWin1252(input string) (string, error) {
	// Define uma tabela de caracteres válidos no Windows-1252
	validChars := charmap.Windows1252

	// Remove ou substitui caracteres inválidos
	t := transform.Chain(
		runes.Remove(runes.Predicate(func(r rune) bool {
			// Remove caracteres que não são válidos no Windows-1252
			_, ok := validChars.EncodeRune(r)
			return !ok
		})),
		validChars.NewEncoder(),
	)

	// Transforma a string
	var buf bytes.Buffer
	writer := transform.NewWriter(&buf, t)

	_, err := writer.Write([]byte(input))
	if err != nil {
		return "", fmt.Errorf("erro ao codificar para Windows-1252: %w", err)
	}

	if err := writer.Close(); err != nil {
		return "", fmt.Errorf("erro ao finalizar o writer: %w", err)
	}

	return buf.String(), nil
}

func DesativaAtivaTriggers(state string) {
	cnx_fdb, err := connection.ConexaoDestino()
	if err != nil {
		panic("Falha ao conectar com o banco de destino: " + err.Error())
	}
	defer cnx_fdb.Close()

	query := fmt.Sprintf(`execute block
        as
            declare variable alter_trigger varchar(1024);
        begin
            for select 'alter trigger ' || trim(rdb$trigger_name) || ' %v;' 
            from RDB$TRIGGERS
            where (rdb$trigger_sequence = 200 OR (trim(rdb$trigger_name) STARTING WITH 'TBI_') OR (trim(rdb$trigger_name) STARTING WITH 'TBU_') OR (trim(rdb$trigger_name) STARTING WITH 'TBD_') OR (trim(rdb$trigger_name) STARTING WITH 'TD_'))
            AND rdb$relation_name IN (
                'CADUNIMEDIDA',
                'CADGRUPO',
                'CADSUBGR',
                'CADEST',
                'DESTINO',
                'CENTROCUSTO',
                'CADORC',
                'ICADORC',
                'FCADORC',
                'VCADORC',
                'CADLIC',
                'CADPROLIC',
                'CADPROLIC_DETALHE',
                'CADPRO_STATUS',
                'CADLIC_SESSAO',
                'PROLIC',
                'PROLICS',
                'CADPRO_PROPOSTA',
                'CADPRO_LANCE',
                'CADPRO_FINAL',
                'CADPRO',
                'CADPROLIC_DETALHE_FIC',
                'REGPRECODOC',
                'REGPRECO',
                'REGPRECOHIS',
                'CADPED',
                'ICADPED',
                'REQUI',
                'ICADREQ',
                'PT_CADTIP',
                'PT_CADPATD',
                'PT_CADPATS',
                'PT_CADPATG',
                'PT_CADPAT',
                'PT_MOVBEM'
            )
            into :alter_trigger
            do
                execute statement :alter_trigger;
        end`, state)

    _, err = cnx_fdb.Exec(query)
    if err != nil {
        panic("Falha ao executar execute block: " + err.Error())
    }
}

func LimpaCompras() {
	cnx_aux, err := connection.ConexaoDestino()
	if err != nil {
		panic("Falha ao conectar com o banco de destino: " + err.Error())
	}
	defer cnx_aux.Close()

	_, err = cnx_aux.Exec(`execute block as
		begin
		DELETE FROM ICADREQ;
		DELETE FROM REQUI;
		DELETE FROM ICADPED;
		DELETE FROM CADPED;
		DELETE FROM regpreco;
		DELETE FROM regprecohis;
		DELETE FROM regprecodoc;
		DELETE FROM CADPROLIC_DETALHE_FIC;
		DELETE FROM CADPRO;
		DELETE FROM CADPRO_FINAL;
		DELETE FROM CADPRO_LANCE;
		DELETE FROM CADPRO_PROPOSTA;
		DELETE FROM PROLICS;
		DELETE FROM PROLIC;
		DELETE FROM CADPRO_STATUS;
		DELETE FROM CADLIC_SESSAO;
		DELETE FROM CADPROLIC_DETALHE;
		DELETE FROM CADPROLIC;
		DELETE FROM CADLIC;
		DELETE FROM VCADORC;
		DELETE FROM FCADORC;
		DELETE FROM ICADORC;
		DELETE FROM CADORC;
		DELETE FROM CADEST;
		DELETE FROM CENTROCUSTO;
		DELETE FROM DESTINO;
		end;`)
	if err != nil {
		panic("Falha ao executar delete: " + err.Error())
	}
}