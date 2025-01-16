package connection

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/joho/godotenv"
	_ "github.com/mattn/go-adodb"
	_ "github.com/nakagami/firebirdsql"
)

var dsnFdb string
var dsnSql string

func init() {
	envPath, err := os.Getwd()
	if err != nil {
		log.Fatalf("Erro ao obter diretório: %v", err)
	}

	if err = godotenv.Load(filepath.Join(envPath, ".env")); err != nil {
		log.Fatalf("Erro ao carregar .env: %v", err)
	}

<<<<<<< HEAD
	dsnFdb = fmt.Sprintf("%s:%s@%s:%s/%s?charset=win1252",
=======
	dsnFdb := fmt.Sprintf("%s:%s@%s:%s/%s?charset=win1252",
>>>>>>> 077bf8f21eabe7ac32d1c3c8e0de47dc1b9124b8
		os.Getenv("FDB_USER"),
		os.Getenv("FDB_PASS"),
		os.Getenv("FDB_HOST"),
		os.Getenv("FDB_PORT"),
		os.Getenv("FDB_PATH"))

<<<<<<< HEAD
	dsnSql = fmt.Sprintf("server=%s;user=%s;password=%s;port=%s;database=%s;charset=windows-1252",
=======
	dsnSql := fmt.Sprintf("server=%s;user=%s;password=%s;port=%s;database=%s;charset=windows-1252",
>>>>>>> 077bf8f21eabe7ac32d1c3c8e0de47dc1b9124b8
		os.Getenv("SQLS_HOST"),
		os.Getenv("SQLS_USER"),
		os.Getenv("SQLS_PASS"),
		os.Getenv("SQLS_PORT"),
		os.Getenv("SQLS_DB"))
}

func ConexaoDestino() (*sql.DB, error) {
	ConexaoFdb, err := sql.Open("firebirdsql", dsnFdb)
	if err != nil {
		log.Fatalf("Erro ao estabelecer conexão FDB: %v", err)
	}

	return ConexaoFdb, nil
}

func ConexaoOrigem() (*sql.DB, error) {
	ConexaoSql, err := sql.Open("sqlserver", dsnSql)
	if err != nil {
		log.Fatalf("Erro ao estabelecer conexão SQLServer: %v", err)
	}

	return ConexaoSql, nil
}