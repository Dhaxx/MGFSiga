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

var ConexaoFdb *sql.DB
var ConexaoSql *sql.DB

func init() {
	envPath, err := os.Getwd()
	if err != nil {
		log.Fatalf("Erro ao obter diretório: %v", err)
	}

	if err = godotenv.Load(filepath.Join(envPath, ".env")); err != nil {
		log.Fatalf("Erro ao carregar .env: %v", err)
	}

	dsnFdb := fmt.Sprintf("%s:%s@%s:%s/%s?charset=ISO8859_1",
		os.Getenv("FDB_USER"),
		os.Getenv("FDB_PASS"),
		os.Getenv("FDB_HOST"),
		os.Getenv("FDB_PORT"),
		os.Getenv("FDB_PATH"))

	dsnSql := fmt.Sprintf("server=%s;user=%s;password=%s;port=%s;database=%s;charset=ISO8859_1",
		os.Getenv("SQLS_HOST"),
		os.Getenv("SQLS_USER"),
		os.Getenv("SQLS_PASS"),
		os.Getenv("SQLS_PORT"),
		os.Getenv("SQLS_DB"))

	ConexaoFdb, err = sql.Open("firebirdsql", dsnFdb)
	if err != nil {
		log.Fatalf("Erro ao estabelecer conexão FDB: %v", err)
	}

	ConexaoSql, err = sql.Open("sqlserver", dsnSql)
	if err != nil {
		log.Fatalf("Erro ao estabelecer conexão SQLServer: %v", err)
	}
	if err = ConexaoSql.Ping(); err != nil {
		log.Fatalf("Erro ao pingar sql: %v", err)
	}
}
