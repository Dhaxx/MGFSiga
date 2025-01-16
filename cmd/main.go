package main

import (
	"MGFSiga/connection"
	"MGFSiga/modules"
	"MGFSiga/modules/compras"

	"github.com/vbauerster/mpb"
)

func main() {
	p := mpb.New()

	compras.Cadunimedida(connection.ConexaoSql, connection.ConexaoFdb, p)
	modules.ArmazenaGruposSubgrupos()
	compras.GrupoSubgrupo(connection.ConexaoSql, connection.ConexaoFdb, p)
	compras.Cadest(connection.ConexaoSql, connection.ConexaoFdb, p)
}