package main

import (
<<<<<<< HEAD
=======
	"MGFSiga/connection"
>>>>>>> 077bf8f21eabe7ac32d1c3c8e0de47dc1b9124b8
	"MGFSiga/modules"
	"MGFSiga/modules/compras"

	"github.com/vbauerster/mpb"
)

func main() {
	p := mpb.New()

<<<<<<< HEAD
	// compras.Cadunimedida(p)
	// compras.GrupoSubgrupo(p)
	modules.ArmazenaGruposSubgrupos()
	modules.ArmazenaUnidadesMedidas()
	// compras.Cadest(p)
	// compras.Destino(p)
	// compras.CentroCusto(p)
	// compras.Cadorc(p)
	compras.Icadorc(p)
=======
	compras.Cadunimedida(connection.ConexaoSql, connection.ConexaoFdb, p)
	modules.ArmazenaGruposSubgrupos()
	compras.GrupoSubgrupo(connection.ConexaoSql, connection.ConexaoFdb, p)
	compras.Cadest(connection.ConexaoSql, connection.ConexaoFdb, p)
>>>>>>> 077bf8f21eabe7ac32d1c3c8e0de47dc1b9124b8
}