package main

import (
	"MGFSiga/modules"
	"MGFSiga/modules/compras"

	"github.com/vbauerster/mpb"
)

func main() {
	p := mpb.New()

	// compras.Cadunimedida(p)
	// compras.GrupoSubgrupo(p)
	modules.ArmazenaGruposSubgrupos()
	modules.ArmazenaUnidadesMedidas()
	// compras.Cadest(p)
	// compras.Destino(p)
	// compras.CentroCusto(p)
	// compras.Cadorc(p)
	compras.Icadorc(p)
}