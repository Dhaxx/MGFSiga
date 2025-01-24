package main

import (
	"MGFSiga/modules"
	"MGFSiga/modules/compras"
	// "MGFSiga/modules/patrimonio"

	"github.com/vbauerster/mpb"
)

func main() {
	p := mpb.New()
	modules.DesativaAtivaTriggers("INACTIVE")
	// modules.LimpaCompras()
	// compras.Cadunimedida(p)
	// compras.GrupoSubgrupo(p)
	modules.ArmazenaGruposSubgrupos()
	modules.ArmazenaUnidadesMedidas()
	modules.ArmazenaFornecedor()
	// compras.Cadest(p)
	// compras.CentroCusto(p)
	// compras.Cadorc(p)
	modules.ArmazenaIdCadorc()
	modules.ArmazenaItens()
	modules.ArmazenaNumlicAtravesDaNumorc()
	// compras.Icadorc(p)
	// compras.Fcadorc(p)
	// compras.Vcadorc(p)
	// compras.Cadlic(p)
	// compras.Cadprolic(p)
	// compras.CadprolicDetalhe(p)
	// compras.ProlicProlics(p)
	// compras.CadlicSessao(p)
	// compras.CadproProposta(p)
	// compras.CadproLance(p)
	// compras.CadproFinal(p)
	// compras.Cadpro(p)
	// compras.Regpreco(p)
	// compras.Cadped(p)
	// compras.Icadped(p)
	compras.Requi(p)

	// modules.LimpaPatrimonio()
	// patrimonio.TipoMov(p)
	// patrimonio.Cadajuste(p)
	// patrimonio.Cadbai(p)
	// patrimonio.Cadsit(p)
	// patrimonio.Cadtip(p)
	// patrimonio.Cadpatd(p)
	// patrimonio.Cadpats(p)
	// patrimonio.Cadpatg(p)
	// patrimonio.Cadresponsavel(p)
	// modules.ArmazenaSituacoes()
	// patrimonio.Cadpat(p)
	// patrimonio.Aquisicoes(p)
	// patrimonio.Movimentacoes(p)
	// modules.AtualizaPatrimonio()

	modules.DesativaAtivaTriggers("ACTIVE")
}