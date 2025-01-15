package main

import (
	"MGFSiga/connection"
	"MGFSiga/modules/compras"
	"sync"

	"github.com/vbauerster/mpb"
)

func main() {
	var wg1 sync.WaitGroup
	var wg2 sync.WaitGroup
	p := mpb.New()

	wg1.Add(2)
	go func() {
		compras.Cadunimedida(connection.ConexaoSql, connection.ConexaoFdb, &wg1, p)
	}()
	go func() {
		compras.GrupoSubgrupo(connection.ConexaoSql, connection.ConexaoFdb, &wg1, p)
	}()
	wg1.Wait()

	wg2.Add(1)
	go func() {
		compras.Cadest(connection.ConexaoSql, connection.ConexaoFdb, &wg2, p)
	}()
	wg2.Wait()
}