package main

import (
	"MGFSiga/connection"
	"MGFSiga/modules/compras"
	"sync"

	"github.com/vbauerster/mpb"
)

func main() {
	progress := mpb.New()

	var wg sync.WaitGroup

	compras := []func(*mpb.Progress, *sync.WaitGroup){
        func(progress *mpb.Progress, wg *sync.WaitGroup) {
            wg.Add(1)
            compras.Cadunimedida(connection.ConexaoSql, connection.ConexaoFdb, progress, wg)
        },
        func(progress *mpb.Progress, wg *sync.WaitGroup) {
            wg.Add(1)
            compras.GrupoSubgrupo(connection.ConexaoSql, connection.ConexaoFdb, progress, wg)
        },
    }

	for _, f := range compras {
        go f(progress, &wg)
    }

    // Aguarda todas as goroutines terminarem
    wg.Wait()
    progress.Wait()
}