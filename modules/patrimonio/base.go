package patrimonio

import (
	"MGFSiga/connection"
	"MGFSiga/modules"
	"fmt"

	"github.com/vbauerster/mpb"
)

func TipoMov(p *mpb.Progress) {
	modules.LimpaTabela("pt_tipomov")

	cnxFdb, err := connection.ConexaoDestino()
	if err != nil {
		panic("Falha ao conectar com o banco de destino: " + err.Error())
	}
	defer cnxFdb.Close()

	cnxSqls, err := connection.ConexaoOrigem()
	if err != nil {
		panic("Falha ao conectar com o banco de origem: " + err.Error())
	}
	defer cnxSqls.Close()

	tx, err := cnxFdb.Begin()
	if err != nil {
		fmt.Printf("Erro ao iniciar transação: %v", err)
	}
	defer tx.Commit()

	valores := map[string]string{
		"A": "Aquisição",
		"B": "Baixa",
		"T": "Transferência",
		"R": "Procedimento Contábil",
		"P": "Transferência de Plano Contábil",
	}

	insert, err := tx.Prepare("INSERT INTO PT_TIPOMOV (codigo_tmv, descricao_tmv) VALUES (?, ?)")
	if err != nil {
		fmt.Printf("Erro ao preparar insert: %v", err)
	}

	barTipoMov := modules.NewProgressBar(p, 1, "TIPOMOV")

	for sigla, descricao := range valores {
		_, err := insert.Exec(sigla, descricao)
		if err != nil {
			fmt.Printf("Erro ao inserir valores: %v", err)
		}
	}
	barTipoMov.Completed()
}

func Cadajuste(p *mpb.Progress) {
	modules.LimpaTabela("pt_cadajuste")

	cnxFdb, err := connection.ConexaoDestino()
	if err != nil {
		panic("Falha ao conectar com o banco de destino: " + err.Error())
	}
	defer cnxFdb.Close()

	cnxSqls, err := connection.ConexaoOrigem()
	if err != nil {
		panic("Falha ao conectar com o banco de origem: " + err.Error())
	}
	defer cnxSqls.Close()

	tx, err := cnxFdb.Begin()
	if err != nil {
		fmt.Printf("Erro ao iniciar transação: %v", err)
	}
	defer tx.Commit()

	barCadAjuste := modules.NewProgressBar(p, 1, "CADAJUSTE")
	cnxFdb.Exec("INSERT INTO PT_CADAJUSTE (CODIGO_AJU, EMPRESA_AJU, DESCRICAO_AJU) VALUES (1, ?, 'REAVALIAÇÃO (ANTES DO CORTE)')", modules.Cache.Empresa)
	barCadAjuste.Completed()
}

func Cadbai(p *mpb.Progress) {
	modules.LimpaTabela("pt_cadbai")

	cnxFdb, err := connection.ConexaoDestino()
	if err != nil {
		panic("Falha ao conectar com o banco de destino: " + err.Error())
	}
	defer cnxFdb.Close()

	cnxSqls, err := connection.ConexaoOrigem()
	if err != nil {
		panic("Falha ao conectar com o banco de origem: " + err.Error())
	}
	defer cnxSqls.Close()

	tx, err := cnxFdb.Begin()
	if err != nil {
		fmt.Printf("Erro ao iniciar transação: %v", err)
	}
	defer tx.Commit()

	barCadBai := modules.NewProgressBar(p, 1, "CADBAI")

	query := `select * from MGFPatri.dbo.TipoMovimento tm where descricao like '%baixa%'`
	rows, err := cnxSqls.Query(query)
	if err != nil {
		fmt.Printf("Erro ao executar query: %v", err)
	}

	for rows.Next() {
		var codigo, descricao string
		err := rows.Scan(&codigo, &descricao)
		if err != nil {
			fmt.Printf("Erro ao escanear valores: %v", err)
		}

		cnxFdb.Exec("INSERT INTO PT_CADBAI (CODIGO_BAI, EMPRESA_BAI, DESCRICAO_BAI) VALUES (?, ?, ?)", codigo, modules.Cache.Empresa, descricao)
	}
	barCadBai.Completed()
}

func Cadsit(p *mpb.Progress) {
	modules.LimpaTabela("pt_cadsit")

	cnxFdb, err := connection.ConexaoDestino()
	if err != nil {
		panic("Falha ao conectar com o banco de destino: " + err.Error())
	}
	defer cnxFdb.Close()

	cnxSqls, err := connection.ConexaoOrigem()
	if err != nil {
		panic("Falha ao conectar com o banco de origem: " + err.Error())
	}
	defer cnxSqls.Close()

	tx, err := cnxFdb.Begin()
	if err != nil {
		fmt.Printf("Erro ao iniciar transação: %v", err)
	}
	defer tx.Commit()

	barCadSit := modules.NewProgressBar(p, 1, "CADSIT")

	query := `select * from MGFPatri.dbo.EstadoDeConservacao edc`
	rows, err := cnxSqls.Query(query)
	if err != nil {
		fmt.Printf("Erro ao executar query: %v", err)
	}

	for rows.Next() {
		var codigo, descricao string
		err := rows.Scan(&codigo, &descricao)
		if err != nil {
			fmt.Printf("Erro ao escanear valores: %v", err)
		}

		descricaoConvertido1252, err := modules.DecodeToWin1252(descricao)
		if err != nil {
			fmt.Printf("Erro ao converter descrição para Win1252: %v", err)
		}

		cnxFdb.Exec("INSERT INTO PT_CADSIT (CODIGO_SIT, EMPRESA_SIT, DESCRICAO_SIT) VALUES (?, ?, ?)", codigo, modules.Cache.Empresa, descricaoConvertido1252)
	}
	barCadSit.Completed()
}

func Cadtip(p *mpb.Progress) {
	modules.LimpaTabela("pt_cadtip")

	cnxFdb, err := connection.ConexaoDestino()
	if err != nil {
		fmt.Print("Falha ao conectar com o banco de destino: " + err.Error())
	}
	defer cnxFdb.Close()

	cnxSqls, err := connection.ConexaoOrigem()
	if err != nil {
		fmt.Print("Falha ao conectar com o banco de origem: " + err.Error())
	}
	defer cnxSqls.Close()

	tx, err := cnxFdb.Begin()
	if err != nil {
		fmt.Printf("Erro ao iniciar transação: %v", err)
	}
	defer tx.Commit()

	insert, err := cnxFdb.Prepare("INSERT INTO PT_CADTIP (CODIGO_TIP, EMPRESA_TIP, DESCRICAO_TIP, OCULTAR_TIP) VALUES (?, ?, ?, 'N')")
	if err != nil {
		fmt.Print("Erro ao Prepara Insert: "+err.Error())
	}

	query := `select
		row_number() over (
		order by IdClasse) codigo_tip,
		substring(descricao,0,60) descricao
	from
		MGFPatri.dbo.classes`

	totalLinhas, err := modules.CountRows(query)
	if err != nil {
		fmt.Print("Erro ao contar linhas: "+err.Error())
	}

	barCadTip := modules.NewProgressBar(p, totalLinhas, "CADTIP")

	rows, err := cnxSqls.Query(query)
	if err != nil {
		fmt.Printf("Erro ao executar query: %v", err)
	}

	for rows.Next() {
		var (
			codigo_tip, descricao string
		)

		err := rows.Scan(&codigo_tip, &descricao)
		if err != nil {
			fmt.Printf("Erro ao escanear valores: %v", err)
		}

		descricaoConvertido1252, err := modules.DecodeToWin1252(descricao)
		if err != nil {
			fmt.Printf("Erro ao converter descrição para Win1252: %v", err)
		}

		_, err = insert.Exec(codigo_tip, modules.Cache.Empresa, descricaoConvertido1252)
		if err != nil {
			fmt.Printf("Erro ao inserir valores: %v", err)
		}
		barCadTip.Increment()
	}
}

func Cadpatd(p *mpb.Progress) {
	modules.LimpaTabela("pt_cadtip")

	cnxFdb, err := connection.ConexaoDestino()
	if err != nil {
		fmt.Print("Falha ao conectar com o banco de destino: " + err.Error())
	}
	defer cnxFdb.Close()

	cnxSqls, err := connection.ConexaoOrigem()
	if err != nil {
		fmt.Print("Falha ao conectar com o banco de origem: " + err.Error())
	}
	defer cnxSqls.Close()

	barCadpatd := modules.NewProgressBar(p, 1, "CADPATD")
	cnxFdb.Exec("INSERT INTO PT_CADPATD (codigo_des, empresa_des, nauni_des, ocultar_des) VALUES (?, ?, ?, ?)", 1, modules.Cache.Empresa, "SAAE DE RAUL SOARES", "N")
	barCadpatd.Completed()
}

func Cadpats(p *mpb.Progress) {
	modules.LimpaTabela("pt_cadpats")

	cnxFdb, err := connection.ConexaoDestino()
	if err != nil {
		fmt.Print("Falha ao conectar com o banco de destino: " + err.Error())
	}
	defer cnxFdb.Close()

	cnxSqls, err := connection.ConexaoOrigem()
	if err != nil {
		fmt.Print("Falha ao conectar com o banco de origem: " + err.Error())
	}
	defer cnxSqls.Close()

	insert, err := cnxFdb.Prepare("insert into pt_cadpats (codigo_set, empresa_set, codigo_des_set, noset_set, ocultar_set) values (?,?,?,?,?)")
	if err != nil {
		fmt.Print("Erro ao preparar insert: "+err.Error())
	}

	query := `select
		l.IdLocal codigo_set,
		1 codigo_des_set, 
		Descricao noset_set, 
		'N' ocultar_set
	from
		MGFPatri.dbo.Locais l`

	totalLinhas, err := modules.CountRows(query)
	if err != nil {
		fmt.Print("Erro ao contar linhas: "+err.Error())
	}

	barCadPats := modules.NewProgressBar(p, totalLinhas, "CADPATS")

	rows, err := cnxSqls.Query(query)
	if err != nil {
		fmt.Printf("Erro ao executar query: %v", err)
	}

	for rows.Next() {
		var(
			codigo_set, codigo_des_set int
			noset_set, ocultar_set string
		)

		err := rows.Scan(&codigo_set, &codigo_des_set, &noset_set, &ocultar_set)
		if err != nil {
			fmt.Printf("Erro ao escanear valores: %v", err)
		}

		descricaoConvertido1252, err := modules.DecodeToWin1252(noset_set)
		if err != nil {
			fmt.Printf("Erro ao converter descrição para Win1252: %v", err)
		}

		_, err = insert.Exec(codigo_set, modules.Cache.Empresa, codigo_des_set, descricaoConvertido1252, ocultar_set)
		if err != nil {
			fmt.Printf("Erro ao inserir valores: %v", err)
		}
		barCadPats.Increment()
	}
}

func Cadpatg(p *mpb.Progress) {
	modules.LimpaTabela("pt_cadpatg")

	cnxFdb, err := connection.ConexaoDestino()
	if err != nil {
		fmt.Print("Falha ao conectar com o banco de destino: " + err.Error())
	}
	defer cnxFdb.Close()

	grupos := []string{
		"MÓVEIS",
		"IMÓVEIS",
		"INTANGÍVEIS",
	}

	totalGrupos := len(grupos)

	barCadpatg := modules.NewProgressBar(p, int64(totalGrupos), "CADPATG")

	for i, grupo := range grupos {
		i ++

		grupoConvertido1252, err := modules.DecodeToWin1252(grupo)
		if err != nil {
			fmt.Printf("Erro ao converter descrição para Win1252: %v", err)
		}
		
		cnxFdb.Exec("INSERT INTO PT_CADPATG (CODIGO_GRU, EMPRESA_GRU, NOGRU_GRU, ocultar_gru) VALUES (?, ?, ?, 'N')", i, modules.Cache.Empresa, grupoConvertido1252)
		barCadpatg.Increment()
	}
}