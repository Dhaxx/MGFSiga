package patrimonio

import (
	"MGFSiga/connection"
	"MGFSiga/modules"
	"database/sql"
	"fmt"
	"time"

	"github.com/vbauerster/mpb"
)

func Aquisicoes(p *mpb.Progress) {
	cnxFdb, err := connection.ConexaoDestino()
	if err != nil {
		panic(err)
	}
	defer cnxFdb.Close()

	barAquisicao := modules.NewProgressBar(p, 1, "MOVBEM - AQUISIÇÕES")

	cnxFdb.Exec("alter SEQUENCE gen_pt_movbem_id RESTART WITH (select max(codigo_mov) from pt_movbem);")
	_, err = cnxFdb.Exec(`EXECUTE BLOCK AS
						BEGIN
							DELETE FROM PT_MOVBEM WHERE TIPO_MOV = 'A';
							INSERT
								INTO
								pt_movbem (empresa_mov,
								codigo_mov,
								codigo_pat_mov,
								data_mov,
								tipo_mov,
								codigo_cpl_mov,
								codigo_set_mov,
								valor_mov,
								documento_mov,
								historico_mov,
								HASH_SINC)
							SELECT
								EMPRESA_PAT,
								gen_id(gen_pt_movbem_id,1) as seq,
								CODIGO_PAT,
								DATAE_PAT,
								'A' tipo_mov,
								CODIGO_CPL_PAT,
								CODIGO_SET_PAT,
								VALAQU_PAT,
								NOTA_PAT,
								'AQUISIÇÃO' HISTORICO_MOV,
								CODIGO_PAT 
							FROM PT_CADPAT a 
							WHERE  NOT EXISTS (SELECT 1 FROM pt_movbem b WHERE a.CODIGO_PAT = b.codigo_pat_mov AND b.tipo_mov = 'A');
							
							--UPDATE PT_MOVBEM SET HASH_SINC = HASH_SINC*1000;
							--UPDATE PT_MOVBEM SET HASH_SINC = CODIGO_MOV;
						END`)
	if err != nil {
		panic(err)
	}
	barAquisicao.Completed()
}

func Movimentacoes(p *mpb.Progress) {
	modules.LimpaTabela("pt_movbem where tipo_mov <> 'A'")
	modules.NewCol("pt_movbem", "codigo_set_mov_ant", "integer")

	cnxFdb, err := connection.ConexaoDestino()
	if err != nil {
		panic(err)
	}
	defer cnxFdb.Close()

	cnxSqls, err := connection.ConexaoOrigem()
	if err != nil {
		panic(err)
	}

	tx, err := cnxFdb.Begin()
	if err != nil {
		panic(err)
	}

	insert, err := tx.Prepare(`insert into pt_movbem (codigo_mov, empresa_mov, codigo_pat_mov, data_mov, tipo_mov, codigo_set_mov, historico_mov, hash_sinc, lote_mov, valor_mov, depreciacao_mov, codigo_set_mov_ant) values (?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		panic(err)
	}

	query := `select		
		im.IdMovimento loteMov,
		im.idBem,
		im.DataMovimento,
		case
			when m.TipoMovimento = 7 then 'B'
			when m.TipoMovimento = 9 then 'T'
			else 'R'
		end tipo_mov,
		m.TipoMovimento,
		Descricao historicoMov,
		im.InformacaoAnterior,
		im.InformacaoAtual
	from
		MGFPatri.dbo.ItensMovimento im
	join MGFPatri.dbo.Movimento m 
	on im.IdMovimento = m.IdMovimento 
	and m.TipoMovimento in (2, 3, 4, 7, 9)
	join MGFPatri.dbo.Bens b ON 
	b.idBem = im.IdBem`

	rows, err := cnxSqls.Query(query)
	if err != nil {
		panic(err)
	}

	totalLinhas, err := modules.CountRows(query)
	if err != nil {
		panic(err)
	}

	barMovimentacao := modules.NewProgressBar(p, totalLinhas, "MOVBEM - MOVIMENTAÇÕES")

	codigoMov := 0
	err = tx.QueryRow("select max(codigo_mov) from pt_movbem").Scan(&codigoMov)
	if err != nil {
		panic(err)
	}

	for rows.Next() {
		var (
			codigoPatMov, loteMov, codTipoMov, codigoSetMov, codigoSetMovAnt int
			dataMov, tipoMov, historicoMov, informacaoAnterior, informacaoAtual string
			valorMov float64
		)
		depreciacaoMov := "N"
		codigoMov++

		err = rows.Scan(&loteMov, &codigoPatMov, &dataMov, &tipoMov, &codTipoMov, &historicoMov, &informacaoAnterior, &informacaoAtual)
		if err != nil {
			panic(err)
		}

		historicoMovConvertido1252, err := modules.DecodeToWin1252(historicoMov)
		if err != nil {
			panic(err)
		}

		dataMovParseada, err := time.Parse(time.RFC3339, dataMov)
		if err != nil {
			fmt.Printf("erro ao parsear string para data: %v", err)
		}
		dataMovFormatada := dataMovParseada.Format("02.01.2006")

		switch codTipoMov {
		case 2,4: // Reavaliação
			if _, err := tx.Exec("update pt_cadpat set dtlan_pat = ? where codigo_pat = ?", dataMovFormatada, codigoPatMov); err != nil {
				tx.Rollback()
				panic(err)
			}

			if codTipoMov == 2 {
				valorMov, err = modules.ConvertStringToFloat(informacaoAtual) //Valor da reavaliação
				if err != nil {
					tx.Rollback()
					panic(err)
				}
			} else {
				informacaoAnteriorFloat, err := modules.ConvertStringToFloat(informacaoAnterior)
				if err != nil {
					tx.Rollback()
					panic(err)
				}
				informacaoAtualFloat, err := modules.ConvertStringToFloat(informacaoAtual)
				if err != nil {
					tx.Rollback()
					panic(err)
				}
				valorMov = informacaoAtualFloat - informacaoAnteriorFloat //Calcula o valor da diferença e usa como valor da movimentação
			}
		case 3: // Depreciação
			depreciacaoMov = "S"
			informacaoAtualFloat, err := modules.ConvertStringToFloat(informacaoAtual)
			if err != nil {
				tx.Rollback()
				panic(err)
			}
			valorMov = informacaoAtualFloat*-1 
		case 7:
			if _, err := tx.Exec("update pt_cadpat set dtpag_pat = ? where codigo_pat = ?", dataMovFormatada, codigoPatMov); err != nil {
				tx.Rollback()
				panic(err)
			}
			informacaoAnteriorFloat, err := modules.ConvertStringToFloat(informacaoAnterior)
			if err != nil {
				tx.Rollback()
				panic(err)
			}
			informacaoAtualFloat, err := modules.ConvertStringToFloat(informacaoAtual)
			if err != nil {
				tx.Rollback()
				panic(err)
			}
			valorMov = informacaoAtualFloat - informacaoAnteriorFloat
		case 9:
			if err := tx.QueryRow(fmt.Sprintf("select codigo_set from pt_cadpats where noset_set = '%v'", informacaoAtual)).Scan(&codigoSetMov); err != nil {
				if err == sql.ErrNoRows {
					codigoSetMov = 55
				} else {
					tx.Rollback()
					panic(err)
				}
			}
			if err := tx.QueryRow(fmt.Sprintf("select codigo_set from pt_cadpats where noset_set = '%v'", informacaoAnterior)).Scan(&codigoSetMovAnt); err != nil {
				if err == sql.ErrNoRows {
					codigoSetMovAnt = 55
				} else {
					tx.Rollback()
					panic(err)
				}
			}
		}

		_, err = insert.Exec(codigoMov, modules.Cache.Empresa, codigoPatMov, dataMovFormatada, tipoMov, codigoSetMov, historicoMovConvertido1252, codigoMov, loteMov, valorMov, depreciacaoMov, codigoSetMovAnt)
		if err != nil {
			tx.Rollback()
			fmt.Printf("Erro ao inserir valores: %v", err)
		}
		barMovimentacao.Increment()
	}
	tx.Commit()
}