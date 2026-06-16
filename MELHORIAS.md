# Catálogo de Melhorias — CADOC 3040 (Glue/PySpark)

> Consolidado em 2026-06-12. Fonte única de melhorias/pendências do projeto.
> Status: ✅ feito | 🔄 pendente (a fazer) | ❓ aguarda decisão negócio/analista | 💡 sugestão (não aprovada ainda)

| # | Categoria | Melhoria | Prioridade | Status | Origem |
|---|-----------|----------|------------|--------|--------|
| 1 | Conformidade Bacen | **S81**: CNPJ do header = prefixo CNPJ8 do IPOC no PPBANK (constante única `PPBANK_CNPJ8`) | ALTA | ✅ feito 2026-06-12 | Validador Bacen 13657 |
| 2 | Conformidade Bacen | **B01**: ordem oficial dos filhos de `Op` = `Venc, Gar, Inf, 4966` (script + XSD local + smoke test) | ALTA | ✅ feito 2026-06-12 | Validador Bacen 13657 / SCR3040_Leiaute.xls |
| 3 | Conformidade Bacen | **C83**: regra para Op com `EstInstFin` sem `<Estagio Motivo= DtAlocacao=>` e sem Inf 03xx — preencher, suprimir EstInstFin ou corrigir upstream (erro vem do DADO de entrada) | ALTA | ❓ negócio | Validador 2026-06-12 |
| 4 | Conformidade Bacen | **I13**: filtro de cliente com soma de vencimentos < R$200 sem Inf saída 03xx (não existe no job); verificar se o split por Op (`FiltroTabela` por conta) divide cliente entre os 2 arquivos e derruba a soma por arquivo | ALTA | ❓ negócio | Validador 2026-06-12 |
| 5 | Conformidade Bacen | Leiaute permite **N** `<Estagio>` e **N** `<Perda>` por `ContInstFinRes4966`; job lê máx. 1 Estagio (struct→virar array) e ignora Perda (perda silenciosa de dado) | MÉDIA | 🔄 pendente | Análise leiaute 2026-06-12 |
| 6 | Regra de negócio | `cpf_deletar_contrato` / `is_tp_0316` calculados mas nenhum filtro aplica — confirmar se DataStage excluía (pode ser parte da resposta do I13) | MÉDIA | ❓ negócio | Migração 2026-06-10 |
| 7 | Lookups posicionais | Remover união com CSVs legados `lookups/base_run_off` e `lookups/cta_dia` — downdircontas como fonte única (regra do analista "sem carga prévia em tabela"; elimina os WARNs de lookup opcional) | MÉDIA | 🔄 pendente (usuário confirmou que não alimenta as pastas) | Msg analista 2026-06-12 |
| 8 | Lookups posicionais | Unificar normalização de conta entre fontes (downdir = trim; CSV legado = lpad 19 zeros; XML = 19 primeiros chars do Contrt). Risco real descartado em 2026-06-12 (SOMA_MOD provou RUNOFF casando) — vira robustez | BAIXA | 🔄 pendente | Análise 2026-06-12 |
| 9 | Lookups posicionais | Confirmar com analista os filtros herdados do DataStage no downdircontas: linha < 175 chars descartada e `NUM_ORGNZ` (pos 1-3) ∉ {000, 999} (header/trailer) | BAIXA | ❓ analista | Análise 2026-06-12 |
| 10 | Cosmético | Renomear `status_runoff_values` → `cta_dia_status_values` (a lista é da exclusão CTA_DIA, não do RunOff) | BAIXA | 🔄 pendente | Análise 2026-06-12 |
| 11 | Observabilidade | **Log de contagem no job**: (a) contas RunOff lidas do downdircontas × Ops casadas (`FiltroTabela=1`); (b) contas exclusão CTA_DIA lidas × Ops `Mod=1904` removidas; (c) Ops por arquivo (PPBANK × BASE). 3 counts sobre `work/normalized` — falha silenciosa de join vira evidência no log | MÉDIA | 🔄 parcial: **reneg feito 2026-06-15** (linhas cruas/válidas do ctrl_divda_reneg + Ops com `VerificaReneg=1`); faltam os counts RunOff/CTA_DIA/por-arquivo | Análise 2026-06-12 |
| 12 | Observabilidade | Detector de drift de layout na ENTRADA: ler 1º MB no driver, comparar tags/atributos com os schemas explícitos, `WARN` para tag não mapeada (hoje spark-xml descarta em silêncio) | MÉDIA | 🔄 pendente | Backlog 2026-06-11 |
| 13 | Observabilidade | Validação da SAÍDA contra XSD com `lxml` no fim do job (amostra de fragments + header/footer, falha dura). XSD no S3. **Atenção: fonte da verdade = leiaute oficial/validador; XSD local foi corrigido em 2026-06-12** | MÉDIA | 🔄 pendente | Backlog 2026-06-11 |
| 14 | Validação local | Script `tools/validate_cadoc3040.ps1` (PowerShell): checa S81/B01/C83/I13/RunOff/CTA_DIA/estrutura nos arquivos baixados, antes do validador oficial | — | ✅ feito 2026-06-12 (testado com fixtures) | Sessão 2026-06-12 |
| 15 | Teste | Rodar `tests/smoke_test.py` em ambiente com PySpark (parte 2 + caso novo Venc+Inf da ordem B01) | ALTA | 🔄 pendente (máquina local sem PySpark) | 2026-06-11/12 |
| 16 | Homologação | Checklist vs DataStage: comparar `soma_modalidade`, contagens (Ops, PPBANK, BASE_RUN_OFF, duplicados, cd2_alterados), diff amostral de fragments `<Cli>` | ALTA | 🔄 pendente | README_otimizacao |
| 17 | Homologação | Confirmar com saída real os overrides PPBANK: IPOC prefixo `09516419` (=PPBANK_CNPJ8, validador já confirmou via S81) e CEP `05317020` | MÉDIA | 🔄 parcial (S81 ok; CEP pendente) | 2026-06-11 |
| 18 | Infra validador | Robustez do `validar_cadoc3040_Bacen.sh` (servidor /xetl): não deletar o `.log` do Bacen, capturar output fora do padrão `[SCR2]`, exigência de gawk no `match()` 3-args, `-Xmx` p/ 19GB, resumo "ARQUIVO OK" inconsistente | MÉDIA | 🔄 pendente | Diagnóstico 2026-06-11 |
| 19 | Performance (opcional) | Split de parse 32MB (mais paralelismo na leitura dos 20GB) | BAIXA | 💡 sugestão (não necessária: job roda em ~18-20 min) | Run 2026-06-11 |
| 20 | Performance (opcional) | Assembly dos 2 XMLs finais em paralelo (2 threads no driver) | BAIXA | 💡 sugestão | Run 2026-06-11 |
| 21 | Performance (opcional) | `upload_part_copy` server-side no assembly multipart (evita download/upload no driver) | BAIXA | 💡 sugestão | Run 2026-06-11 |

## Mudanças DSX 09/06 → 15/06 (portadas para o Glue em 2026-06-15)

> Comparação dos exports DataStage `cadoc3040_ctcr_20260609` vs `..._20260615` (análise GPT 5.5,
> validada stage a stage contra as derivations). 12 pontos. **Premissa do projeto:** trocou Oracle
> por arquivos, mas TODO o tratamento/WHERE/derivação é no Glue. Edições na cópia do Repo_IA; a
> cópia de deploy (G:) o usuário sincroniza só ao finalizar.

| # | Mudança | Status no Glue | Detalhe |
|---|---------|----------------|---------|
| D1 | "Operação baixada PicPay" (`Trf_Reneg`): `vOperBaixadaPipcay=1` se `Tp_2='0316' & Qtd='2'`; se `VerificaReneg≠1` → nula `Cd_2/Ident/Valor/Qtd` e `Tp_2='0301'` | ✅ feito 2026-06-15 | nulo de VerificaReneg conta como não-reneg (`!=1`) — confirmado pelo DSX (`vVerificaReneg=0` se ≠'1') |
| D2 | `CTRL_DIVDA_RENEG`: replicar a **query Oracle inteira** no dump cru (S3 tem o dump CRU da tabela, confirmado GPT 2026-06-15): `WHERE IND_RENEG>1 AND NUM_ORGNZ=212 AND HOR_ATULZ>SYSDATE-65`; derivar `CliCd`/`Contrt19`/`MES_MOVTO_ACORD`/`NUM_OPER_RENEG_PCELD`=lpad(NUM_CONTR,15,'0')/`VAL_RENEG_OPER_CATAO`; **`VerificaReneg='1'` DERIVADO** (não é coluna física — era lido e virava null no dump → bug latente que zerava reneg/PicPay/IPOC/Alterados) | ✅ feito 2026-06-15 | filtro só de HOR_ATULZ estava **incompleto** → adicionados `IND_RENEG>1`+`NUM_ORGNZ=212`; `VerificaReneg` passou a ser derivado do match do join; `SYSDATE=current_date`; parser de data tolerante (ISO/dd-MON-yyyy/timestamp). **+ log de contagem** (cruas/válidas/HOR não-parseado/Ops com match) p/ flagrar falha silenciosa. **Validar no arquivo real:** formato de `HOR_ATULZ`/`DAT_PROCM` e nomes das colunas |
| D3 | Dedup `vRegDuplicado` (Contrt+Mod, Tp_2 in 0310/0399) removida do `Trf_Filtra` | ✅ feito 2026-06-15 | neutralizado p/ 0 (15/06 só tem `FiltroTabela` 1/0 + `VerificaReneg=1`) |
| D4 | `Cd_2` direto (sem rewrite `0299`); `CADOC3040_Alterados` por `VerificaReneg=1` | ✅ feito 2026-06-15 | removidos filtro/TIPO/parte1-3/vCD2; XML usa `Cd_2`; auditoria dispara por reneg |
| D5 | IPOC de renegociação (NOVO no Glue — não existia) | ✅ feito 2026-06-15 | só `VerificaReneg=1`: `'0951641902991' + (Cd se Tp='1' senão DetCli[1..8]) + right('0'×15 + NUM_OPER_RENEG_PCELD, 15)`; senão mantém IPOC de origem |
| D6 | `mod_soma_FIS` (Copy_of_Trf_Xml agrega só colunas de valor) | ✅ já coberto | agregador do Glue já seleciona só `dt_data/MODALIDADE/<valores>` |
| D7 | `TotalCli` direto de `Le_CadocMensal_temp` | ✅ equivalente | Glue conta blocos `<Cli>`; comparar amostra na homologação |
| D8 | `FatAnual`/`PorteCli` passthrough (sem recalcular no Trf_Reneg) | ✅ já correto | Glue já passa direto da origem |
| D9 | Limpeza de `.ds` (DS_CTCR_*) | n/a | DataStage-only (Glue usa parquet `overwrite`) |
| D10 | `Validador_Bacen` + `Envia_slack` no fim da malha | fora do Glue | orquestração Control-M pós-geração, não tratamento de dado |
| D11 | Soma modalidade "F" no 0005 (`SOMA_MOD_F`, `Trf_Filtra_F`, `Ora_BASE_RUN_OFF_f`, `LKP_01_BASE_RUN_OFF_F`, `AGG_*_f`) | ✅ feito 2026-06-15 | GPT confirmou: 2 XLSX DISTINTOS (`CADOC3040_CTCR_DSTG_SOMA_MOD` + `CADOC3040_CTCR_SOMA_MOD`), mesmas 3 abas (`TOTAL FIS`/`TOTAL RUNON`/`TOTAL RUNOFF`); colunas de valor só repassadas/somadas (não mudam pré/pós-reneg). Glue já gerava os 2 arquivos com nomes certos; **faltava o layout em 3 abas → implementado** (`_write_xlsx_sheets`). Sem pipeline `_F` duplicado (números reaproveitados do `agg_modalidade_df`) |
| D12 | Regra de data `Pj_CTCR_0001` (se `Dt_Data` no mês corrente do sistema → usa; senão → último dia do mês anterior) | ✅ decisão: MANTER | GPT concordou com a recomendação: `dt_data` segue vindo da entrada/header, sem regra dependente de `SYSDATE`. É param de orquestração; aplicar no Glue mudaria partição/nome conforme a data de execução (perigoso em reprocessamento). Só implementar se o negócio exigir o snap fora do mês corrente |

**Limpeza S3 (política fina, feito 2026-06-15):** `_tmp/` SEMPRE apagado (~19GB scratch); `work/`+`xml_ready/`
sob `KEEP_WORK_PARQUET`; `aggregates/`+`audit/` sob **novo** `--KEEP_AUDIT` (default manter, são evidência
de homologação); `final/delivery/` nunca tocado. Sem impacto de desempenho (S3 LIST+DELETE pós-assembly,
sem recomputo Spark). Antes: flag único apagava tudo junto e só com `KEEP_WORK_PARQUET=false`.

**Smoke test estendido (2026-06-15):** Parte 1b (PicPay: 4 casos) + Parte 1c (IPOC de reneg: PF/PJ/não-reneg);
removidas as asserts de `vRegDuplicado` (dedup não existe mais). Rodar em ambiente com PySpark.

## Evidências já coletadas (2026-06-12)

- **SOMA_MOD (print do usuário)**: linhas RUNOFF com valores reais (0204=36.210,15; 0210=3.069.643; 1904=227.853 VlrContBr) e RUNON+RUNOFF ≈ FIS → **join do RunOff confirmado funcionando**; risco de normalização de conta descartado como bug ativo (item 8 rebaixado para robustez). `VlrContr=0.00` na 1904 é normal (cartão).
- **Regras do analista (RunOff pos 592='S' + conta pos 7/19; exclusão CTA_DIA status pos 83/84 ∈ {B,H,I,J,L,M,Q,S,U,Y,X} ou atraso pos 171/5 > 5 + Mod 1904)**: já implementadas fielmente em `_derive_downdircontas_lookups` — **nenhuma alteração de regra necessária**.
- **WARNs de "lookup opcional não encontrado"**: benignos (CSVs legados ausentes; usuário não alimenta as pastas) — somem com o item 7.

## Próxima ação

Rodar `tools/validate_cadoc3040.ps1` no XML PPBANK novo (+ downdircontas) → se PASS em S81/B01/RUNOFF/CTA_DIA, sobram apenas itens ❓ (C83/I13 com o analista) e os 🔄 de robustez/observabilidade.
