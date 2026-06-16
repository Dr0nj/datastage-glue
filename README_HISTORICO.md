# Glue-datastage — Migração CADOC 3040 (DataStage/Oracle → AWS Glue/PySpark)

> Contexto persistido em 2026-06-10 a partir de sessão de agente local (Claude).
> Se estamos retomando este projeto: leia este arquivo inteiro antes de mexer em qualquer coisa.

## O problema (cenário de trabalho)

Processo regulatório **CADOC 3040 (Bacen)** que rodava em **DataStage + Oracle** precisa migrar para
**AWS Glue / PySpark sem Oracle**, operando só com arquivos em S3:

- **Entrada:** 1 arquivo XML de ~20GB (estrutura `Doc3040 > Cli > Op > Inf/Gar`), sem compressão.
- **Saídas:** 1 XML consolidado (mesmo layout 3040) + arquivo regulatório + parquets intermediários + XLSX de conferência (`soma_modalidade`).

A primeira tentativa de migração foi um script gerado pelo **GPT 5.5** ([original/glue_ctcr_cadoc3040_full.py](original/glue_ctcr_cadoc3040_full.py)),
que ficou **3h+ travado** com log `submitting 1 missing task`, `nonSplittable: true`, `fromRDD at DynamicFrame.scala:297`,
4 executores vivos só em polling.

## Diagnóstico (por que travava)

| # | Problema no script do GPT | Sintoma no log |
|---|---------------------------|----------------|
| 1 | Leitor XML nativo do Glue **não é splittable** → 1 core parseando 20GB | `nonSplittable: true`, `submitting 1 missing task` |
| 2 | `Relationalize` computado e **nunca usado** (1 pass extra nos 20GB) | `fromRDD at DynamicFrame.scala` |
| 3 | Joins por `monotonically_increasing_id` (explode→pivot→3 joins) — caro **e bug de corretude** (ID não estável) | shuffles gigantes |
| 4 | `Window.orderBy` global sem `partitionBy` na dedup | dataset inteiro em 1 partição |
| 5 | Sem cache/checkpoint, ~12 actions → cada write reparsearia os 20GB | — |

## Veredicto de viabilidade (resposta dada ao time)

**O projeto É VIÁVEL.** Nada no requisito (XML 20GB in → XML + regulatório out, sem Oracle) é
incompatível com Spark/Glue. O travamento era 100% implementação, não plataforma.
**Não há motivo técnico para parar o desenvolvimento.**

## A solução — [glue_ctcr_cadoc3040_optimized.py](glue_ctcr_cadoc3040_optimized.py)

Mesmas entradas, mesmas saídas, mesmos parâmetros do job (`--S3_INPUT_PATH`, `--S3_OUTPUT_PATH`, opcionais `--P_CNPJ` etc.). Mudanças:

1. **spark-xml** (`com.databricks.spark.xml`, `rowTag=Cli`) com **schema explícito** → leitura paralela (~160–640 tasks).
2. Relationalize / ResolveChoice / DropNullFields removidos.
3. v20..v330, primeiro `Inf` e primeiro `Gar` extraídos com funções de array (`filter` + `try_element_at`) direto da struct `Op` — zero shuffle, zero join, determinístico.
4. Window de dedup particionada por `(Contrt, Mod)`.
5. **Checkpoint parquet único** (`work/normalized`): o XML é parseado exatamente 1 vez; todos os ramos leem do parquet.
6. Montagem do XML final único mantida (fragments distribuídos + multipart upload S3 — design que já estava correto).
7. **Layout 2026** ([Codoc3040_2026.xsd](Codoc3040_2026.xsd) + amostra real, 2026-06-11): `<Cli>` agrupado por
   cliente com todas as `<Op>` aninhadas (ordenadas por `op_pos`); leitura+emissão de `IPOC`, `<Venc v20..v330>`,
   `<ContInstFinRes4966>`/`<Estagio>`, `IniRelactCli`, `CongEcon`, `DetCli`; elementos/atributos vazios omitidos;
   `TotalCli` = clientes por arquivo; header preserva `MetodApPE`/`MetodDifTJE`.

Detalhes técnicos completos em [README_otimizacao_cadoc3040.md](README_otimizacao_cadoc3040.md).
Smoke test (lógica de extração/dedup, PySpark local 3.5, passou em todos os asserts) em [tests/smoke_test.py](tests/smoke_test.py).

## Configuração do job Glue (5.0)

**3 JARs obrigatórios** (o Glue não traz o spark-xml). Baixar do Maven Central, subir num bucket S3 e
apontar em *Job details → Advanced properties → Libraries → Dependent JARs path* (separados por vírgula, sem espaços)
— equivale a `--extra-jars`:

```
https://repo1.maven.org/maven2/com/databricks/spark-xml_2.12/0.18.0/spark-xml_2.12-0.18.0.jar
https://repo1.maven.org/maven2/org/glassfish/jaxb/txw2/3.0.2/txw2-3.0.2.jar
https://repo1.maven.org/maven2/org/apache/ws/xmlschema/xmlschema-core/2.3.0/xmlschema-core-2.3.0.jar
```

- Workers: **10 × G.2X** (80 cores); 20 × G.2X se quiser <30 min. Estimativa total: **30–60 min**.
- Entrada **sem gzip** (gzip mata a splittability). Job bookmark desabilitado. Falta de JAR aparece como `ClassNotFoundException` no início do log.
- A role do job precisa de leitura no bucket dos JARs.

## Histórico de execução / bugs corrigidos

1. **Run 1 (script do GPT):** 3h+ travado no parse — morto. Causa: itens 1–5 acima.
2. **Run 2 (script otimizado):** passou da leitura dos 20GB (✓ prova que o fix funcionou) e estourou na
   montagem do XML final: `unresolved column ... 'Autorzc' cannot be resolved`.
   **Causa:** bug herdado do GPT — PPBANK/BASE_RUN_OFF eram selecionados com `base_columns`, que não incluía
   `Autorzc`, `PorteCli`, `Cosif` e ~30 outras colunas que o gerador de XML usa. O original tinha o mesmo
   defeito latente, só nunca tinha chegado nessa etapa.
   **Fix aplicado:** lista `XML_FRAGMENT_COLUMNS` adicionada e incluída nos dois selects (já está no script desta pasta).
3. **Run 3:** pendente na data deste snapshot — usuário ia resubir o script corrigido e rodar do zero
   (tudo usa `mode('overwrite')`, não precisa limpar nada).
4. **2026-06-11 — correção de layout 2026 (XSD + amostra real):** usuário forneceu o
   [Codoc3040_2026.xsd](Codoc3040_2026.xsd) e uma amostra real de `<Cli>` e reportou tags faltando na saída.
   Diagnóstico: (a) job emitia 1 `<Cli>` por Op em vez de agrupar as Op do cliente; (b) `_IPOC` era descartado
   pelo schema de leitura (era a "coluna ipoc inexistente"); (c) `<Venc v20..v330>` não era lido nem emitido —
   os v-codes eram buscados só no pivot de `<Inf Cd=...>` (suposição do GPT; no layout 2026 vêm do `<Venc>`);
   (d) `<ContInstFinRes4966>`/`<Estagio>` (Res. 4966) não existiam no job; (e) `<Inf>`/`<Gar>` saíam vazios
   quando sem dados; (f) `IniRelactCli`/`CongEcon`/`DetCli` lidos mas não emitidos. Tudo corrigido no script
   (v-codes via `coalesce(Venc, pivot Inf)` — funciona nos dois layouts). PPBANK: `IPOC` recebe prefixo
   `09516419` e `CEP` fixo `05317020` (colunas `ipoc_ppbank`/`CEP_PPBANK` que o DataStage preparava agora são
   aplicadas no XML — **validar na homologação**). Smoke test estendido (parte 2: agrupamento/omissão) —
   ainda não executado (máquina local sem PySpark/Java); rodar no ambiente de trabalho.
5. **Run com layout 2026 (2026-06-11): SUCESSO — ~19GB escritos em 18 min.** Prova que leitura splittable,
   checkpoint e o groupBy do `<Cli>` agrupado escalam (vs 3h+ travado do script original).
5b. **Run da versão indentada (2026-06-11): SUCESSO — 20 min, porém com 40×G.8X (320 DPUs, ~US$47/run).**
   Tempo ≈ igual ao run anterior com cluster pequeno: o job satura em ~300 tasks de parse (split 64MB)
   e tem cauda serial no driver (~6-9 min: assembly multipart dos XMLs + collects + bootstrap).
   **Config recomendada: 10-20×G.2X (~US$3-6/run) + Auto Scaling** — acima disso é custo sem ganho.
   Melhorias de velocidade futuras (se algum dia precisar): split 32MB (+paralelismo do parse),
   assembly PPBANK/BASE em paralelo (2 threads no driver), `upload_part_copy` server-side no assembly.
6. **2026-06-11 (após o run): indentação da saída XML** — usuário aprovou o formato simulado; fragments agora
   saem indentados por elemento (1 espaço por nível: `Op`=1, filhos=2, `Estagio`=3) via prefixos `\n ` fora
   das tags (tag nunca é cortada; cada `<Cli>` segue sendo 1 registro atômico). Op sem filhos abre/fecha na
   mesma linha. Smoke test (parte 2) atualizado com as strings esperadas indentadas.

7. **2026-06-12 — 1ª validação REAL no validador oficial Bacen (Release 13657), arquivo PPBANK:**
   o XML gerado rodou no validador correto e reprovou com 1000+ erros (corte B09). Diagnóstico
   confirmado contra o leiaute oficial (`SCR3040_Leiaute.xls`, aba Doc3040) — 4 causas independentes:
   - **S81 (massivo, BUG corrigido):** header saiu `CNPJ="92894922"` mas o IPOC de toda Op foi
     prefixado com `09516419`. O job só sobrescrevia o CNPJ do header se `--P_CNPJ` fosse passado.
     **Fix:** constante `PPBANK_CNPJ8` (= `--P_CNPJ` ou default `09516419`) usada nos DOIS lugares
     (header + prefixo IPOC) — agora é impossível divergirem.
   - **B01 (estrutural, BUG corrigido):** ordem dos filhos de `Op` era `Inf, Venc, Gar, 4966`
     (vinha do XSD local, que estava ERRADO). Ordem oficial do leiaute (seções b..h):
     **`Venc, Gar, Inf, [Sicor], ContInstFinRes4966`**. O validador rejeita `Venc` depois de `Inf`
     ("One of {Inf, Sicor, ContInstFinRes4966} is expected" — Inf é repetível, por isso aparece na
     lista). **Fix:** ordem trocada no `_build_xml_fragments`, no XSD local e no smoke test
     (caso novo: Op com Venc E Inf). O erro só aparecia em Op com Inf+Venc juntos.
   - **C83 (regra de negócio, PENDENTE):** Op com `EstInstFin` preenchido e sem `Inf` de saída 03xx
     precisa de `<Estagio Motivo= DtAlocacao=>`. Vários registros da fonte têm EstInstFin sem
     Estagio → o erro vem do DADO de entrada (vigência do Estagio é jan/2026; upstream pode ainda
     não estar gerando). Decidir com o negócio: preencher de outra origem, suprimir o
     EstInstFin nesses casos, ou corrigir upstream. Obs. estrutural: o leiaute permite **N**
     `<Estagio>` por 4966 (e também `<Perda>`), mas o job lê/emite no máx. 1 Estagio e nenhum Perda.
   - **I13 (regra de negócio, PENDENTE):** clientes com soma de vencimentos < R$ 200 (sem info
     adicional de saída 03xx) não podem ser individualizados. Não há filtro disso no job (paridade
     com a suspeita antiga: `cpf_deletar_contrato`/`is_tp_0316` calculados e nunca aplicados).
     Agravante possível: o split PPBANK × BASE_RUN_OFF é por Op (`FiltroTabela` por conta) — as Op
     de um cliente podem se dividir entre os 2 arquivos e a soma POR ARQUIVO cair abaixo de 200
     mesmo com o total ≥ 200. Confirmar como o DataStage tratava (exclusão? agregado?).
   - Lembrete: o validador corta em 1000 erros (B09) — depois dos fixes S81/B01 a contagem real
     de C83/I13 vai aparecer.

8. **2026-06-15 — mudanças do DSX 09/06→15/06 portadas pro Glue (5 regras + limpeza S3).** Usuário
   trouxe a análise do GPT 5.5 comparando os exports DataStage `cadoc3040_ctcr_20260609` vs
   `..._20260615` (12 pontos, validada stage a stage). Premissa: trocou Oracle por arquivos, mas
   TODO o tratamento é no Glue. Implementado em `glue_ctcr_cadoc3040_optimized.py`:
   - **PicPay baixada (Trf_Reneg):** `vOperBaixadaPipcay=1` se `Tp_2='0316' & Qtd='2'`; quando
     `VerificaReneg≠1` (nulo conta como não-reneg, confirmado pelo DSX) zera `Cd_2/Ident/Valor/Qtd`
     e troca `Tp_2='0301'`.
   - **CTRL_DIVDA_RENEG = dump cru da tabela** (confirmado com o GPT) → o Glue replica a **query Oracle
     inteira** em PySpark: `WHERE IND_RENEG>1 AND NUM_ORGNZ=212 AND HOR_ATULZ>SYSDATE-65` + derivações
     (`Contrt19`, `NUM_OPER_RENEG_PCELD=lpad(NUM_CONTR,15,'0')`, etc.). **`VerificaReneg` é DERIVADO**
     (`'1'` p/ toda linha válida; após o left join, sem match = null = 0) — antes era lido como coluna,
     o que viraria `null` no dump cru e **zeraria silenciosamente** PicPay/IPOC/Alterados (bug latente
     corrigido). `SYSDATE=current_date`; parser de data tolerante. **+ log de contagem** (linhas
     cruas/válidas/`HOR_ATULZ` não parseado + Ops com match) pra flagrar falha silenciosa.
   - **Dedup removida:** `vRegDuplicado` (Contrt+Mod, Tp_2 in 0310/0399) saiu do Trf_Filtra (neutralizado p/ 0).
   - **Cd_2 direto:** removido o rewrite `0299` (filtro/TIPO/parte1-3/vCD2); o XML usa `Cd_2` direto e
     `CADOC3040_Alterados.txt` passa a disparar por `VerificaReneg=1`.
   - **IPOC de renegociação (NOVO — não existia no Glue):** só para `VerificaReneg=1`,
     `IPOC = '0951641902991' + (Cd se Tp='1' senão DetCli[1..8]) + NUM_OPER_RENEG_PCELD a 15 posições`;
     senão mantém o IPOC de origem. O prefixo embute o CNPJ8 09516419 (=PPBANK_CNPJ8); a sobrescrita
     PPBANK posterior normaliza os 8 primeiros dígitos (S81 preservado mesmo com `--P_CNPJ` custom).
   - **Limpeza S3 (política fina):** `_tmp/` (~19GB scratch) SEMPRE apagado; `work/`+`xml_ready/` sob
     `KEEP_WORK_PARQUET`; `aggregates/`+`audit/` sob **novo** `--KEEP_AUDIT` (default manter, são a
     evidência de homologação); `final/delivery/` nunca tocado. Sem impacto de desempenho.
   - **Já corretos / sem ação:** `mod_soma_FIS` (agregador já só soma colunas de valor), `TotalCli`
     (contagem de `<Cli>`, equivalente), `FatAnual`/`PorteCli` (já passthrough); `.ds` e
     `Validador_Bacen`/`Envia_slack` ficam fora do Glue (orquestração Control-M).
   - Smoke test estendido (Parte 1b PicPay + Parte 1c IPOC de reneg). **Rodar em PySpark.**
   - **D11/D12 resolvidos (mesmo dia, com o GPT 5.5):** D11 — o `_F` gera 2 XLSX distintos
     (`DSTG_SOMA_MOD` + `SOMA_MOD`) com as mesmas 3 abas (`TOTAL FIS`/`TOTAL RUNON`/`TOTAL RUNOFF`);
     os nomes já batiam, faltava o layout em 3 abas → implementado o writer multi-aba
     (`_write_xlsx_sheets`), sem duplicar pipeline. D12 — decidido **manter** `dt_data` da entrada
     (regra é orquestração, perigosa em reprocessamento). Premissas confirmadas: D1 (nulos=não-reneg),
     D2 (`HOR_ATULZ`, `SYSDATE`=execução).
   - Edições na cópia do Repo_IA; **a cópia de deploy (G:, máquina do trabalho) o usuário sincroniza
     só ao finalizar** — G: não fica acessível nas sessões do agente.

## Pendências / próximos passos

> **Catálogo consolidado de melhorias (tabela com status/prioridade): [MELHORIAS.md](MELHORIAS.md)** — é a fonte única; a lista abaixo é o histórico detalhado.

- [x] **DSX 15/06 — D11 (soma `_F`):** GPT confirmou 2 XLSX distintos (`DSTG_SOMA_MOD` + `SOMA_MOD`)
      com as mesmas 3 abas (`TOTAL FIS`/`TOTAL RUNON`/`TOTAL RUNOFF`); valores não mudam pré/pós-reneg.
      Glue já gerava os 2 arquivos; faltava o layout em 3 abas → **implementado** (`_write_xlsx_sheets`),
      sem pipeline `_F` duplicado.
- [x] **DSX 15/06 — D12 (regra de data `Pj_CTCR_0001`):** decisão (GPT concordou) = **manter** o
      comportamento atual (`dt_data` da entrada/header). Reabrir só se o negócio exigir o snap.
- [ ] **DSX 15/06 — homologar** os fixes (PicPay, query reneg completa, dedup removida, Cd_2 direto,
      IPOC de reneg, XLSX 3 abas) vs DataStage 15/06 e revalidar no validador oficial Bacen.
- [ ] **DSX 15/06 — validar o dump cru `CTRL_DIVDA_RENEG` no arquivo real:** formato de `HOR_ATULZ`/
      `DAT_PROCM` e nomes das colunas. No 1º run, olhar o log: se *"válidas pós-filtro = 0"* ou
      *"HOR_ATULZ não parseado"* alto, ajustar o parser/mapping (o reneg estaria silenciosamente zerado).
- [ ] **Melhorias futuras — "XSD embutido no ETL" (paridade com o DataStage; NÃO bloqueiam a execução atual):**
  1. *Detector de drift na entrada*: estender a leitura do 1º MB do XML (driver, já existe para o header)
     para extrair os nomes de atributos/elementos da amostra e comparar com os schemas explícitos do job
     (`CLI_SCHEMA`/`OP_SCHEMA`/`VENC_SCHEMA`/`CONT4966_SCHEMA`/`ESTAGIO_SCHEMA`) — logar `WARN` se a entrada
     trouxer tag não mapeada (hoje o spark-xml descarta em silêncio). Custo ~zero.
  2. *Validação da saída contra o XSD*: no fim do job, montar um mini-Doc3040 com amostra dos fragments
     `<Cli>` + header/footer e validar com `lxml` contra o `Codoc3040_2026.xsd` (subir o XSD num S3, ex.:
     junto dos lookups; adicionar `--additional-python-modules lxml`). Falha dura se a estrutura divergir.
     Decisão combinada: WARN no drift de entrada, falha dura na validação de saída.
- [ ] **Troubleshoot do validador oficial Bacen** (`validar_cadoc3040_Bacen.sh`, servidor /xetl): 1ª execução
      no XML de 19GB falhou com "0 registro(s) no analitico" = Java saiu com erro SEM linhas `[SCR2]`
      capturadas — **não diz nada sobre o XML em si**. Suspeita do usuário: falta de espaço na partição
      (plausível: arquivo truncado na cópia → erro de parse). Bugs do script: deleta o `.log` do Bacen sem
      ler; ignora output fora do padrão `[SCR2]` (crash vira "0 registros" sem pista); resumo pode dizer
      "ARQUIVO OK" com o shell dizendo "falhou"; `match()` 3-args exige gawk; `-Xmx` máx 20g p/ 19GB.
      Diagnóstico na ordem: (1) `df -h` + `tail -c 200 arquivo.xml` (tem que terminar em `</Doc3040>`) +
      comparar `stat -c%s` com o ContentLength do S3; (2) validar amostra pequena
      (`head -n 1001 arquivo.xml > amostra.xml; echo '</Doc3040>' >> amostra.xml` — só funciona no arquivo
      SEM indentação, 1 Cli/linha); (3) rodar o `java` na mão sem pipe, preservando o `.log`.
- [ ] Rodar `tests/smoke_test.py` em ambiente com PySpark 3.5 (parte 2 nova: agrupamento `<Cli>` + omissão de vazios).
- [ ] Resubir o job no Glue e validar a saída XML contra o [Codoc3040_2026.xsd](Codoc3040_2026.xsd)
      (ex.: `xmllint --schema` numa amostra) e contra a amostra real do DataStage.
- [ ] Checklist de homologação vs DataStage (seção final do README_otimizacao): comparar `soma_modalidade`,
      contagens (Ops, PPBANK, BASE_RUN_OFF, duplicados, cd2_alterados), diff amostral de fragments `<Cli>`.
      **Atenção:** se a entrada estiver no layout 2026, `soma_modalidade` das execuções anteriores estava com
      v-codes zerados (eram lidos só do pivot de `<Inf>`); a partir desta versão vêm do `<Venc>`.
- [ ] **C83 (validador 2026-06-12):** decidir com o negócio a regra para Op com `EstInstFin` sem
      `Estagio` (Motivo/DtAlocacao) e sem Inf 03xx — preencher, suprimir EstInstFin ou corrigir upstream.
- [ ] **I13 (validador 2026-06-12):** decidir/portar a exclusão de clientes com soma de vencimentos
      < R$ 200 sem saída 03xx; verificar se o split por Op (FiltroTabela) divide clientes entre os
      2 arquivos e derruba a soma por arquivo.
- [ ] **Estagio repetível + Perda:** leiaute permite N `<Estagio>` e N `<Perda>` por
      `ContInstFinRes4966`; job hoje lê/emite 1 Estagio e ignora Perda (schema struct → virar array).
- [ ] Rodar `tests/smoke_test.py` após os fixes S81/B01 (caso novo Venc+Inf) e revalidar o XML
      PPBANK no validador oficial. Pré-validação local: `tools/validate_cadoc3040.ps1` (PowerShell,
      checa S81/B01/C83/I13/RunOff/CTA_DIA direto nos arquivos baixados; testado em 2026-06-12).
- [ ] **SUGESTÃO — log de contagem no job (observabilidade das regras de arquivo):** 3 counts baratos
      sobre o parquet `work/normalized` logados a cada run: (1) contas RunOff lidas do downdircontas
      e quantas Ops casaram (`FiltroTabela=1`); (2) contas de exclusão CTA_DIA lidas e quantas Ops
      `Mod=1904` foram removidas; (3) Ops por arquivo (PPBANK × BASE_RUN_OFF). Falha silenciosa de
      normalização de conta (join que "não morde") viraria evidência imediata no log. Não aplicado
      ainda — aguardando validação manual com o validate_cadoc3040.ps1.
- [ ] Suspeitas restantes a validar com o negócio:
  1. `cpf_deletar_contrato` / `is_tp_0316` são calculados mas **nenhum filtro os aplica** — no DataStage há exclusão?
  2. Override de `IPOC` (prefixo `09516419` = `PPBANK_CNPJ8`) e `CEP` (`05317020`) no arquivo PPBANK — confirmar com saída real. **Validador confirmou (S81): header e IPOC têm que usar o MESMO CNPJ8 — corrigido em 2026-06-12.**
  3. ~~Só primeiro `<Inf>`/`<Gar>`~~ **resolvido**: XSD 2026 permite no máx. 1 de cada por Op (`maxOccurs` default).
  4. ~~Coluna `ipoc` inexistente~~ **resolvido**: era o atributo `_IPOC` descartado pelo schema de leitura.
  5. ~~`TotalCli` por linha Op~~ **resolvido**: agora conta clientes (blocos `<Cli>`) por arquivo.

## Estrutura desta pasta

```
original/glue_ctcr_cadoc3040_full.py   ← script do GPT 5.5 (referência do que NÃO fazer; era a versão travada)
glue_ctcr_cadoc3040_optimized.py       ← script atual, layout 2026 + Cli agrupado (usar este)
Codoc3040_2026.xsd                     ← XSD oficial do layout 3040/2026 (fonte da verdade da saída)
README_otimizacao_cadoc3040.md         ← doc técnico detalhado da otimização + checklist homologação
MELHORIAS.md                           ← catálogo único de melhorias/pendências (status) + seção DSX 15/06
MANUTENCAO-COMENTARIOS.md              ← como manter os comentários de regra do .py honestos ao alterar
tests/smoke_test.py                    ← smoke test local: extração/dedup + montagem/agrupamento do XML
README.md                              ← este arquivo (contexto completo)
```

> O `.py` tem um bloco-cabeçalho "REGRAS DE NEGÓCIO" com índice + tags de proveniência
> (`[DSX]`/`[BACEN]`/`[ANALISTA]`/`[PENDENTE]`). Ao mexer numa regra, siga o
> [MANUTENCAO-COMENTARIOS.md](MANUTENCAO-COMENTARIOS.md).
