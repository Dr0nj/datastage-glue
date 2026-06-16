# datastage-glue — CADOC 3040 (migração DataStage → AWS Glue/PySpark)

Job AWS Glue que substitui o processo DataStage/Oracle do **CADOC 3040 (Bacen/SCR)**,
operando só com arquivos em S3 (sem Oracle). Entrada: XML ~20GB. Saídas: XML consolidado
(layout Doc3040/2026) + `CADOC3040_Alterados.txt` + 2 XLSX de soma por modalidade + parquets.

> Repositório de transferência: o desenvolvimento/contexto completo (README detalhado,
> histórico, MELHORIAS, XSD) fica no repo de trabalho local. Aqui ficam só os arquivos que
> sobem/rodam.

## Arquivos

| Arquivo | Para que serve |
|---|---|
| `glue_ctcr_cadoc3040_optimized.py` | **O job** — único arquivo que sobe no AWS Glue 5.0 |
| `tests/smoke_test.py` | Smoke test local (PySpark 3.5) das expressões: extração v-codes/Inf/Gar, PicPay, IPOC de reneg, query CTRL_DIVDA_RENEG, montagem do XML 2026 |
| `MANUTENCAO-COMENTARIOS.md` | Como manter os comentários de regra do `.py` honestos ao alterar (tags `[DSX]`/`[BACEN]`/`[ANALISTA]`/`[PENDENTE]`) |

> O `glue_ctcr_cadoc3040_optimized.py` começa com um bloco-cabeçalho **"REGRAS DE NEGÓCIO"**
> (índice + tags de proveniência). Antes de mexer numa regra, leia o `MANUTENCAO-COMENTARIOS.md`.

## Rodar o job (Glue 5.0 / Spark 3.5)

JARs obrigatórios (`--extra-jars`, baixar do Maven Central e subir no S3 — o Glue não traz spark-xml):

```
com.databricks:spark-xml_2.12:0.18.0
org.glassfish.jaxb:txw2:3.0.2
org.apache.ws.xmlschema:xmlschema-core:2.3.0
```

Workers: 10–20 × G.2X. Entrada deve permanecer `.xml` **sem compressão** (gzip mata a splittability).

Parâmetros obrigatórios: `--JOB_NAME --S3_INPUT_PATH --S3_OUTPUT_PATH`
Opcionais: `--FINAL_SINGLE_FILE --GENERATE_SUPPLIER_FILES --KEEP_WORK_PARQUET --KEEP_AUDIT
--P_CNPJ --P_NOMERESP --P_EMAILRESP --P_TELRESP --FINAL_XML_PPBANK_NAME
--FINAL_XML_BASE_RUN_OFF_NAME --DOWNDIRCONTAS_PATH`

## Smoke test

```bash
python tests/smoke_test.py    # precisa de PySpark 3.5 + Java; imprime SMOKE_TEST_OK no fim
```
