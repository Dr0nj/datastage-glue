# CADOC 3040 — Job Glue otimizado (DataStage → PySpark, S3-only)

## Por que o job original travava 3h

| # | Problema | Sintoma no log | Efeito |
|---|----------|----------------|--------|
| 1 | Leitor XML nativo do Glue não é splittable | `nonSplittable: true`, `submitting 1 missing task`, executores em polling | 1 core parseando 20GB sozinho |
| 2 | `Relationalize` computado e nunca usado | `fromRDD at DynamicFrame.scala` | 1 pass completo extra nos 20GB |
| 3 | `monotonically_increasing_id` como chave de join (explode Inf → pivot → 3 joins) | shuffles gigantes | Custo alto **e risco de join errado** (ID não é estável entre recomputações) |
| 4 | `Window.orderBy` global sem `partitionBy` na regra de duplicidade | 1 task gigante no stage de sort | Dataset inteiro em 1 partição |
| 5 | Nenhum cache/checkpoint, ~12 actions | — | Cada write/count reparsea os 20GB |

## O que mudou no script otimizado

1. **spark-xml** (`com.databricks.spark.xml`, rowTag=`Cli`) com **schema explícito** → leitura paralela em ~160–640 tasks; sem scan de inferência.
2. Relationalize / ResolveChoice / DropNullFields removidos do caminho dos 20GB.
3. v20..v330, primeiro `Inf` e primeiro `Gar` extraídos com funções de array (`filter` + `try_element_at`) direto da struct `Op` → zero shuffle, zero join, determinístico.
4. Janela de duplicidade particionada por `(Contrt, Mod)`.
5. **Checkpoint único**: dataset de negócio materializado em `work/normalized` (parquet); todos os ramos (7 parquets, fragments XML, counts, xlsx) leem do parquet. O XML é parseado **exatamente 1 vez**.
6. `TotalCli` via `groupBy().count()` único.
7. Montagem do XML final único mantida (fragments distribuídos + multipart upload S3 — esse design já estava correto).
8. **Layout 2026 (Codoc3040_2026.xsd + amostra real DataStage, 2026-06-11):**
   - Fragment = **1 `<Cli>` por cliente** com todas as `<Op>` aninhadas, ordenadas pela posição original
     (`op_pos`). Antes era 1 `<Cli>` por Op (Cd repetido) — XML bem-formado mas estrutura errada.
   - Filhos de `<Op>` na ordem do `xs:sequence`: `Inf`, `Venc`, `Gar`, `ContInstFinRes4966` (com filho `Estagio`).
   - Elementos opcionais e atributos sem valor são **omitidos** (não saem vazios).
   - `_IPOC` adicionado ao schema de leitura e emitido (antes era silenciosamente descartado).
   - v20..v330: `coalesce(<Venc v..>, pivot de <Inf Cd=..>)` — cobre layout 2026 e layout antigo.
   - `VlrContBr`: `coalesce(Op._VlrContBr, ContInstFinRes4966._VlrContBr)` para os agregados.
   - `TotalCli` no header = nº de blocos `<Cli>` (clientes) por arquivo/dt_data.
   - PPBANK: `IPOC` → prefixo `09516419` + resto; `CEP` → `05317020` (validar na homologação).
   - `VlrContr` no XML sai **raw** (passthrough), não mais o `_dec` zero-filled (zero fabricado violava a
     regra de omitir atributos sem valor).
   - **Saída indentada por elemento** (1 espaço por nível: `Op`=1, filhos=2, `Estagio`=3): a indentação entra
     como literais `\n `+espaços SEMPRE fora das tags (tag nunca cortada); Op sem filhos abre/fecha inline;
     cada `<Cli>` segue sendo 1 registro atômico no write/assembly. Custo: alguns % de whitespace no arquivo.

## Configuração do job (Glue 5.0)

JARs (baixar do Maven Central, subir no S3, passar em `--extra-jars` separados por vírgula):

- `spark-xml_2.12-0.18.0.jar` (com.databricks)
- `txw2-3.0.2.jar` (org.glassfish.jaxb)
- `xmlschema-core-2.3.0.jar` (org.apache.ws.xmlschema)

Workers: comece com **10 × G.2X** (80 cores). Se quiser <30 min, 20 × G.2X.

Parâmetros: os mesmos do job original (`--S3_INPUT_PATH`, `--S3_OUTPUT_PATH`, opcionais `--P_CNPJ` etc.).

**Regras de ouro:**
- O XML de entrada deve permanecer **sem compressão** (gzip torna o arquivo não-splittable de novo). Se um dia vier .gz, descomprimir no S3 antes.
- Não habilitar job bookmark (não há `transformation_ctx`; o job é full-load por natureza).

## Estimativa de tempo

Parse paralelo de 20GB (~80 cores): 10–20 min. Pipeline completo (parse + shuffle de dedup + writes + assembly do XML final no driver): **~30–60 min**. O assembly do XML único é sequencial no driver (~streaming S3 a ~100–200 MB/s), conte ~3–6 min por arquivo final de ~20GB.

## Veredicto de viabilidade

**O projeto é viável.** Nada no requisito (XML 20GB in → XML + regulatório out, sem Oracle) é incompatível com Spark/Glue. O travamento era 100% de implementação (leitor não-splittable + recomputações), não de plataforma. Não há motivo técnico para parar o desenvolvimento.

## Checklist de validação vs DataStage (rodar antes de homologar)

1. Comparar `soma_modalidade` (XLSX) com a saída DataStage — é o melhor "checksum" de negócio.
2. Comparar contagens: total de Ops, PPBANK, BASE_RUN_OFF, duplicados, cd2_alterados.
3. Diff amostral de fragments `<Cli>` (mesmo contrato) entre saída nova e DataStage.
4. Validar o XML final contra o `Codoc3040_2026.xsd` (ex.: `xmllint --noout --schema Codoc3040_2026.xsd amostra.xml`)
   e fazer diff estrutural com a amostra real do DataStage.
5. **Pontos de atenção (atualizados em 2026-06-11):**
   - ~~Só primeiro `<Inf>`/`<Gar>`~~ **resolvido**: o XSD 2026 permite no máximo 1 `Inf`/`Venc`/`Gar`/`ContInstFinRes4966`
     por Op (`maxOccurs` default = 1). Emitir o primeiro é o comportamento correto.
   - Os campos do "primeiro Inf" no original eram escolhidos por `first(ignorenulls)` **por coluna**, sem ordem garantida (não-determinístico, podia misturar campos de Infs diferentes). A versão nova é determinística (primeiro não-nulo na ordem do XML). Pode haver diferenças pontuais vs execuções do script antigo — a versão nova é a defensável.
   - ~~`TotalCli` por linha Op~~ **resolvido**: agora conta clientes (blocos `<Cli>`).
   - ~~Coluna `ipoc` inexistente~~ **resolvido**: era o atributo `_IPOC` descartado pelo schema de leitura explícito.
   - `cpf_deletar_contrato` e `is_tp_0316` são calculados mas **nenhum filtro os aplica** (herdado do original — no DataStage há exclusão? validar).
   - Override PPBANK de `IPOC` (prefixo `09516419`) e `CEP` (`05317020`): inferido das colunas `ipoc_ppbank`/
     `CEP_PPBANK` que o pipeline preparava sem usar; confirmar contra a saída real do DataStage.
   - Se a entrada estiver no layout 2026, o `soma_modalidade` de execuções anteriores estava com v-codes
     **zerados** (lidos do lugar errado). Comparações históricas devem usar esta versão em diante.
