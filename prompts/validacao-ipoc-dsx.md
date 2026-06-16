# Validação no DSX — montagem e local do IPOC de renegociação (CADOC 3040)

> Prompt para um modelo com acesso ao export `.dsx` do job `cadoc3040_ctcr`
> (IBM DataStage). Objetivo: confirmar, lendo as *derivations* reais, **onde** o
> IPOC de operação renegociada é gravado no XML de saída (`<Inf Cd>` vs `<Op IPOC>`)
> e a **fórmula exata** (incl. pad do CPF), para alinhar a migração Glue/PySpark.
>
> Cole o bloco abaixo no modelo do trabalho junto com o `.dsx`.

---

```
Você é um especialista em IBM DataStage. Tenho o export .dsx do job
`cadoc3040_ctcr` (CADOC 3040 / Bacen-SCR, export 2026-06-15) e preciso que você
VALIDE, lendo as derivations reais do DSX, como o IPOC de operação renegociada é
montado e ONDE ele é gravado no XML de saída. Não suponha — cite o stage, o link
e a expressão exata de cada resposta.

## Contexto
Estamos migrando esse job de DataStage+Oracle para AWS Glue/PySpark. Comparando o
XML regulatório antigo (gerado pelo DataStage) com o novo (gerado pelo Glue), achei
uma divergência no IPOC de renegociação:
- No arquivo do DataStage, o IPOC montado (prefixo + documento do cliente + contrato
  renegociado) aparece no atributo `Cd` do elemento `<Inf>` (ex.: <Inf Cd="0951641902991...">).
- No arquivo do Glue, o `<Inf Cd>` está VAZIO e quem foi sobrescrito com esse valor
  foi o atributo `IPOC` do elemento `<Op>` (<Op IPOC="0951641902991...">).

Preciso saber qual dos dois é o comportamento correto do DataStage.

## Perguntas (responda uma a uma, citando stage + derivation do DSX)
1. LOCAL DE GRAVAÇÃO (o mais importante): no mapeamento do XML de saída, a coluna que
   recebe o IPOC de renegociação é mapeada para:
   (a) o atributo `Cd` de um elemento `<Inf>`, ou
   (b) o atributo `IPOC` do elemento `<Op>`?
   Trace de qual coluna de saída (ex.: vIPOC) sai esse valor e para qual nó XML ela vai
   no stage de escrita (ex.: Pj_CTCR_0003 / Copy_of_Trf_Xml / o XML Output stage).

2. PRESERVAÇÃO: quando a operação é renegociada, o DataStage SUBSTITUI o IPOC original
   da `<Op>` por esse valor montado, ou MANTÉM o IPOC original da Op e apenas ADICIONA
   um `<Inf>` novo com o valor montado no `Cd`? (Quantos `<Inf>` a Op passa a ter?)

3. FÓRMULA EXATA do valor montado (campo a campo, com tamanho/pad de cada parte):
   - Prefixo fixo: confirme se é `'0951641902991'` e como ele se decompõe
     (suspeito: CNPJ8 `09516419` + `0299` + `1`).
   - Documento do cliente: é `Cd` quando `Tp='1'` (PF) e `DetCli[1..8]` quando PJ?
     >>> O CPF/documento recebe lpad/rpad para 11 posições, ou entra cru (trim só)? <<<
   - Contrato: é `NUM_OPER_RENEG_PCELD` justificado à direita em 15 posições com zeros
     à esquerda? E `NUM_OPER_RENEG_PCELD = lpad(NUM_CONTR, 15, '0')` vindo de
     CTRL_DIVDA_RENEG?

4. CONDIÇÃO DE DISPARO: a montagem só ocorre quando `VerificaReneg = 1`? E
   `VerificaReneg` é coluna física da CTRL_DIVDA_RENEG ou é derivado de "houve match
   no join com a CTRL_DIVDA_RENEG" (linha existe no controle de renegociação)?

## Implementação ATUAL no Glue (para você comparar e apontar divergência)
- Dispara só se VerificaReneg=1; senão mantém o IPOC de origem.
- IPOC = '0951641902991' + documento + contrato, onde:
    documento = Cd            (se Tp='1')
              = DetCli[1..8]  (senão)      -- NOTA: NÃO há pad para 11 hoje
    contrato  = right('0'*15 + trim(NUM_OPER_RENEG_PCELD), 15)
- Esse valor é gravado no atributo IPOC da <Op> (NÃO em <Inf Cd>).
- No arquivo PPBANK, os 8 primeiros dígitos do IPOC são trocados pelo CNPJ8 do header.

Aponte EXATAMENTE onde o Glue diverge do DataStage (lugar de gravação e/ou pad do CPF).

## Bônus (se conseguir, no mesmo DSX) — me ajuda em dois achados relacionados
A) SPLIT PPBANK x BASE_RUN_OFF: no DataStage, as operações de renegociação/baixadas
   (Tp 0316) iam para o MESMO arquivo principal, ou eram separadas num arquivo de
   run-off à parte? Qual stage/condição faz esse roteamento (FiltroTabela / lookup
   BASE_RUN_OFF / RUNOFF_IND)? Quero saber se um cliente podia ter operações divididas
   entre os dois arquivos.
B) Cd_2 (atributo Cd do <Inf>): no export 2026-06-15, existe ainda algum rewrite do
   Cd_2 (ex.: prefixo 928949220210 -> '0299', coluna vCD2 no Trf_Filtra), ou o Cd_2
   passa direto? Se o `<Inf Cd>` é alimentado por esse rewrite, isso explicaria o
   campo vazio no Glue.

## Formato da resposta
Para cada item: [stage] + [coluna/link] + [expressão exata do DSX] + conclusão objetiva
(SIM/NÃO/qual dos dois). Se algo não der pra determinar no DSX, diga explicitamente
"não determinável no export" em vez de inferir.
```
