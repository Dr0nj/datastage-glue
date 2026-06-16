# Manutenção dos comentários de regra de negócio (`glue_ctcr_cadoc3040_optimized.py`)

> Por que este arquivo existe: o `.py` é o **único artefato que sobe no Glue**, então as
> regras de negócio não-óbvias estão comentadas **dentro dele** (pra ninguém — humano ou IA —
> se perder ao abrir só o código). Este guia diz **como manter esses comentários honestos**
> quando o código mudar. Regra de ouro: **o código diz "o porquê + a fonte"; o
> [MELHORIAS.md](MELHORIAS.md) é a fonte profunda**. Os dois não podem brigar.

## Tags de proveniência (no topo do `.py` e inline)

| Tag | Significado | Onde confirmar |
|-----|-------------|----------------|
| `[DSX]` | Derivação do DataStage (export `cadoc3040_ctcr_20260615`) | comparar com o DSX / análise do GPT na conversa de 2026-06-15 |
| `[BACEN]` | Exigência do validador oficial (Release 13657) / leiaute `SCR3040_Leiaute.xls` | rodar o validador / conferir o leiaute |
| `[ANALISTA]` | Regra confirmada pelo analista de negócio | mensagem do analista (registrada no MELHORIAS / conversa) |
| `[PENDENTE]` | **Suspeita / não confirmado** — NÃO é regra estabelecida | decisão de negócio ou validação no arquivo real |

O índice de regras fica no **bloco-cabeçalho "REGRAS DE NEGÓCIO"** no topo do `.py`. O detalhe
fica **inline**, no ponto de uso.

## Quando você ALTERAR uma lógica marcada com tag

1. **Atualize o comentário/tag** no mesmo commit da mudança de código. Comentário que sobra
   apontando pra lógica que sumiu é pior que comentário nenhum.
2. **Registre no [MELHORIAS.md](MELHORIAS.md)** (tabela de status / seção DSX). O `.py` guarda o
   "porquê curto"; o MELHORIAS guarda o histórico/decisão.
3. Se a mudança veio de uma **nova fonte** (novo export DSX, nova regra do analista, novo
   resultado do validador), troque/registre a tag e a data.

## Quando um `[PENDENTE]` for RESOLVIDO

1. Troque a tag `[PENDENTE]` pela fonte real (`[DSX]` / `[BACEN]` / `[ANALISTA]`).
2. Implemente (ou registre explicitamente que **não** se aplica — ex.: "lookup vazio na prática").
3. Atualize o índice do cabeçalho e o MELHORIAS. Remova o item da lista de pendências do README.

### `[PENDENTE]` em aberto hoje (2026-06-15)
- `cpf_tratamento`/`Cd_1` e `CPF(14)` calculados e **não emitidos** no `<Cli Cd>` (emite `Cd` cru).
- Pad do CPF para 11 posições dentro do IPOC de renegociação (ponto levantado pelo analista).
- `cpf_deletar_contrato` / `is_tp_0316` calculados e **nunca aplicados** (havia exclusão no DataStage?).
- `C83` / `I13` (regras de arquivo do validador ainda não implementadas) — ver MELHORIAS itens 3/4.

## Checklist antes de commitar uma mudança no `.py`

- [ ] As tags afetadas foram atualizadas (ou nenhuma foi tocada).
- [ ] `MELHORIAS.md` reflete a mudança (status/tabela).
- [ ] Se a regra mudou de fonte, a tag e a data foram ajustadas.
- [ ] `python -c "import ast; ast.parse(open('glue_ctcr_cadoc3040_optimized.py').read())"` passa.
- [ ] Se mudou lógica (não só comentário): `tests/smoke_test.py` atualizado e rodado em PySpark.

## Onde está o contexto profundo de cada regra

| Regra | Documento |
|-------|-----------|
| Mudanças DSX 09/06 → 15/06 (D1–D12) | [MELHORIAS.md](MELHORIAS.md) (seção "Mudanças DSX") + [README.md](README.md) item 8 |
| S81 / B01 / C83 / I13 (validador) | [MELHORIAS.md](MELHORIAS.md) itens 1–4 + README histórico |
| RunOff / CTA_DIA (downdircontas) | [MELHORIAS.md](MELHORIAS.md) + conversa 2026-06-12 (regras do analista) |
| Histórico completo de execução | [README.md](README.md) seção "Histórico de execução / bugs corrigidos" |
