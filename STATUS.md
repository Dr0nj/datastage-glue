# STATUS — CADOC 3040 Glue/PySpark (pós-validação contra o DSX)

> Snapshot de orientação para retomar o trabalho. Atualizado em 2026-06-16, depois de
> validar a migração contra o export `.dsx` do `cadoc3040_ctcr` (2026-06-15).
> Detalhe das pendências: [MELHORIAS.md](MELHORIAS.md) · Histórico completo:
> [README_HISTORICO.md](README_HISTORICO.md) · Checklist de homologação:
> [README_otimizacao_cadoc3040.md](README_otimizacao_cadoc3040.md).

## Onde estamos
Pipeline estruturalmente pronto: performance resolvida (leitura splittable + checkpoint),
e S81 / B01 / RUNOFF / CTA_DIA passando no validador local. Falta: **1 bug confirmado de
IPOC**, homologação dos números vs DataStage, e **3 decisões de negócio**.

---

## 🔴 BUG CONFIRMADO (validado contra o DSX): IPOC de renegociação no nó XML errado

O Glue grava o `vIPOC` de renegociação no atributo **`<Op IPOC>`** e deixa **`<Inf Cd>`** cru.
O DataStage faz o **inverso**.

**Comportamento correto (DSX `Pj_CTCR_0005`, stage `Trf_Reneg`):**
- Coluna **`Cd_2`** (`Description "/Doc3040/Cli/Op/Inf/@Cd"` = `<Inf Cd>`) recebe o `vIPOC`
  quando `VerificaReneg='1'`.
- Coluna **`ipoc`** (`Description "./ns0:@IPOC"` = `<Op IPOC>`) passa `Lnk_Srt_All.ipoc`
  direto — **o `<Op IPOC>` original é PRESERVADO**.
- Na renegociação, o `<Inf>` é reescrito **inteiro**, não só o `Cd`:

  | Atributo `<Inf>` | Valor na reneg | Linha DSX |
  |---|---|---|
  | `Cd`    | `vIPOC` | 2994 |
  | `Ident` | `'0299'` | 3031 |
  | `Tp`    | `'0301'` (ramo baixada) | 3067 |
  | `Valor` | `VAL_RENEG_OPER_CATAO` | 3104 |
  | `Qtd`   | `'2'` | 3174 |

- Derivation de `Cd_2` = **3 ramos** (linhas 2983-2994):
  ```
  IF vVerificaReneg=0 AND vOperBaixadaPipcay=1   -> Setnull()
  ELSE IF VerificaReneg='1' AND vValidaTipo=1    -> Setnull()
  ELSE IF VerificaReneg='1'                      -> vIPOC
  ELSE                                            -> Lnk_Srt_All.Cd_2 (original)
  ```

**Pontos de atenção ao corrigir no Glue (`glue_ctcr_cadoc3040_optimized.py`):**
1. Mover o `vIPOC` da coluna `IPOC` para `Cd_2`; `IPOC` volta a ser só passagem da origem.
2. Implementar também `Ident`/`Valor`/`Qtd` da reneg (hoje faltam).
3. Localizar a StageVar **`vValidaTipo`** no DSX e reproduzir o ramo 2 (sem ela, fica incompleto).
4. **Substituir** (não empilhar) o zeramento PicPay atual pela derivation unificada de 3 ramos.
5. **Manter separado** o rewrite do CNPJ8 no PPBANK (`Trf_Filtra`, `InterVar0_0="09516419"`,
   incide sobre `<Op IPOC>`) — o S81 continua intacto.
6. Atualizar o **smoke test (Parte 1c)**, que hoje valida o comportamento errado.

---

## ✅ Confirmado CORRETO no Glue (sem ação)
- Fórmula `vIPOC = '0951641902991' + trim(vCnpjCpf) + vContratoNew`.
- `vCnpjCpf = Cd` (PF, `Tp='1'`) / `DetCli[1..8]` (PJ) — **sem pad para 11** (o DSX também não pada).
- Contrato `right('0'×15 + trim(NUM_OPER_RENEG_PCELD), 15)`; `NUM_OPER_RENEG_PCELD = lpad(NUM_CONTR,15,'0')`.
- `VerificaReneg='1'` derivado do match (alias `'1'` na query Oracle), não coluna física.
- Rewrite antigo `928949220210` no `Cd_2`: removido no export 15/06 (Glue já sem ele).

## ✅ Split PPBANK × BASE_RUN_OFF = comportamento legítimo (não é escrita perdida)
O DataStage racha por **operação** (`FiltroTabela = FLAG` do lookup `BASE_RUN_OFF`, `Trf_Filtra`):
`FiltroTabela=1 -> BASE_RUN_OFF`, `=0 -> PPBANK`. Um mesmo cliente **pode** ter Ops divididas
entre os dois arquivos. As operações `Tp 0316` (baixadas) seguem o mesmo split.
**Implicação:** "clientes ausentes do PPBANK" estão no `CADOC3040_xml_*.xml` (BASE_RUN_OFF).
A homologação deve comparar **(PPBANK + BASE_RUN_OFF) novos vs (principal + run-off) antigos**.

---

## ❓ Decisões de negócio em aberto (travam a homologação regulatória)
- **C83** — Op com `EstInstFin` sem `<Estagio>` e sem Inf 03xx: preencher de outra origem,
  suprimir o EstInstFin, ou corrigir upstream?
- **I13** — cliente com soma de vencimentos < R$ 200 sem saída 03xx: excluir? a regra é por
  ARQUIVO ou por TOTAL? (o split por Op pode derrubar a soma por arquivo).
- **Volume RunOff esperado** — confirmar com o negócio se o volume do BASE_RUN_OFF bate.

(detalhe e os demais itens de robustez/observabilidade em [MELHORIAS.md](MELHORIAS.md))

---

## Próximos passos (ordem sugerida)
1. Aplicar o fix do IPOC (seção 🔴) + atualizar o smoke test.
2. Rodar o job e conferir no log a linha `reneg: Ops com match no ctrl_divda_reneg (VerificaReneg=1) = N`
   (esperado da ordem de ~262 mil; se vier ~20, o join do dump `CTRL_DIVDA_RENEG` está quebrado —
   conferir nomes de coluna do dump e formato de `HOR_ATULZ`).
3. Homologar números vs DataStage unindo **PPBANK + BASE_RUN_OFF** (soma_modalidade, contagens, diff de `<Cli>`).
4. Pré-validar com [tools/validate_cadoc3040.ps1](tools/validate_cadoc3040.ps1) e revalidar no validador oficial Bacen.
5. Fechar **C83 / I13 / volume RunOff** com o negócio.
