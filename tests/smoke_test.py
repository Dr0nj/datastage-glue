# Smoke test das expressoes novas do job otimizado (sem Glue/spark-xml):
# valida extracao v{code}/first-Inf/first-Gar via funcoes de array, a regra
# "operacao baixada PicPay" (DSX 2026-06-15) e a montagem do XML 2026.
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

spark = SparkSession.builder.master('local[2]').appName('smoke').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

INF_SCHEMA = StructType([StructField(n, StringType()) for n in ['_Cd', '_Tp', '_Ident', '_Valor', '_Perc', '_Qtd']])
GAR_SCHEMA = StructType([StructField(n, StringType()) for n in ['_Tp', '_Ident', '_PercGar', '_VlrOrig', '_VlrData', '_DtReav']])
OP_SCHEMA = StructType([
    StructField('_Contrt', StringType()), StructField('_Mod', StringType()),
    StructField('Inf', ArrayType(INF_SCHEMA)), StructField('Gar', ArrayType(GAR_SCHEMA)),
])
CLI_SCHEMA = StructType([StructField('_Cd', StringType()), StructField('Op', ArrayType(OP_SCHEMA))])

data = [
    ('11122233344', [
        # Op 1: dois Inf (codes 20 e 330), primeiro Inf com _Ident nulo
        ('CT001', '1904',
         [(' 20', '0310', None, '100,50', None, None), ('330', '0316', 'ID-B', '7', '1', '2')],
         [('T1', 'G-1', '50', '10', '11', '20240101')]),
        # Op 2 duplicada (mesma chave CT001+1904, Tp_2 0310) -> deve marcar vRegDuplicado=1
        ('CT001', '1904', [('20', '0310', 'X', '5', None, None)], None),
        # Op 3 chave distinta, Tp_2 fora de 0310/0399 -> nunca duplicado
        ('CT002', '0201', [('40', '0299', 'Y', '9', None, None)], []),
    ]),
]
raw = spark.createDataFrame(data, CLI_SCHEMA)

cli = raw.select(F.col('_Cd').alias('Cd'), F.posexplode_outer('Op').alias('op_pos', 'op'))

def first_inf(field):
    return F.expr(f"try_element_at(filter(op.Inf, x -> x.{field} is not null), 1).{field}")

def inf_code(code):
    return F.expr(f"try_element_at(filter(op.Inf, x -> trim(x._Cd) = '{code}'), 1)._Valor")

op = cli.select(
    'Cd', 'op_pos',
    F.col('op._Contrt').alias('Contrt'), F.col('op._Mod').alias('Mod'),
    inf_code('20').alias('v20'), inf_code('330').alias('v330'), inf_code('40').alias('v40'),
    first_inf('_Tp').alias('Tp_2'), first_inf('_Ident').alias('Ident'),
    F.expr("try_element_at(filter(op.Gar, x -> x._Ident is not null), 1)._Ident").alias('Ident_2'),
)

# DSX 2026-06-15: a deduplicacao por (Contrt+Mod, Tp_2 in 0310/0399) SAIU do
# Trf_Filtra -- nenhum registro e mais marcado como duplicado (vRegDuplicado=0).
rows = {(r['Contrt'], r['op_pos']): r.asDict() for r in op.collect()}
op.show(truncate=False)

assert rows[('CT001', 0)]['v20'] == '100,50', 'v20 com _Cd=" 20" (trim) falhou'
assert rows[('CT001', 0)]['v330'] == '7'
assert rows[('CT001', 0)]['Ident'] == 'ID-B', 'first-non-null Ident deveria pular Inf[0] nulo'
assert rows[('CT001', 0)]['Ident_2'] == 'G-1'
assert rows[('CT002', 2)]['v40'] == '9' and rows[('CT002', 2)]['v20'] is None
assert rows[('CT001', 1)]['Ident_2'] is None, 'Gar nulo deve produzir null sem erro'

# ---------------------------------------------------------------------------
# Parte 1b: regra "operacao baixada PicPay" (Trf_Reneg, DSX 2026-06-15)
#   vOperBaixadaPipcay = 1 quando Tp_2='0316' e Qtd='2'. Quando VerificaReneg != 1
#   E baixada PicPay: zera Cd_2/Ident/Valor/Qtd e troca Tp_2 -> '0301'.
# ---------------------------------------------------------------------------
pip_data = [
    # (id, Tp_2, Qtd, VerificaReneg, Cd_2, Ident, Valor)
    ('aplica',     '0316', '2', None, 'CD-1', 'ID-1', '9'),  # baixada + nao reneg -> aplica
    ('reneg',      '0316', '2', '1',  'CD-2', 'ID-2', '9'),  # baixada mas renegociada -> NAO aplica
    ('qtd_difere', '0316', '1', None, 'CD-3', 'ID-3', '9'),  # Qtd != 2 -> nao e baixada
    ('tp_difere',  '0310', '2', None, 'CD-4', 'ID-4', '9'),  # Tp_2 != 0316 -> nao e baixada
]
pip_df = spark.createDataFrame(pip_data, ['id', 'Tp_2', 'Qtd', 'VerificaReneg', 'Cd_2', 'Ident', 'Valor'])
_oper_baixada = (F.trim(F.col('Tp_2')) == F.lit('0316')) & (F.trim(F.col('Qtd')) == F.lit('2'))
pip_df = pip_df.withColumn('vOperBaixadaPipcay', F.when(_oper_baixada, F.lit(1)).otherwise(F.lit(0)))
_aplica = (F.coalesce(F.col('VerificaReneg').cast('int'), F.lit(0)) != F.lit(1)) & (F.col('vOperBaixadaPipcay') == F.lit(1))
for _c in ['Cd_2', 'Ident', 'Valor', 'Qtd']:
    pip_df = pip_df.withColumn(_c, F.when(_aplica, F.lit(None).cast('string')).otherwise(F.col(_c)))
pip_df = pip_df.withColumn('Tp_2', F.when(_aplica, F.lit('0301')).otherwise(F.col('Tp_2')))
pip = {r['id']: r.asDict() for r in pip_df.collect()}

assert pip['aplica']['vOperBaixadaPipcay'] == 1
assert pip['aplica']['Tp_2'] == '0301', 'PicPay: Tp_2 deveria virar 0301'
assert pip['aplica']['Cd_2'] is None and pip['aplica']['Ident'] is None, 'PicPay: Cd_2/Ident deveriam zerar'
assert pip['aplica']['Valor'] is None and pip['aplica']['Qtd'] is None, 'PicPay: Valor/Qtd deveriam zerar'
assert pip['reneg']['Tp_2'] == '0316' and pip['reneg']['Cd_2'] == 'CD-2', 'PicPay: op renegociada nao deve ser alterada'
assert pip['qtd_difere']['vOperBaixadaPipcay'] == 0 and pip['qtd_difere']['Tp_2'] == '0316'
assert pip['tp_difere']['vOperBaixadaPipcay'] == 0 and pip['tp_difere']['Tp_2'] == '0310'

# ---------------------------------------------------------------------------
# Parte 1c: IPOC de renegociacao (Trf_Reneg, DSX 2026-06-15)
#   So reneg (VerificaReneg=1): '0951641902991' + (Cd se Tp='1' senao DetCli[1..8])
#   + NUM_OPER_RENEG_PCELD com 15 posicoes (zeros a esquerda). Senao mantem o IPOC.
# ---------------------------------------------------------------------------
ipoc_data = [
    # (id, VerificaReneg, Tp, Cd, DetCli, NUM_OPER_RENEG_PCELD, IPOC)
    ('reneg_pf',  '1', '1', '12345678901', 'DET12345', '987654321', 'IPOC-ORIG'),  # Tp=1 -> usa Cd
    ('reneg_pj',  '1', '2', 'ZZZ',         'DETCLI99', '55',        'IPOC-ORIG'),  # Tp!=1 -> usa DetCli[1..8]
    ('nao_reneg', '0', '1', '12345678901', 'DET12345', '987654321', 'IPOC-ORIG'),  # nao reneg -> mantem
]
ipoc_df = spark.createDataFrame(ipoc_data, ['id', 'VerificaReneg', 'Tp', 'Cd', 'DetCli', 'NUM_OPER_RENEG_PCELD', 'IPOC'])
_v_cnpjcpf = F.when(F.trim(F.col('Tp')) == F.lit('1'), F.col('Cd')).otherwise(F.substring(F.col('DetCli'), 1, 8))
_v_contrato = F.expr("right(concat('000000000000000', trim(NUM_OPER_RENEG_PCELD)), 15)")
ipoc_df = ipoc_df.withColumn(
    'IPOC',
    F.when(
        F.coalesce(F.col('VerificaReneg').cast('int'), F.lit(0)) == F.lit(1),
        F.concat(F.lit('0951641902991'), F.trim(_v_cnpjcpf), _v_contrato),
    ).otherwise(F.col('IPOC')),
)
ipoc = {r['id']: r['IPOC'] for r in ipoc_df.collect()}

assert ipoc['reneg_pf'] == '0951641902991' + '12345678901' + '000000987654321', ipoc['reneg_pf']
assert ipoc['reneg_pj'] == '0951641902991' + 'DETCLI99' + '000000000000055', ipoc['reneg_pj']
assert ipoc['nao_reneg'] == 'IPOC-ORIG', 'op nao renegociada deve manter o IPOC de origem'

# ---------------------------------------------------------------------------
# Parte 1d: replicacao da query CTRL_DIVDA_RENEG sobre o dump cru (DSX 15/06)
#   WHERE IND_RENEG > 1 AND NUM_ORGNZ = 212 AND HOR_ATULZ > current_date - 65
#   VerificaReneg = '1' (derivado, nao e coluna fisica); NUM_OPER_RENEG_PCELD = lpad(NUM_CONTR,15,'0')
# ---------------------------------------------------------------------------
from datetime import date, timedelta
_recent = (date.today() - timedelta(days=10)).strftime('%Y-%m-%d')
_old = '2000-01-01'
reneg_raw_data = [
    # (id, NUM_OPER, IND_RENEG, NUM_ORGNZ, HOR_ATULZ, NUM_CONTR)
    ('ok',       'OP-A', '2', '212', _recent, '12345'),  # passa em tudo
    ('ind1',     'OP-B', '1', '212', _recent, '67'),     # IND_RENEG nao > 1
    ('org',      'OP-C', '2', '999', _recent, '89'),     # NUM_ORGNZ != 212
    ('hor_old',  'OP-D', '2', '212', _old,    '111'),    # HOR_ATULZ fora da janela
]
reneg_raw = spark.createDataFrame(reneg_raw_data, ['id', 'NUM_OPER', 'IND_RENEG', 'NUM_ORGNZ', 'HOR_ATULZ', 'NUM_CONTR'])
_hor = F.coalesce(F.to_date(F.col('HOR_ATULZ')), F.to_date(F.col('HOR_ATULZ'), 'yyyy-MM-dd'))
_where = (
    (F.col('IND_RENEG').cast('double') > F.lit(1))
    & (F.col('NUM_ORGNZ').cast('double') == F.lit(212))
    & (_hor > F.date_sub(F.current_date(), 65))
)
reneg_lookup = reneg_raw.filter(_where).select(
    F.col('id'),
    F.lpad(F.trim(F.col('NUM_CONTR')), 15, '0').alias('NUM_OPER_RENEG_PCELD'),
    F.lit('1').alias('VerificaReneg'),
)
reneg = {r['id']: r.asDict() for r in reneg_lookup.collect()}
assert set(reneg.keys()) == {'ok'}, f'so a linha valida deveria sobreviver: {set(reneg.keys())}'
assert reneg['ok']['VerificaReneg'] == '1', 'VerificaReneg derivado deve ser 1 para linha valida'
assert reneg['ok']['NUM_OPER_RENEG_PCELD'] == '000000000012345', reneg['ok']['NUM_OPER_RENEG_PCELD']

# ---------------------------------------------------------------------------
# Parte 2: montagem do XML final (layout Codoc3040_2026.xsd)
# Replica _xml_attr/_optional_xml_element/_build_xml_fragments do job:
# - um <Cli> por cliente com as <Op> ordenadas por op_pos
# - elementos opcionais (Inf/Venc/Gar/ContInstFinRes4966/Estagio) e atributos
#   vazios sao omitidos
# ---------------------------------------------------------------------------

def _xml_escape(col):
    value = F.coalesce(col.cast('string'), F.lit(''))
    for src, dst in [('&', '&amp;'), ('<', '&lt;'), ('>', '&gt;'), ('"', '&quot;'), ("'", '&apos;')]:
        value = F.regexp_replace(value, src, dst)
    return value

def _xml_attr(attr_name, col_name):
    expr = F.col(col_name) if isinstance(col_name, str) else col_name
    return F.when(expr.isNull() | (F.length(F.trim(expr.cast('string'))) == 0), F.lit('')).otherwise(
        F.concat(F.lit(f' {attr_name}="'), _xml_escape(expr), F.lit('"'))
    )

def _optional_xml_element(tag, attrs_col, children_col=None, prefix='', close_prefix=''):
    has_content = F.length(attrs_col) > 0
    if children_col is not None:
        has_content = has_content | (F.length(children_col) > 0)
        closing_indent = F.when(F.length(children_col) > 0, F.lit(close_prefix)).otherwise(F.lit(''))
        body = F.concat(F.lit(f'{prefix}<{tag}'), attrs_col, F.lit('>'), children_col, closing_indent, F.lit(f'</{tag}>'))
    else:
        body = F.concat(F.lit(f'{prefix}<{tag}'), attrs_col, F.lit('/>'))
    return F.when(has_content, body).otherwise(F.lit(''))

frag_data = [
    # (dt_data, Cd, Tp, Autorzc, PorteCli, IniRelactCli, FatAnual, ClassCli, TpCtrlNorm, CongEcon,
    #  op_pos, IPOC, Contrt, Mod, VlrContr, v110, v120, ClasAtFin, EstInstFin, VlrContBr4966,
    #  EstagioMotivo, EstagioDtAlocacao, Tp_2, Tp_3)
    # Cliente A, op 2 chega "antes" no df mas op_pos ordena; op 0 tem 4966+Estagio e Venc;
    # op 1 tem Venc E Inf (valida a ordem oficial Venc->Inf exigida pelo validador Bacen/B01).
    ('20260427', '00021017344', '1', 'N', '1', '2021-11-17', '0.01', None, None, None,
     1, 'IPOC-B', 'CT-B', '0204', '15.24', '5.00', None, None, None, None, None, None, '0301', None),
    ('20260427', '00021017344', '1', 'N', '1', '2021-11-17', '0.01', None, None, None,
     0, 'IPOC-A', 'CT-A', '1304', '100.00', None, '50.00', '1', '1', '50.00', '101', '2026-04', None, None),
    # Cliente B, 1 op com Gar e 4966 sem Estagio.
    ('20260427', '00099', '2', None, None, None, None, None, None, None,
     0, 'IPOC-C', 'CT-C', '1904', None, '38.31', None, '1', '1', '38.31', None, None, None, 'G1'),
]
frag_cols = ['dt_data', 'Cd', 'Tp', 'Autorzc', 'PorteCli', 'IniRelactCli', 'FatAnual', 'ClassCli',
             'TpCtrlNorm', 'CongEcon', 'op_pos', 'IPOC', 'Contrt', 'Mod', 'VlrContr', 'v110', 'v120',
             'ClasAtFin', 'EstInstFin', 'VlrContBr4966', 'EstagioMotivo', 'EstagioDtAlocacao', 'Tp_2', 'Tp_3']
from pyspark.sql.types import IntegerType
FRAG_SCHEMA = StructType([
    StructField(name, IntegerType() if name == 'op_pos' else StringType(), True) for name in frag_cols
])
frag_df = spark.createDataFrame(frag_data, FRAG_SCHEMA)

inf_attrs = F.concat(_xml_attr('Tp', 'Tp_2'))
venc_attrs = F.concat(_xml_attr('v110', 'v110'), _xml_attr('v120', 'v120'))
gar_attrs = F.concat(_xml_attr('Tp', 'Tp_3'))
estagio_attrs = F.concat(_xml_attr('Motivo', 'EstagioMotivo'), _xml_attr('DtAlocacao', 'EstagioDtAlocacao'))
cont_attrs = F.concat(_xml_attr('ClasAtFin', 'ClasAtFin'), _xml_attr('EstInstFin', 'EstInstFin'), _xml_attr('VlrContBr', 'VlrContBr4966'))

# Ordem oficial Bacen (SCR3040_Leiaute.xls / validador Release 13657): Venc, Gar, Inf, 4966.
op_children = F.concat(
    _optional_xml_element('Venc', venc_attrs, prefix='\n  '),
    _optional_xml_element('Gar', gar_attrs, prefix='\n  '),
    _optional_xml_element('Inf', inf_attrs, prefix='\n  '),
    _optional_xml_element(
        'ContInstFinRes4966', cont_attrs,
        _optional_xml_element('Estagio', estagio_attrs, prefix='\n   '),
        prefix='\n  ', close_prefix='\n  ',
    ),
)
op_xml = F.concat(
    F.lit('\n <Op'),
    _xml_attr('IPOC', 'IPOC'), _xml_attr('Contrt', 'Contrt'), _xml_attr('Mod', 'Mod'), _xml_attr('VlrContr', 'VlrContr'),
    F.lit('>'),
    op_children,
    F.when(F.length(op_children) > 0, F.lit('\n </Op>')).otherwise(F.lit('</Op>')),
)

cli_keys = ['dt_data', 'Cd', 'Tp', 'Autorzc', 'PorteCli', 'IniRelactCli', 'FatAnual', 'ClassCli', 'TpCtrlNorm', 'CongEcon']
grouped = frag_df.withColumn('op_xml', op_xml).groupBy(*cli_keys).agg(
    F.array_sort(F.collect_list(F.struct(F.col('op_pos').alias('pos'), F.col('op_xml').alias('xml')))).alias('ops_sorted')
)
cli_attrs = F.concat(
    _xml_attr('Cd', 'Cd'), _xml_attr('Tp', 'Tp'), _xml_attr('Autorzc', 'Autorzc'),
    _xml_attr('PorteCli', 'PorteCli'), _xml_attr('IniRelactCli', 'IniRelactCli'),
    _xml_attr('FatAnual', 'FatAnual'), _xml_attr('ClassCli', 'ClassCli'),
    _xml_attr('TpCtrl', 'TpCtrlNorm'), _xml_attr('CongEcon', 'CongEcon'),
)
frag_out = grouped.withColumn(
    'value',
    F.concat(F.lit('<Cli'), cli_attrs, F.lit('>'),
             F.concat_ws('', F.expr('transform(ops_sorted, o -> o.xml)')),
             F.lit('\n</Cli>')),
)
frags = {r['Cd']: r['value'] for r in frag_out.collect()}
print(frags['00021017344'])
print(frags['00099'])

cli_a = frags['00021017344']
expected_a = (
    '<Cli Cd="00021017344" Tp="1" Autorzc="N" PorteCli="1" IniRelactCli="2021-11-17" FatAnual="0.01">'
    '\n <Op IPOC="IPOC-A" Contrt="CT-A" Mod="1304" VlrContr="100.00">'
    '\n  <Venc v120="50.00"/>'
    '\n  <ContInstFinRes4966 ClasAtFin="1" EstInstFin="1" VlrContBr="50.00">'
    '\n   <Estagio Motivo="101" DtAlocacao="2026-04"/>'
    '\n  </ContInstFinRes4966>'
    '\n </Op>'
    '\n <Op IPOC="IPOC-B" Contrt="CT-B" Mod="0204" VlrContr="15.24">'
    '\n  <Venc v110="5.00"/>'
    '\n  <Inf Tp="0301"/>'
    '\n </Op>'
    '\n</Cli>'
)
assert cli_a == expected_a, f'Cli agrupado divergente:\n{cli_a}\n!=\n{expected_a}'

cli_b = frags['00099']
expected_b = (
    '<Cli Cd="00099" Tp="2">'
    '\n <Op IPOC="IPOC-C" Contrt="CT-C" Mod="1904">'
    '\n  <Venc v110="38.31"/>'
    '\n  <Gar Tp="G1"/>'
    '\n  <ContInstFinRes4966 ClasAtFin="1" EstInstFin="1" VlrContBr="38.31"></ContInstFinRes4966>'
    '\n </Op>'
    '\n</Cli>'
)
assert cli_b == expected_b, f'Cli B divergente:\n{cli_b}\n!=\n{expected_b}'

assert len(frags) == 2, 'TotalCli deve contar clientes (2), nao Ops (3)'
print('SMOKE_TEST_OK')
spark.stop()
