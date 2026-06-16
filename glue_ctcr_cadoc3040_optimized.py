# -*- coding: utf-8 -*-
"""
CADOC 3040 (Bacen/SCR) - Migracao DataStage -> Glue/PySpark (S3-only, sem Oracle).

VERSAO OTIMIZADA. Correcoes em relacao ao script original (glue_ctcr_cadoc3040_full.py):

  1. LEITURA SPLITTABLE: o leitor XML nativo do Glue (create_dynamic_frame format='xml')
     nao e splittable -> 1 task parseava os 20GB sozinho ("nonSplittable: true",
     "submitting 1 missing task", executores em polling). Substituido por spark-xml
     (com.databricks:spark-xml) com rowTag=Cli, que divide XML NAO comprimido em
     splits Hadoop -> ~160-640 tasks paralelas para 20GB.
  2. SCHEMA EXPLICITO: evita a inferencia de schema do spark-xml, que faria um
     scan completo extra dos 20GB.
  3. REMOVIDO Relationalize (era computado e nunca usado = 1 pass completo extra),
     ResolveChoice e DropNullFields no caminho dos 20GB.
  4. REMOVIDO monotonically_increasing_id como chave de join. Alem do custo
     (explode Inf -> pivot -> 3 joins de volta, com shuffles gigantes), o ID nao e
     estavel entre recomputacoes -> joins potencialmente ERRADOS. Substituido por
     funcoes de array (filter/try_element_at) direto na struct Op: zero shuffle,
     zero join, deterministico.
  5. WINDOW DE DEDUP: Window.orderBy global sem partitionBy puxava o dataset
     inteiro para 1 unica particao. Substituido por partitionBy(Contrt, Mod).
  6. CHECKPOINT: o dataframe de negocio final e materializado UMA vez em parquet
     (work/normalized) e relido; todos os ramos de saida (7 parquets, fragments
     XML, counts, xlsx) leem do parquet. O XML de 20GB e parseado exatamente 1 vez
     (antes: ~12 actions x reparse completo).
  7. Counts de TotalCli via groupBy().count() unico, em vez de .count() por dt
     re-disparando o pipeline.
  8. LAYOUT 2026 (Codoc3040_2026.xsd + amostra real DataStage):
     - <Cli> agrupado: um <Cli> por cliente com TODAS as suas <Op> aninhadas
       (antes: um <Cli> por Op, com Cd repetido).
     - Atributo _IPOC lido do Op e emitido (antes era descartado pelo schema).
     - Elemento <Venc v20..v330> lido (fonte preferencial dos v-codes, com
       fallback para o pivot de <Inf Cd=...>) e emitido na saida.
     - Elemento <ContInstFinRes4966> + filho <Estagio> lidos e emitidos.
     - Atributos IniRelactCli/CongEcon (Cli) e DetCli (Op) emitidos.
     - Elementos opcionais (Inf/Venc/Gar/ContInstFinRes4966/Estagio) e atributos
       sem valor sao OMITIDOS (nao saem vazios).
     - Header Doc3040 preserva MetodApPE/MetodDifTJE.
     - TotalCli = numero de clientes (blocos <Cli>) por arquivo, nao de Ops.

REQUISITOS DE EXECUCAO (Glue 5.0 / Spark 3.5 / Scala 2.12):
  --extra-jars com (baixar do Maven Central e subir no S3):
      com.databricks:spark-xml_2.12:0.18.0
      org.glassfish.jaxb:txw2:3.0.2
      org.apache.ws.xmlschema:xmlschema-core:2.3.0
  Workers sugeridos: 10-20 x G.2X. Entrada deve permanecer .xml SEM compressao
  (gzip tornaria o arquivo nao-splittable de novo).

Parametros obrigatorios: --JOB_NAME --S3_INPUT_PATH --S3_OUTPUT_PATH
Opcionais (iguais ao original): --FINAL_SINGLE_FILE --GENERATE_SUPPLIER_FILES
  --KEEP_WORK_PARQUET --KEEP_AUDIT --P_CNPJ --P_NOMERESP --P_EMAILRESP --P_TELRESP
  --FINAL_XML_PPBANK_NAME --FINAL_XML_BASE_RUN_OFF_NAME --DOWNDIRCONTAS_PATH
"""

# =============================================================================
# REGRAS DE NEGOCIO (indice) -- LEIA antes de alterar qualquer logica
# =============================================================================
# Este .py e o UNICO artefato que sobe no Glue. O contexto profundo (historico,
# decisoes, validacoes) vive FORA dele, em:
#     projects/Glue-datastage/MELHORIAS.md   (fonte unica de status/regras)
#     projects/Glue-datastage/README.md      (historico detalhado)
# Aqui ficam so o "porque + fonte" das regras nao-obvias. Tags de proveniencia:
#     [DSX]      derivacao do DataStage (export 2026-06-15, job cadoc3040_ctcr)
#     [BACEN]    exigencia do validador oficial (Release 13657) / leiaute SCR3040
#     [ANALISTA] regra confirmada pelo analista de negocio
#     [PENDENTE] NAO confirmado / suspeita -- NAO tratar como regra estabelecida
#
# MANUTENCAO: ao mudar uma logica marcada com tag, ATUALIZE o comentario/tag aqui
# E registre a mudanca no MELHORIAS.md. Guia de manutencao dos comentarios:
#     projects/Glue-datastage/MANUTENCAO-COMENTARIOS.md
#
# Mapa das regras (o detalhe esta inline, no ponto de uso):
#   * Leitura splittable do XML ~20GB (spark-xml rowTag=Cli) ......... performance
#   * [BACEN] S81 : CNPJ do header == CNPJ8 do IPOC no PPBANK (PPBANK_CNPJ8)
#   * [BACEN] B01 : ordem dos filhos de <Op> = Venc, Gar, Inf, ContInstFinRes4966
#   * [DSX] Reneg : query CTRL_DIVDA_RENEG -> WHERE IND_RENEG>1 AND NUM_ORGNZ=212
#                   AND HOR_ATULZ>SYSDATE-65; VerificaReneg='1' DERIVADO (nao e coluna)
#   * [DSX] PicPay: Tp_2='0316' & Qtd='2' & nao-reneg -> Tp_2='0301' e zera Cd_2/Ident/Valor/Qtd
#   * [ANALISTA] IPOC reneg: '0951641902991' (= cnpj8 09516419 + 0299 + 1) + cpf(11) + contrato(15)
#   * [ANALISTA] RunOff/CTA_DIA: downdircontas pos 592='S' (RunOff); conta pos 7 tam 19;
#                exclusao por status pos 83/84 ou atraso pos 171 tam 5; match conta vs Contrt[1..19]
#   * [DSX] Cd_2 : passa direto (rewrite '0299' REMOVIDO no DSX 15/06)
#   * [DSX] dedup vRegDuplicado REMOVIDA no DSX 15/06 (ramos usam so FiltroTabela)
#   * Daily imutavel: instances congeladas; TotalCli = nº de blocos <Cli> por arquivo
#   * [PENDENTE] cpf_tratamento/Cd_1 e CPF(14) calculados e NAO emitidos no XML;
#                pad CPF p/ 11 no IPOC; cpf_deletar_contrato/is_tp_0316 calculados e nunca aplicados;
#                C83/I13 (regras de arquivo do validador ainda nao implementadas)
# =============================================================================

import io
import re
import sys
import zipfile
from urllib.parse import urlparse

import boto3

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import ArrayType, DecimalType, StringType, StructField, StructType

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_OUTPUT_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Garante splits razoaveis (>=64MB) ao ler o XML unico via spark-xml/Hadoop.
sc._jsc.hadoopConfiguration().set('mapreduce.input.fileinputformat.split.minsize', str(64 * 1024 * 1024))


def _optional_arg(name, default_value):
    token = f'--{name}'
    if token in sys.argv:
        index = sys.argv.index(token)
        if index + 1 < len(sys.argv):
            return sys.argv[index + 1]
    return default_value


# =============================================
# Configuracao / caminhos
# =============================================

S3_INPUT_ROOT = args['S3_INPUT_PATH'].rstrip('/')
S3_OUTPUT_ROOT = args['S3_OUTPUT_PATH'].rstrip('/')
FINAL_SINGLE_FILE = _optional_arg('FINAL_SINGLE_FILE', 'false').lower() in {'1', 'true', 'yes', 'y'}
GENERATE_SUPPLIER_FILES = _optional_arg('GENERATE_SUPPLIER_FILES', 'true').lower() in {'1', 'true', 'yes', 'y'}
KEEP_WORK_PARQUET = _optional_arg('KEEP_WORK_PARQUET', 'true').lower() in {'1', 'true', 'yes', 'y'}
# KEEP_AUDIT preserva aggregates/soma_modalidade e audit/cd2_alterados (evidencia de
# homologacao vs DataStage; sao pequenos). Default = manter mesmo com KEEP_WORK_PARQUET=false.
KEEP_AUDIT = _optional_arg('KEEP_AUDIT', 'true').lower() in {'1', 'true', 'yes', 'y'}
PPBANK_HEADER_CNPJ = _optional_arg('P_CNPJ', '')
# [BACEN] Regra S81 do validador: o CNPJ do header <Doc3040> e os 8 primeiros
# digitos do IPOC de TODAS as Op precisam ser o MESMO valor. No arquivo PPBANK
# usamos um unico CNPJ8 nos dois lugares: o --P_CNPJ se informado, senao o
# default historico 09516419 (mesmo prefixo que o DataStage aplicava ao IPOC).
PPBANK_CNPJ8 = (PPBANK_HEADER_CNPJ.strip() or '09516419')[:8]
PPBANK_HEADER_NOME_RESP = _optional_arg('P_NOMERESP', '')
PPBANK_HEADER_EMAIL_RESP = _optional_arg('P_EMAILRESP', '')
PPBANK_HEADER_TEL_RESP = _optional_arg('P_TELRESP', '')
FINAL_XML_PPBANK_NAME = _optional_arg('FINAL_XML_PPBANK_NAME', '')
FINAL_XML_BASE_RUN_OFF_NAME = _optional_arg('FINAL_XML_BASE_RUN_OFF_NAME', '')
DOWNDIRCONTAS_PATH_ARG = _optional_arg('DOWNDIRCONTAS_PATH', '')

s3_client = boto3.client('s3')

INFO_CODES = [
    '20', '40', '60', '80', '110', '120', '130', '140', '150', '160', '165',
    '170', '175', '180', '190', '199', '205', '210', '220', '230', '240',
    '245', '250', '255', '260', '270', '280', '290', '310', '320', '330'
]
VALUE_COLUMNS = ['VlrContr', 'VlrContBr'] + [f'v{code}' for code in INFO_CODES]


def _s3_parent(path):
    parsed = urlparse(path)
    if parsed.scheme != 's3':
        return path.rstrip('/').rsplit('/', 1)[0]
    key = parsed.path.strip('/')
    parent_key = key.rsplit('/', 1)[0] if '/' in key else ''
    if parent_key:
        return f's3://{parsed.netloc}/{parent_key}'
    return f's3://{parsed.netloc}'


def _path_join(*parts):
    clean = []
    for index, part in enumerate(parts):
        if index == 0:
            clean.append(part.rstrip('/'))
        else:
            clean.append(part.strip('/'))
    return '/'.join(clean)


def _looks_like_xml_path(path):
    lower = path.lower()
    return lower.endswith('.xml') or lower.endswith('.xml.gz') or '*' in lower


if _looks_like_xml_path(S3_INPUT_ROOT):
    XML_INPUT_PATH = S3_INPUT_ROOT
    LOOKUP_ROOT = _path_join(_s3_parent(S3_INPUT_ROOT), 'lookups')
else:
    XML_INPUT_PATH = _path_join(S3_INPUT_ROOT, 'xml')
    LOOKUP_ROOT = _path_join(S3_INPUT_ROOT, 'lookups')

DOWNDIRCONTAS_PATH = DOWNDIRCONTAS_PATH_ARG or _path_join(LOOKUP_ROOT, 'downdircontas')

NORMALIZED_PARQUET_PATH = _path_join(S3_OUTPUT_ROOT, 'work/normalized')


def _log_step(message):
    print(f'[CADOC3040] {message}', flush=True)


_log_step(f'job initialized input_root={S3_INPUT_ROOT} output_root={S3_OUTPUT_ROOT}')
_log_step(f'paths xml_input={XML_INPUT_PATH} lookup_root={LOOKUP_ROOT} downdircontas={DOWNDIRCONTAS_PATH}')


def _optional_path(name):
    return _path_join(LOOKUP_ROOT, name)


# =============================================
# Helpers S3 (driver) - identicos ao original
# =============================================

def _parse_s3_uri(uri):
    parsed = urlparse(uri)
    if parsed.scheme != 's3' or not parsed.netloc:
        raise ValueError(f'URI S3 invalida: {uri}')
    return parsed.netloc, parsed.path.lstrip('/')


def _list_s3_keys(uri):
    bucket, prefix = _parse_s3_uri(uri.rstrip('/') + '/')
    paginator = s3_client.get_paginator('list_objects_v2')
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get('Contents', []):
            key = item['Key']
            name = key.rsplit('/', 1)[-1]
            if name.startswith('part-') and not key.endswith('/'):
                keys.append(key)
    return bucket, sorted(keys)


def _upload_bytes_part(bucket, key, upload_id, part_number, payload, parts):
    if not payload:
        return part_number
    response = s3_client.upload_part(
        Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=part_number, Body=bytes(payload),
    )
    parts.append({'ETag': response['ETag'], 'PartNumber': part_number})
    return part_number + 1


def _assemble_s3_text_objects(source_uri, target_uri, header='', footer='', min_part_size=8 * 1024 * 1024):
    """Concatena os part-files distribuidos em um unico objeto S3 (multipart upload)."""
    source_bucket, source_keys = _list_s3_keys(source_uri)
    target_bucket, target_key = _parse_s3_uri(target_uri)
    _log_step(f'assembling text objects source={source_uri} parts={len(source_keys)} target={target_uri}')
    upload = s3_client.create_multipart_upload(Bucket=target_bucket, Key=target_key, ContentType='application/octet-stream')
    upload_id = upload['UploadId']
    parts = []
    part_number = 1
    buffer = bytearray()

    try:
        if header:
            buffer.extend(header.encode('utf-8'))

        for source_key in source_keys:
            body = s3_client.get_object(Bucket=source_bucket, Key=source_key)['Body']
            for chunk in iter(lambda: body.read(1024 * 1024), b''):
                buffer.extend(chunk)
                if len(buffer) >= min_part_size:
                    part_number = _upload_bytes_part(target_bucket, target_key, upload_id, part_number, buffer, parts)
                    buffer = bytearray()
            if buffer and not buffer.endswith(b'\n'):
                buffer.extend(b'\n')

        if footer:
            buffer.extend(footer.encode('utf-8'))

        part_number = _upload_bytes_part(target_bucket, target_key, upload_id, part_number, buffer, parts)
        s3_client.complete_multipart_upload(
            Bucket=target_bucket, Key=target_key, UploadId=upload_id, MultipartUpload={'Parts': parts},
        )
        _log_step(f'finished assembly target={target_uri}')
    except Exception:
        _log_step(f'assembly failed target={target_uri}; aborting multipart upload')
        s3_client.abort_multipart_upload(Bucket=target_bucket, Key=target_key, UploadId=upload_id)
        raise


def _delete_s3_prefix(uri):
    bucket, prefix = _parse_s3_uri(uri.rstrip('/') + '/')
    _log_step(f'deleting s3 prefix {uri}')
    paginator = s3_client.get_paginator('list_objects_v2')
    batch = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get('Contents', []):
            batch.append({'Key': item['Key']})
            if len(batch) == 1000:
                s3_client.delete_objects(Bucket=bucket, Delete={'Objects': batch})
                batch = []
    if batch:
        s3_client.delete_objects(Bucket=bucket, Delete={'Objects': batch})
    _log_step(f'finished deleting s3 prefix {uri}')


def _read_doc3040_header_attrs():
    """Le apenas o 1o MB do XML (Range GET) para recuperar atributos do root Doc3040."""
    _log_step('reading Doc3040 header attributes')
    try:
        if XML_INPUT_PATH.lower().endswith('.xml'):
            bucket, key = _parse_s3_uri(XML_INPUT_PATH)
        else:
            bucket, prefix = _parse_s3_uri(XML_INPUT_PATH.rstrip('/') + '/')
            response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=50)
            xml_keys = [item['Key'] for item in response.get('Contents', []) if item['Key'].lower().endswith('.xml')]
            if not xml_keys:
                return {}
            key = sorted(xml_keys)[0]
        response = s3_client.get_object(Bucket=bucket, Key=key, Range='bytes=0-1048575')
        sample = response['Body'].read().decode('utf-8', errors='ignore')
        match = re.search(r'<Doc3040\s+([^>]+)>', sample)
        if not match:
            return {}
        attrs = {}
        for attr, value in re.findall(r'(\w+)="([^"]*)"', match.group(1)):
            attrs[attr] = value
        _log_step(f'Doc3040 header attributes loaded keys={sorted(attrs.keys())}')
        return attrs
    except Exception as exc:
        _log_step(f'WARN: nao foi possivel ler header Doc3040 do XML: {exc}')
        return {}


def _xml_attr_from_dict(attrs, key):
    value = attrs.get(key, '')
    if value is None or str(value).strip() == '':
        return ''
    value = str(value).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;').replace("'", '&apos;')
    return f' {key}="{value}"'


def _doc3040_header(attrs):
    order = ['DtBase', 'CNPJ', 'Remessa', 'Parte', 'TpArq', 'TotalCli', 'NomeResp', 'EmailResp', 'TelResp', 'MetodApPE', 'MetodDifTJE']
    return '<?xml version="1.0" encoding="UTF-8"?>\n<Doc3040' + ''.join(_xml_attr_from_dict(attrs, key) for key in order) + '>\n'


def _write_xlsx_sheets(sheets, target_uri):
    """Escreve um XLSX multi-aba. sheets = lista de (sheet_name, headers, rows),
    uma aba por elemento. Paridade com o DSG 15/06 (abas TOTAL FIS/RUNON/RUNOFF)."""
    def esc(value):
        text = '' if value is None else str(value)
        return text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

    def col_name(index):
        name = ''
        while index:
            index, rem = divmod(index - 1, 26)
            name = chr(65 + rem) + name
        return name

    def sheet_xml(headers, rows):
        all_rows = [headers] + [[row.get(header) for header in headers] for row in rows]
        sheet_rows = []
        for r_index, row in enumerate(all_rows, start=1):
            cells = []
            for c_index, value in enumerate(row, start=1):
                ref = f'{col_name(c_index)}{r_index}'
                cells.append(f'<c r="{ref}" t="inlineStr"><is><t>{esc(value)}</t></is></c>')
            sheet_rows.append(f'<row r="{r_index}">' + ''.join(cells) + '</row>')
        return '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>' + ''.join(sheet_rows) + '</sheetData></worksheet>'

    count = len(sheets)
    sheets_xml = ''.join(
        f'<sheet name="{esc(name[:31])}" sheetId="{i + 1}" r:id="rId{i + 1}"/>'
        for i, (name, _, _) in enumerate(sheets)
    )
    workbook_xml = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets>' + sheets_xml + '</sheets></workbook>'
    workbook_rels_xml = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">' + ''.join(
        f'<Relationship Id="rId{i + 1}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{i + 1}.xml"/>'
        for i in range(count)
    ) + '</Relationships>'
    rels_xml = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>'
    overrides = ''.join(
        f'<Override PartName="/xl/worksheets/sheet{i + 1}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        for i in range(count)
    )
    content_types_xml = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>' + overrides + '</Types>'

    payload = io.BytesIO()
    with zipfile.ZipFile(payload, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr('[Content_Types].xml', content_types_xml)
        zf.writestr('_rels/.rels', rels_xml)
        zf.writestr('xl/workbook.xml', workbook_xml)
        zf.writestr('xl/_rels/workbook.xml.rels', workbook_rels_xml)
        for i, (_, headers, rows) in enumerate(sheets):
            zf.writestr(f'xl/worksheets/sheet{i + 1}.xml', sheet_xml(headers, rows))
    payload.seek(0)
    _log_step(f'writing xlsx sheets={[name for name, _, _ in sheets]} target={target_uri}')
    bucket, key = _parse_s3_uri(target_uri)
    s3_client.put_object(Bucket=bucket, Key=key, Body=payload.getvalue(), ContentType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    _log_step(f'finished xlsx target={target_uri}')


# =============================================
# Helpers de coluna
# =============================================

def _has_column(df, name):
    return name in df.columns


def _coalesce_existing(df, names, default=None):
    cols = [F.col(name).cast('string') for name in names if _has_column(df, name)]
    if not cols:
        return F.lit(default).cast('string')
    return F.coalesce(*cols)


def _clean_account(col_expr):
    return F.substring(
        F.regexp_replace(F.trim(col_expr.cast('string')), '[\\u0000\\t\\n\\r]', ''),
        1,
        19,
    )


def _decimal_or_zero(col_expr):
    return F.coalesce(
        F.regexp_replace(F.trim(col_expr.cast('string')), ',', '.').cast(DecimalType(18, 2)),
        F.lit(0).cast(DecimalType(18, 2)),
    )


def _csv_text_line(*cols):
    return F.concat_ws(';', *[F.coalesce(col.cast('string'), F.lit('')) for col in cols])


def _delivery_name(default_name, override_name):
    return override_name.strip('/') if override_name else default_name


def _xml_escape(col_expr):
    value = F.coalesce(col_expr.cast('string'), F.lit(''))
    value = F.regexp_replace(value, '&', '&amp;')
    value = F.regexp_replace(value, '<', '&lt;')
    value = F.regexp_replace(value, '>', '&gt;')
    value = F.regexp_replace(value, '"', '&quot;')
    value = F.regexp_replace(value, "'", '&apos;')
    return value


def _xml_attr(attr_name, col_name):
    expr = F.col(col_name) if isinstance(col_name, str) else col_name
    return F.when(expr.isNull() | (F.length(F.trim(expr.cast('string'))) == 0), F.lit('')).otherwise(
        F.concat(F.lit(f' {attr_name}="'), _xml_escape(expr), F.lit('"'))
    )


def _optional_xml_element(tag, attrs_col, children_col=None, prefix='', close_prefix=''):
    """Emite prefix + <tag attrs/> (ou <tag attrs>children</tag>) apenas se houver
    conteudo; caso contrario string vazia - elementos opcionais nao saem vazios.

    prefix/close_prefix carregam a indentacao ('\\n' + espacos) e ficam SEMPRE fora
    das tags - nenhuma tag e cortada no meio. close_prefix so e aplicado quando ha
    children (sem children a tag abre e fecha na mesma linha)."""
    has_content = F.length(attrs_col) > 0
    if children_col is not None:
        has_content = has_content | (F.length(children_col) > 0)
        closing_indent = F.when(F.length(children_col) > 0, F.lit(close_prefix)).otherwise(F.lit(''))
        body = F.concat(F.lit(f'{prefix}<{tag}'), attrs_col, F.lit('>'), children_col, closing_indent, F.lit(f'</{tag}>'))
    else:
        body = F.concat(F.lit(f'{prefix}<{tag}'), attrs_col, F.lit('/>'))
    return F.when(has_content, body).otherwise(F.lit(''))


def _build_xml_fragments(df):
    """Monta um fragment <Cli> POR CLIENTE com todas as suas <Op> aninhadas
    (layout Doc3040 / Codoc3040_2026.xsd, confirmado em amostra real DataStage).

    - Ops ordenadas pela posicao original no XML de entrada (op_pos).
    - Filhos de Op na ordem do leiaute OFICIAL Bacen (SCR3040_Leiaute.xls, secoes
      b..h): Venc, Gar, Inf, [Sicor - nao gerado], ContInstFinRes4966.
      ATENCAO: o validador Bacen (Release 13657) rejeita Venc depois de Inf com
      B01 "One of {Inf, Sicor, ContInstFinRes4966} is expected" - a ordem antiga
      (Inf, Venc, Gar) vinha do Codoc3040_2026.xsd local, que estava divergente.
    - Elementos opcionais e atributos sem valor sao OMITIDOS (nao saem vazios).
    - Saida INDENTADA por elemento (1 espaco por nivel: Op=1, filhos=2, Estagio=3),
      via prefixos '\\n ' entre as tags - tags nunca sao cortadas no meio e cada
      <Cli> continua sendo 1 registro atomico no write/assembly.
    """
    inf_attrs = F.concat(
        # DSX 2026-06-15: Cd_2 passa DIRETO de Join_conta_cartao.Cd_2; o rewrite
        # antigo (prefixo 928949220210 -> 0299, coluna vCD2) saiu do Trf_Filtra.
        _xml_attr('Cd', 'Cd_2'),
        _xml_attr('Ident', 'Ident'),
        _xml_attr('Tp', 'Tp_2'),
        _xml_attr('Valor', 'Valor'),
        _xml_attr('Perc', 'Perc'),
        _xml_attr('Qtd', 'Qtd'),
    )
    venc_attrs = F.concat(*[_xml_attr(f'v{code}', f'v{code}') for code in INFO_CODES])
    gar_attrs = F.concat(
        _xml_attr('Tp', 'Tp_3'),
        _xml_attr('Ident', 'Ident_2'),
        _xml_attr('PercGar', 'PercGar'),
        _xml_attr('VlrOrig', 'VlrOrig'),
        _xml_attr('VlrData', 'VlrData'),
        _xml_attr('DtReav', 'DtReav'),
    )
    estagio_attrs = F.concat(
        _xml_attr('Motivo', 'EstagioMotivo'),
        _xml_attr('DtAlocacao', 'EstagioDtAlocacao'),
    )
    cont4966_attrs = F.concat(
        _xml_attr('ClasAtFin', 'ClasAtFin'),
        _xml_attr('EstInstFin', 'EstInstFin'),
        _xml_attr('VlrContBr', 'VlrContBr4966'),
        _xml_attr('TJE', 'TJE'),
        _xml_attr('CartProvMin', 'CartProvMin'),
        _xml_attr('RendMes', 'RendMes'),
    )

    op_children = F.concat(
        _optional_xml_element('Venc', venc_attrs, prefix='\n  '),
        _optional_xml_element('Gar', gar_attrs, prefix='\n  '),
        _optional_xml_element('Inf', inf_attrs, prefix='\n  '),
        _optional_xml_element(
            'ContInstFinRes4966', cont4966_attrs,
            _optional_xml_element('Estagio', estagio_attrs, prefix='\n   '),
            prefix='\n  ', close_prefix='\n  ',
        ),
    )

    op_xml = F.concat(
        F.lit('\n <Op'),
        _xml_attr('IPOC', 'IPOC'),
        _xml_attr('Contrt', 'Contrt'),
        _xml_attr('Mod', 'Mod'),
        _xml_attr('Cosif', 'Cosif'),
        _xml_attr('OrigemRec', 'OrigemRec'),
        _xml_attr('Indx', 'Indx'),
        _xml_attr('PercIndx', 'PercIndx'),
        _xml_attr('VarCamb', 'VarCamb'),
        _xml_attr('CEP', 'CEP'),
        _xml_attr('TaxEft', 'TaxEft'),
        _xml_attr('DtContr', 'DtContr'),
        _xml_attr('VlrContr', 'VlrContr'),
        _xml_attr('NatuOp', 'NatuOp'),
        _xml_attr('DtVencOp', 'DtVencOp'),
        _xml_attr('ClassOp', 'ClassOp'),
        _xml_attr('ProvConsttd', 'ProvConsttd'),
        _xml_attr('DtaProxParcela', 'DtaProxParcela'),
        _xml_attr('VlrProxParcela', 'VlrProxParcela'),
        _xml_attr('QtdParcelas', 'QtdParcelas'),
        _xml_attr('DiaAtraso', 'DiaAtraso'),
        _xml_attr('DetCli', 'DetCli'),
        _xml_attr('CaracEspecial', 'CaracEspecialFinal'),
        F.lit('>'),
        op_children,
        # Sem filhos a Op abre e fecha na mesma linha.
        F.when(F.length(op_children) > 0, F.lit('\n </Op>')).otherwise(F.lit('</Op>')),
    )

    cli_key_columns = [
        'dt_data', 'Cd', 'Tp', 'Autorzc', 'PorteCli', 'IniRelactCli',
        'FatAnual', 'ClassCli', 'TpCtrlNorm', 'CongEcon',
    ]
    grouped_df = df.withColumn('op_xml', op_xml).groupBy(*cli_key_columns).agg(
        F.array_sort(
            F.collect_list(F.struct(F.col('op_pos').alias('pos'), F.col('op_xml').alias('xml')))
        ).alias('ops_sorted')
    )

    cli_attrs = F.concat(
        _xml_attr('Cd', 'Cd'),
        _xml_attr('Tp', 'Tp'),
        _xml_attr('Autorzc', 'Autorzc'),
        _xml_attr('PorteCli', 'PorteCli'),
        _xml_attr('IniRelactCli', 'IniRelactCli'),
        _xml_attr('FatAnual', 'FatAnual'),
        _xml_attr('ClassCli', 'ClassCli'),
        _xml_attr('TpCtrl', 'TpCtrlNorm'),
        _xml_attr('CongEcon', 'CongEcon'),
    )
    return grouped_df.withColumn(
        'value',
        F.concat(
            F.lit('<Cli'), cli_attrs, F.lit('>'),
            F.concat_ws('', F.expr('transform(ops_sorted, o -> o.xml)')),
            F.lit('\n</Cli>'),
        ),
    )


# =============================================
# Lookups (pequenos) - Spark puro + broadcast
# =============================================

def _safe_empty_lookup(columns):
    df = spark.createDataFrame([], StructType([]))
    for column in columns:
        df = df.withColumn(column, F.lit(None).cast('string'))
    return df


def _read_s3_dataset(name, path, required=False):
    lower = path.lower().rstrip('/')
    try:
        if lower.endswith('.parquet') or '/parquet/' in lower or lower.endswith('/parquet'):
            df = spark.read.parquet(path)
        elif lower.endswith('.json') or lower.endswith('.jsonl') or '/json/' in lower or lower.endswith('/json'):
            df = spark.read.json(path)
        else:
            df = spark.read.option('header', True).option('quote', '"').csv(path)
        _log_step(f'lookup {name} loaded from {path} columns={len(df.columns)}')
        return df
    except Exception as exc:
        if required:
            raise
        _log_step(f'WARN: lookup opcional {name} nao encontrado/lido em {path}: {exc}')
        return spark.createDataFrame([], StructType([]))


def _normalize_lookup_columns(df, mapping):
    if not df.columns:
        return _safe_empty_lookup(list(mapping.keys()))
    out = df
    for canonical, candidates in mapping.items():
        out = out.withColumn(canonical, _coalesce_existing(out, [canonical] + candidates))
    return out.select(*mapping.keys()).dropDuplicates()


def _read_text_lines(path, required=False):
    _log_step(f'reading text lines from {path}')
    try:
        return spark.read.text(path).withColumnRenamed('value', 'line')
    except Exception as exc:
        if required:
            raise
        _log_step(f'WARN: arquivo texto opcional nao encontrado/lido em {path}: {exc}')
        return spark.createDataFrame([], StructType([])).withColumn('line', F.lit(None).cast('string'))


def _derive_downdircontas_lookups(path):
    """Arquivo posicional downdircontas_YYYYMMDD.txt (layout DataStage, posicoes 1-based)."""
    _log_step(f'preparing downdircontas-derived lookups from {path}')
    lines_df = _read_text_lines(path, required=False)
    if not lines_df.columns:
        return _safe_empty_lookup(['CONTA', 'FLAG', 'NUM_ORGNZ']), _safe_empty_lookup(['NUM_CTA_CATAO', 'MOD'])

    # [ANALISTA] regras dos arquivos posicionais downdircontas (ex-tabelas Oracle), confirmadas:
    #   - RunOff (BASE_RUN_OFF): RUNOFF_IND pos 592 == 'S'  ->  conta DIA_CONTA = pos 7 tam 19
    #   - exclusao CTA_DIA (Mod 1904): status pos 83 OU pos 84 em status_runoff_values
    #                                  OU atraso pos 171 tam 5 > 5; chave = conta + Mod '1904'
    #   - linha < 175 chars e NUM_ORGNZ (pos 1-3) in {000,999} = header/trailer -> descartadas
    # (status_runoff_values e a lista de status de EXCLUSAO CTA_DIA -- nome historico; rename em MELHORIAS item 10)
    status_runoff_values = ['B', 'H', 'I', 'J', 'L', 'M', 'Q', 'S', 'U', 'Y', 'X']
    parsed_df = lines_df.filter(F.col('line').isNotNull()).filter(F.length(F.col('line')) >= 175).withColumn(
        'NUM_ORGNZ', F.substring(F.col('line'), 1, 3)
    ).filter(
        ~F.col('NUM_ORGNZ').isin('000', '999')
    ).withColumn(
        'DIA_CONTA', _clean_account(F.substring(F.col('line'), 7, 19))
    ).withColumn(
        'RUNOFF_IND', F.upper(F.trim(F.substring(F.col('line'), 592, 1)))
    ).withColumn(
        'DLDIA_STATUS', F.upper(F.trim(F.substring(F.col('line'), 83, 1)))
    ).withColumn(
        'DLDIA_STATUS_2', F.upper(F.trim(F.substring(F.col('line'), 84, 1)))
    ).withColumn(
        'DLDIA_DIAS_ATRASO', F.coalesce(F.regexp_extract(F.trim(F.substring(F.col('line'), 171, 5)), r'-?\d+', 0).cast('int'), F.lit(0))
    ).filter(
        F.col('DIA_CONTA').isNotNull() & (F.length(F.col('DIA_CONTA')) > 0)
    )

    base_run_off_df = parsed_df.filter(F.col('RUNOFF_IND') == F.lit('S')).select(
        F.col('DIA_CONTA').alias('CONTA'),
        F.lit('1').alias('FLAG'),
        F.col('NUM_ORGNZ'),
    ).dropDuplicates(['CONTA'])

    cta_dia_df = parsed_df.filter(
        F.col('DLDIA_STATUS').isin(status_runoff_values)
        | F.col('DLDIA_STATUS_2').isin(status_runoff_values)
        | (F.col('DLDIA_DIAS_ATRASO') > 5)
    ).select(
        F.col('DIA_CONTA').alias('NUM_CTA_CATAO'),
        F.lit('1904').alias('MOD'),
    ).dropDuplicates(['NUM_CTA_CATAO', 'MOD'])

    return base_run_off_df, cta_dia_df


# =============================================
# 1. SOURCE - Leitura SPLITTABLE do XML (stage Le_Cadoc3040)
# =============================================
# spark-xml com rowTag=Cli divide o XML nao comprimido em splits Hadoop:
# cada task parseia um trecho do arquivo de 20GB em paralelo.
# Schema EXPLICITO: evita scan extra de inferencia e garante Inf/Gar como array.

INF_SCHEMA = StructType([
    StructField('_Cd', StringType(), True),
    StructField('_Tp', StringType(), True),
    StructField('_Ident', StringType(), True),
    StructField('_Valor', StringType(), True),
    StructField('_Perc', StringType(), True),
    StructField('_Qtd', StringType(), True),
])

GAR_SCHEMA = StructType([
    StructField('_Tp', StringType(), True),
    StructField('_Ident', StringType(), True),
    StructField('_PercGar', StringType(), True),
    StructField('_VlrOrig', StringType(), True),
    StructField('_VlrData', StringType(), True),
    StructField('_DtReav', StringType(), True),
])

# Layout 2026: vencimentos vem no elemento <Venc v20=.. v330=../> (1 por Op).
VENC_SCHEMA = StructType([StructField(f'_v{code}', StringType(), True) for code in INFO_CODES])

ESTAGIO_SCHEMA = StructType([
    StructField('_Motivo', StringType(), True),
    StructField('_DtAlocacao', StringType(), True),
])

CONT4966_SCHEMA = StructType([
    StructField('_ClasAtFin', StringType(), True),
    StructField('_EstInstFin', StringType(), True),
    StructField('_VlrContBr', StringType(), True),
    StructField('_TJE', StringType(), True),
    StructField('_CartProvMin', StringType(), True),
    StructField('_RendMes', StringType(), True),
    StructField('Estagio', ESTAGIO_SCHEMA, True),
])

OP_SCHEMA = StructType([
    StructField('_IPOC', StringType(), True),
    StructField('_Contrt', StringType(), True),
    StructField('_Mod', StringType(), True),
    StructField('_Cosif', StringType(), True),
    StructField('_OrigemRec', StringType(), True),
    StructField('_Indx', StringType(), True),
    StructField('_PercIndx', StringType(), True),
    StructField('_VarCamb', StringType(), True),
    StructField('_CEP', StringType(), True),
    StructField('_TaxEft', StringType(), True),
    StructField('_DtContr', StringType(), True),
    StructField('_VlrContr', StringType(), True),
    StructField('_NatuOp', StringType(), True),
    StructField('_DtVencOp', StringType(), True),
    StructField('_ClassOp', StringType(), True),
    StructField('_ProvConsttd', StringType(), True),
    StructField('_DtaProxParcela', StringType(), True),
    StructField('_VlrProxParcela', StringType(), True),
    StructField('_QtdParcelas', StringType(), True),
    StructField('_DiaAtraso', StringType(), True),
    StructField('_DetCli', StringType(), True),
    StructField('_CaracEspecial', StringType(), True),
    StructField('_VlrContBr', StringType(), True),
    StructField('Inf', ArrayType(INF_SCHEMA), True),
    StructField('Venc', VENC_SCHEMA, True),
    StructField('Gar', ArrayType(GAR_SCHEMA), True),
    StructField('ContInstFinRes4966', CONT4966_SCHEMA, True),
])

CLI_SCHEMA = StructType([
    StructField('_Cd', StringType(), True),
    StructField('_Tp', StringType(), True),
    StructField('_Autorzc', StringType(), True),
    StructField('_PorteCli', StringType(), True),
    StructField('_IniRelactCli', StringType(), True),
    StructField('_FatAnual', StringType(), True),
    StructField('_ClassCli', StringType(), True),
    StructField('_TpCtrl', StringType(), True),
    StructField('_CongEcon', StringType(), True),
    StructField('Op', ArrayType(OP_SCHEMA), True),
])

_log_step('starting splittable XML read (spark-xml, rowTag=Cli)')
cli_raw_df = (
    spark.read.format('com.databricks.spark.xml')
    .option('rowTag', 'Cli')
    .option('attributePrefix', '_')
    .option('valueTag', '_VALUE')
    .option('mode', 'PERMISSIVE')
    .schema(CLI_SCHEMA)
    .load(XML_INPUT_PATH)
)
_log_step('XML dataframe created (lazy)')

# Header do Doc3040 lido no driver (1MB via Range GET) - tambem fornece DtBase.
doc_header_attrs = _read_doc3040_header_attrs()
header_dtbase = (doc_header_attrs.get('DtBase') or '').strip() or None

# =============================================
# Lookups S3-only (pequenos -> broadcast)
# =============================================

downdir_base_run_off_lookup, downdir_cta_dia_lookup = _derive_downdircontas_lookups(DOWNDIRCONTAS_PATH)

base_run_off_lookup_from_files = _normalize_lookup_columns(
    _read_s3_dataset('base_run_off', _optional_path('base_run_off')),
    {
        'CONTA': ['NUM_CTA_CATAO', 'num_cta_catao', 'conta'],
        'FLAG': ['flag'],
        'NUM_ORGNZ': ['num_orgnz'],
    },
).withColumn('CONTA', F.lpad(F.trim(F.col('CONTA')), 19, '0')).withColumn(
    'FLAG', F.coalesce(F.col('FLAG'), F.lit('1'))
)

base_run_off_lookup = base_run_off_lookup_from_files.unionByName(
    downdir_base_run_off_lookup.select('CONTA', 'FLAG', 'NUM_ORGNZ'),
    allowMissingColumns=True,
).dropDuplicates(['CONTA'])

# [DSX] CTRL_DIVDA_RENEG: o arquivo no S3 e o DUMP CRU da tabela CTPL.CTRL_DIVDA_RENEG. Sem
# Oracle, o Glue replica a query INTEIRA do DataStage (DSX 2026-06-15) sobre o dump:
#   WHERE IND_RENEG > 1 AND NUM_ORGNZ = 212 AND HOR_ATULZ > SYSDATE - 65
#   SELECT IND_DOCTO->CliCd, trim(NUM_OPER)->Contrt19, mes(DAT_PROCM)->MES_MOVTO_ACORD,
#          lpad(NUM_CONTR,15,'0')->NUM_OPER_RENEG_PCELD, fmt(NVL(VAL_TOT_RENEG,0))->VAL_RENEG_OPER_CATAO,
#          '1'->VerificaReneg, IND_RENEG
# IMPORTANTE: VerificaReneg NAO e coluna fisica -- e o flag '1' "linha existe no controle
# de renegociacao", DERIVADO. Apos o left join, Op sem match fica null (=0 no Trf_Reneg).
# SYSDATE == data de execucao do job (current_date), paridade com o DataStage.
_reneg_cols = ['CliCd', 'Contrt19', 'MES_MOVTO_ACORD', 'NUM_OPER_RENEG_PCELD',
               'VAL_RENEG_OPER_CATAO', 'VerificaReneg', 'IND_RENEG']
_ctrl_raw = _read_s3_dataset('ctrl_divda_reneg', _optional_path('ctrl_divda_reneg'))
if _ctrl_raw.columns:
    _ctrl_norm = _ctrl_raw
    for _phys, _cands in {
        'IND_DOCTO': ['ind_docto'],
        'NUM_OPER': ['num_oper'],
        'DAT_PROCM': ['dat_procm'],
        'NUM_CONTR': ['num_contr'],
        'VAL_TOT_RENEG': ['val_tot_reneg'],
        'IND_RENEG': ['ind_reneg'],
        'NUM_ORGNZ': ['num_orgnz'],
        'HOR_ATULZ': ['hor_atulz', 'DAT_ATULZ', 'dat_atulz'],
    }.items():
        _ctrl_norm = _ctrl_norm.withColumn(_phys, _coalesce_existing(_ctrl_norm, [_phys] + _cands))

    # HOR_ATULZ/DAT_PROCM podem vir em varios formatos de dump (ISO, dd/MM, Oracle
    # dd-MON-yyyy, timestamp). Parser tolerante -- formato nao reconhecido vira null.
    def _parse_dump_date(col_name):
        col = F.col(col_name)
        return F.coalesce(
            F.to_date(col),
            F.to_date(col, 'yyyy-MM-dd'),
            F.to_date(col, 'dd/MM/yyyy'),
            F.to_date(col, 'dd-MMM-yyyy'),
            F.to_date(col, 'dd-MMM-yy'),
            F.to_date(F.substring(F.trim(col.cast('string')), 1, 10)),
            F.to_date(F.substring(F.trim(col.cast('string')), 1, 10), 'dd/MM/yyyy'),
        )

    _hor_atulz_date = _parse_dump_date('HOR_ATULZ')
    _dat_procm_date = _parse_dump_date('DAT_PROCM')
    _reneg_where = (
        (F.col('IND_RENEG').cast('double') > F.lit(1))
        & (F.col('NUM_ORGNZ').cast('double') == F.lit(212))
        & (_hor_atulz_date > F.date_sub(F.current_date(), 65))
    )

    # Observabilidade (item 11 MELHORIAS): contagem crua x valida + HOR_ATULZ nao parseado.
    # Denuncia coluna ausente ou data em formato inesperado -- ambos zerariam o reneg
    # silenciosamente (VerificaReneg nunca dispararia PicPay/IPOC/Alterados).
    _reneg_stats = _ctrl_norm.agg(
        F.count(F.lit(1)).alias('raw'),
        F.sum(F.when(_reneg_where, F.lit(1)).otherwise(F.lit(0))).alias('valid'),
        F.sum(F.when(_hor_atulz_date.isNull(), F.lit(1)).otherwise(F.lit(0))).alias('hor_nulo'),
    ).collect()[0]
    _log_step(
        f"ctrl_divda_reneg: linhas cruas={_reneg_stats['raw']} | validas pos-filtro "
        f"(IND_RENEG>1 & NUM_ORGNZ=212 & HOR_ATULZ>-65d)={_reneg_stats['valid']} | "
        f"HOR_ATULZ nao parseado={_reneg_stats['hor_nulo']}"
    )
    if (_reneg_stats['valid'] or 0) == 0:
        _log_step('WARN: 0 linhas validas no ctrl_divda_reneg -- NENHUMA Op sera renegociada. '
                  'Conferir nomes de coluna do dump e o formato de HOR_ATULZ/IND_RENEG/NUM_ORGNZ.')

    ctrl_divda_reneg_lookup = _ctrl_norm.filter(_reneg_where).select(
        F.col('IND_DOCTO').alias('CliCd'),
        F.substring(F.trim(F.col('NUM_OPER')), 1, 19).alias('Contrt19'),
        F.date_format(_dat_procm_date, 'MM').alias('MES_MOVTO_ACORD'),
        F.lpad(F.trim(F.col('NUM_CONTR')), 15, '0').alias('NUM_OPER_RENEG_PCELD'),
        F.coalesce(F.col('VAL_TOT_RENEG').cast(DecimalType(18, 2)), F.lit(0).cast(DecimalType(18, 2)))
        .cast('string').alias('VAL_RENEG_OPER_CATAO'),
        F.lit('1').alias('VerificaReneg'),
        F.col('IND_RENEG').cast('string').alias('IND_RENEG'),
    )
else:
    _log_step('WARN: arquivo ctrl_divda_reneg ausente/vazio; NENHUMA Op sera renegociada')
    ctrl_divda_reneg_lookup = _safe_empty_lookup(_reneg_cols)

cta_dia_lookup_from_files = _normalize_lookup_columns(
    _read_s3_dataset('cta_dia', _optional_path('cta_dia')),
    {
        'NUM_CTA_CATAO': ['num_cta_catao', 'CONTA', 'conta'],
        'MOD': ['mod', 'MOD_INT'],
    },
).withColumn('NUM_CTA_CATAO', _clean_account(F.col('NUM_CTA_CATAO'))).withColumn(
    'MOD', F.coalesce(F.col('MOD'), F.lit('1904'))
)

cta_dia_lookup = cta_dia_lookup_from_files.unionByName(
    downdir_cta_dia_lookup.select('NUM_CTA_CATAO', 'MOD'),
    allowMissingColumns=True,
).dropDuplicates(['NUM_CTA_CATAO', 'MOD'])

cpf_tratamento_lookup = _normalize_lookup_columns(
    _read_s3_dataset('cpf_tratamento', _optional_path('cpf_tratamento')),
    {
        'Cd': ['cd', 'cpf_origem'],
        'CdTratado': ['cd_tratado', 'cpf_destino'],
    },
)

cpf_exclusao_lookup = _normalize_lookup_columns(
    _read_s3_dataset('cpf_exclusao', _optional_path('cpf_exclusao')),
    {
        'Cd': ['cd', 'cpf'],
        'Contrt19': ['contrt19', 'Contrt'],
    },
).withColumn('Contrt19', F.substring(F.trim(F.col('Contrt19')), 1, 19))


# =============================================
# 2. TRANSFORMACOES (paridade stage a stage com o DataStage)
# =============================================
_log_step('building business transformation plan')

# Stage Le_Cadoc3040 -> normaliza Cli e explode Op (posexplode da ordem original do Op).
cli_df = cli_raw_df.select(
    F.col('_Cd').alias('Cd'),
    F.col('_Tp').alias('Tp'),
    F.col('_Autorzc').alias('Autorzc'),
    F.col('_PorteCli').alias('PorteCli'),
    F.col('_IniRelactCli').alias('IniRelactCli'),
    F.col('_FatAnual').alias('FatAnual'),
    F.col('_ClassCli').alias('ClassCli'),
    F.col('_TpCtrl').alias('TpCtrl'),
    F.col('_CongEcon').alias('CongEcon'),
    F.input_file_name().alias('_source_file'),
    F.posexplode_outer('Op').alias('op_pos', 'op'),
)


def _first_inf_field(field):
    # Paridade com o original: F.first(<campo>, ignorenulls=True) por coluna
    # (primeiro Inf cujo campo nao e nulo), agora deterministico e sem shuffle.
    return F.expr(f"try_element_at(filter(op.Inf, x -> x.{field} is not null), 1).{field}")


def _first_gar_field(field):
    return F.expr(f"try_element_at(filter(op.Gar, x -> x.{field} is not null), 1).{field}")


def _inf_code_value(code):
    # Paridade com o pivot original: primeiro _Valor do Inf com _Cd == code.
    return F.expr(f"try_element_at(filter(op.Inf, x -> trim(x._Cd) = '{code}'), 1)._Valor")


op_base_df = cli_df.select(
    'Cd', 'Tp', 'Autorzc', 'PorteCli', 'IniRelactCli', 'FatAnual', 'ClassCli', 'TpCtrl', 'CongEcon',
    '_source_file', 'op_pos',
    F.col('op._IPOC').alias('IPOC'),
    F.col('op._Contrt').alias('Contrt'),
    F.col('op._Mod').alias('Mod'),
    F.col('op._Cosif').alias('Cosif'),
    F.col('op._OrigemRec').alias('OrigemRec'),
    F.col('op._Indx').alias('Indx'),
    F.col('op._PercIndx').alias('PercIndx'),
    F.col('op._VarCamb').alias('VarCamb'),
    F.col('op._CEP').alias('CEP'),
    F.col('op._TaxEft').alias('TaxEft'),
    F.col('op._DtContr').alias('DtContr'),
    F.col('op._VlrContr').alias('VlrContr'),
    F.col('op._NatuOp').alias('NatuOp'),
    F.col('op._DtVencOp').alias('DtVencOp'),
    F.col('op._ClassOp').alias('ClassOp'),
    F.col('op._ProvConsttd').alias('ProvConsttd'),
    F.col('op._DtaProxParcela').alias('DtaProxParcela'),
    F.col('op._VlrProxParcela').alias('VlrProxParcela'),
    F.col('op._QtdParcelas').alias('QtdParcelas'),
    F.col('op._DiaAtraso').alias('DiaAtraso'),
    F.col('op._DetCli').alias('DetCli'),
    F.col('op._CaracEspecial').alias('CaracEspecial'),
    # Layout 2026: VlrContBr mora em <ContInstFinRes4966>; layout antigo, no proprio Op.
    F.coalesce(F.col('op._VlrContBr'), F.col('op.ContInstFinRes4966._VlrContBr')).alias('VlrContBr'),
    # ContInstFinRes4966 + Estagio (layout 2026) - passthrough para o XML final.
    F.col('op.ContInstFinRes4966._ClasAtFin').alias('ClasAtFin'),
    F.col('op.ContInstFinRes4966._EstInstFin').alias('EstInstFin'),
    F.col('op.ContInstFinRes4966._VlrContBr').alias('VlrContBr4966'),
    F.col('op.ContInstFinRes4966._TJE').alias('TJE'),
    F.col('op.ContInstFinRes4966._CartProvMin').alias('CartProvMin'),
    F.col('op.ContInstFinRes4966._RendMes').alias('RendMes'),
    F.col('op.ContInstFinRes4966.Estagio._Motivo').alias('EstagioMotivo'),
    F.col('op.ContInstFinRes4966.Estagio._DtAlocacao').alias('EstagioDtAlocacao'),
    # v20..v330: fonte preferencial e o elemento <Venc> (layout 2026); fallback
    # para o pivot de <Inf Cd=...> (layout antigo assumido pelo script original).
    *[F.coalesce(F.col(f'op.Venc._v{code}'), _inf_code_value(code)).alias(f'v{code}') for code in INFO_CODES],
    # Primeiro Inf (Cd_2/Ident/Tp_2/Valor/Perc/Qtd) e primeiro Gar.
    _first_inf_field('_Cd').alias('Cd_2'),
    _first_inf_field('_Ident').alias('Ident'),
    _first_inf_field('_Tp').alias('Tp_2'),
    _first_inf_field('_Valor').alias('Valor'),
    _first_inf_field('_Perc').alias('Perc'),
    _first_inf_field('_Qtd').alias('Qtd'),
    _first_gar_field('_Tp').alias('Tp_3'),
    _first_gar_field('_Ident').alias('Ident_2'),
    _first_gar_field('_PercGar').alias('PercGar'),
    _first_gar_field('_VlrOrig').alias('VlrOrig'),
    _first_gar_field('_VlrData').alias('VlrData'),
    _first_gar_field('_DtReav').alias('DtReav'),
)

# dt_data: data do nome do arquivo -> DtBase do header Doc3040 -> UNKNOWN.
extracted_from_file = F.regexp_extract(F.input_file_name(), r'(\d{8})', 1)
op_base_df = op_base_df.withColumn(
    'dt_data',
    F.coalesce(
        F.when(extracted_from_file != '', extracted_from_file),
        F.lit(header_dtbase).cast('string'),
        F.lit('UNKNOWN'),
    ),
)

# Stage Copy_of_Trf_Xml -> normalizacao CPF/contrato e marca de exclusao.
cpf_tratamento_join = cpf_tratamento_lookup.select(
    F.col('Cd').alias('cpf_tratamento_cd'), F.col('CdTratado'),
)
copy_xml_df = op_base_df.join(
    F.broadcast(cpf_tratamento_join), op_base_df.Cd == cpf_tratamento_join.cpf_tratamento_cd, 'left'
).drop('cpf_tratamento_cd')
copy_xml_df = copy_xml_df.withColumn('Contrt19', F.substring(F.trim(F.col('Contrt')), 1, 19))
copy_xml_df = copy_xml_df.withColumn('NUM_CTA_CATAO', _clean_account(F.col('Contrt')))
# [PENDENTE] cpf_tratamento (CdTratado) e Cd_1 sao calculados aqui, mas o XML final
# emite o 'Cd' CRU no <Cli Cd=...> (ver cli_attrs em _build_xml_fragments) -- Cd_1 NAO
# e emitido. Confirmar com o analista se a saida deveria usar o CPF substituido
# (CdTratado). Se o lookup cpf_tratamento estiver vazio, Cd_1==Cd (Tp=1) e nao muda nada.
copy_xml_df = copy_xml_df.withColumn('CdTratadoFinal', F.coalesce(F.col('CdTratado'), F.col('Cd')))
copy_xml_df = copy_xml_df.withColumn('Cd_1', F.when(F.trim(F.col('Tp')) == '1', F.col('CdTratadoFinal')).otherwise(F.col('DetCli')))
copy_xml_df = copy_xml_df.withColumn(
    'CaracEspecialNorm',
    F.when((F.col('CaracEspecial') == '19') & (F.substring(F.col('Tp_2'), 1, 2) == '03'), F.lit(None).cast('string')).otherwise(F.col('CaracEspecial')),
)

cpf_exclusao_join = cpf_exclusao_lookup.select(
    F.col('Cd').alias('cpf_exclusao_cd'), F.col('Contrt19').alias('cpf_exclusao_contrt19'),
).withColumn('cpf_excluir', F.lit(1))
copy_xml_df = copy_xml_df.join(
    F.broadcast(cpf_exclusao_join),
    (copy_xml_df.Cd == cpf_exclusao_join.cpf_exclusao_cd)
    & ((cpf_exclusao_join.cpf_exclusao_contrt19.isNull()) | (copy_xml_df.Contrt19 == cpf_exclusao_join.cpf_exclusao_contrt19)),
    'left',
).drop('cpf_exclusao_cd', 'cpf_exclusao_contrt19').withColumn(
    'cpf_deletar_contrato', F.coalesce(F.col('cpf_excluir'), F.lit(0))
).drop('cpf_excluir')

# [PENDENTE] cpf_deletar_contrato e is_tp_0316 sao calculados mas NENHUM filtro os aplica
# (nao ha exclusao de operacao em lugar nenhum do job). No DataStage havia exclusao? Pode
# ser parte da resposta de I13 (cliente com soma de vencimentos < R$200). Decidir com o negocio.
copy_xml_df = copy_xml_df.withColumn('is_tp_0316', F.col('Tp_2') == F.lit('0316'))

# Stage LKP_CONTA / OC_CTA_DIA -> exclusao por conta + modalidade fixa 1904.
cta_dia_lookup_prepared = cta_dia_lookup.select(
    F.col('NUM_CTA_CATAO').alias('cta_num_cta_catao'),
    F.col('MOD').alias('cta_mod'),
).dropDuplicates(['cta_num_cta_catao', 'cta_mod']).withColumn('cta_dia_delete', F.lit(1))
with_cta_df = copy_xml_df.join(
    F.broadcast(cta_dia_lookup_prepared),
    (copy_xml_df.NUM_CTA_CATAO == cta_dia_lookup_prepared.cta_num_cta_catao)
    & (F.trim(copy_xml_df.Mod.cast('string')) == F.trim(cta_dia_lookup_prepared.cta_mod.cast('string'))),
    'left',
).drop('cta_num_cta_catao', 'cta_mod')
with_cta_df = with_cta_df.withColumn('cta_dia_delete', F.coalesce(F.col('cta_dia_delete'), F.lit(0)))
with_cta_df = with_cta_df.filter(F.col('cta_dia_delete') == 0)
with_cta_df = with_cta_df.withColumn('ModJoin', F.col('Mod'))

# Stage Ora_CTRL_DIVDA_RENEG / RMD_ctrl_divda_reneg / Join_149 -> dedup por Contrt19 e left join.
ctrl_window = Window.partitionBy('Contrt19').orderBy(F.col('Contrt19'))
ctrl_dedup_df = ctrl_divda_reneg_lookup.withColumn('rn_ctrl', F.row_number().over(ctrl_window)).filter(F.col('rn_ctrl') == 1).drop('rn_ctrl')
with_reneg_df = with_cta_df.join(F.broadcast(ctrl_dedup_df), 'Contrt19', 'left')

# [DSX] Stage Trf_Reneg -> regra "operacao baixada PicPay" (export 2026-06-15):
#   vOperBaixadaPipcay = 1 quando Tp_2 = '0316' e Qtd = '2'.
#   Quando a Op NAO e renegociada (VerificaReneg != 1) E e baixada PicPay,
#   zera Cd_2/Ident/Valor/Qtd e troca Tp_2 para '0301'.
# VerificaReneg nulo (sem match no lookup de reneg) conta como "nao renegociada".
oper_baixada_pipcay = (F.trim(F.col('Tp_2')) == F.lit('0316')) & (F.trim(F.col('Qtd')) == F.lit('2'))
with_reneg_df = with_reneg_df.withColumn(
    'vOperBaixadaPipcay', F.when(oper_baixada_pipcay, F.lit(1)).otherwise(F.lit(0))
)
_aplica_baixada_pipcay = (
    F.coalesce(F.col('VerificaReneg').cast('int'), F.lit(0)) != F.lit(1)
) & (F.col('vOperBaixadaPipcay') == F.lit(1))
for _pip_col in ['Cd_2', 'Ident', 'Valor', 'Qtd']:
    with_reneg_df = with_reneg_df.withColumn(
        _pip_col, F.when(_aplica_baixada_pipcay, F.lit(None).cast('string')).otherwise(F.col(_pip_col))
    )
with_reneg_df = with_reneg_df.withColumn(
    'Tp_2', F.when(_aplica_baixada_pipcay, F.lit('0301')).otherwise(F.col('Tp_2'))
)

# [DSX][ANALISTA] Stage Trf_Reneg -> IPOC de renegociacao. So para operacoes
# renegociadas (VerificaReneg = 1) o IPOC e reconstruido; senao mantem o de origem.
# Estrutura confirmada pelo analista: cnpj8(09516419) + '0299' + '1' + cpf(11) + contrato(15)
# == prefixo fixo '0951641902991' + vCnpjCpf + vContratoNew, onde:
#   vCnpjCpf     = Cd (se Tp = '1') senao DetCli[1..8]
#   vContratoNew = NUM_OPER_RENEG_PCELD ajustado a 15 posicoes (zeros a esquerda)
#   vIPOC        = '0951641902991' + trim(vCnpjCpf) + vContratoNew
# O prefixo fixo embute o CNPJ8 09516419 (= PPBANK_CNPJ8 default). No arquivo PPBANK
# a sobrescrita posterior do CNPJ8 do IPOC normaliza os 8 primeiros digitos para
# PPBANK_CNPJ8 (S81 preservado mesmo com --P_CNPJ custom); no BASE_RUN_OFF o prefixo
# fica como no DataStage (paridade).
_v_cnpj_cpf = F.when(F.trim(F.col('Tp')) == F.lit('1'), F.col('Cd')).otherwise(F.substring(F.col('DetCli'), 1, 8))
_v_contrato_new = F.expr("right(concat('000000000000000', trim(NUM_OPER_RENEG_PCELD)), 15)")
with_reneg_df = with_reneg_df.withColumn(
    'IPOC',
    F.when(
        F.coalesce(F.col('VerificaReneg').cast('int'), F.lit(0)) == F.lit(1),
        F.concat(F.lit('0951641902991'), F.trim(_v_cnpj_cpf), _v_contrato_new),
    ).otherwise(F.col('IPOC')),
)

# Stage TRF_IDENT -> normalizacao CPF, TpCtrl e CaracEspecial.
# [PENDENTE] 'CPF' aqui e o Cd com 14 posicoes (zeros a esquerda), mas NAO e emitido no
# XML (o <Cli Cd> usa o Cd cru). Mantido por paridade/carga. Confirmar se a saida precisa
# do documento padronizado (11 PF / 14 PJ) -- hoje depende do formato que vem na entrada.
trf_ident_df = with_reneg_df.withColumn('CPF', F.expr("right(concat('00000000000000', trim(Cd)), 14)"))
trf_ident_df = trf_ident_df.withColumn(
    'TpCtrlNorm',
    F.when(F.col('TpCtrl').isNull(), F.col('TpCtrl')).otherwise(F.expr("right(concat('0', trim(TpCtrl)), 2)")),
)
trf_ident_df = trf_ident_df.withColumn(
    'CaracEspecialFinal',
    F.when(F.col('v330').isNotNull() & (F.length(F.trim(F.col('v330'))) > 0), F.lit('19;11')).otherwise(F.col('CaracEspecialNorm')),
)

# Stage Ora_BASE_RUN_OFF / LKP_01_BASE_RUN_OFF -> lookup BASE_RUN_OFF.
base_lookup_prepared = base_run_off_lookup.select('CONTA', 'FLAG').dropDuplicates(['CONTA'])
enriched_df = trf_ident_df.join(
    F.broadcast(base_lookup_prepared), trf_ident_df.NUM_CTA_CATAO == base_lookup_prepared.CONTA, 'left'
)

# Stage Srt_Xml / Trf_Filtra -> regra de duplicidade.
# DSX 2026-06-15: a deduplicacao antiga (chave Contrt+Mod com Tp_2 in (0310,0399))
# SAIU do Trf_Filtra; o Data_Set nao tem mais constraint de duplicado e os ramos
# usam so FiltroTabela. Neutralizado para 0 (todos os registros passam) -- a coluna
# vRegDuplicado e mantida como constante para preservar os filtros `== 0` downstream.
dedup_logic_df = enriched_df.withColumn('vRegDuplicado', F.lit(0))

# Stage Trf_Filtra -> separa PPBANK/BASE_RUN_OFF/RUNON/RUNOFF/FIS por FiltroTabela.
# DSX 2026-06-15: o rewrite de Cd_2 (prefixo 928949220210 -> '0299', via colunas
# filtro/TIPO/parte1-3/vCD2 + arquivo CADOC3040_Alterados) SAIU do Trf_Filtra.
# Cd_2 agora vai direto (Join_conta_cartao.Cd_2) para os datasets e para o XML.
dedup_logic_df = dedup_logic_df.withColumn('FiltroTabela', F.when(F.col('FLAG').cast('int') == 1, F.lit(1)).otherwise(F.lit(0)))

for column in VALUE_COLUMNS:
    dedup_logic_df = dedup_logic_df.withColumn(f'{column}_dec', _decimal_or_zero(F.col(column)))

# =============================================
# CHECKPOINT - materializa o resultado de negocio UMA vez.
# E o unico momento em que o XML de 20GB e lido/parseado.
# Todos os ramos de saida leem deste parquet.
# =============================================
_log_step('materializing normalized dataset to parquet (single pass over the 20GB XML)')
dedup_logic_df.write.mode('overwrite').partitionBy('dt_data').parquet(NORMALIZED_PARQUET_PATH)
_log_step('normalized parquet written; reading back for downstream branches')
base_df = spark.read.parquet(NORMALIZED_PARQUET_PATH)

# Observabilidade (item 11 MELHORIAS): quantas Ops casaram no ctrl_divda_reneg.
# Conta sobre o checkpoint (nao re-parseia o XML). 0 aqui com lookup nao-vazio =
# chave de join (Contrt19 x NUM_OPER) divergente -> investigar formato do contrato.
_reneg_matched_ops = base_df.filter(F.coalesce(F.col('VerificaReneg').cast('int'), F.lit(0)) == F.lit(1)).count()
_log_step(f'reneg: Ops com match no ctrl_divda_reneg (VerificaReneg=1) = {_reneg_matched_ops}')

base_columns = [
    'dt_data', '_source_file', 'Cd', 'Tp', 'Cd_1', 'CPF', 'NUM_CTA_CATAO',
    'Contrt', 'Contrt19', 'Mod', 'ModJoin', 'Cd_2', 'Tp_2', 'CaracEspecialFinal',
    'FLAG', 'FiltroTabela', 'vRegDuplicado', 'CliCd', 'MES_MOVTO_ACORD',
    'NUM_OPER_RENEG_PCELD', 'VAL_RENEG_OPER_CATAO', 'VerificaReneg', 'IND_RENEG',
]
available_base_columns = [column for column in base_columns if column in base_df.columns]
available_value_columns = [f'{column}_dec' for column in VALUE_COLUMNS]

# Colunas adicionais exigidas pelo _build_xml_fragments (atributos Cli/Op/Inf/
# Venc/Gar/ContInstFinRes4966/Estagio + op_pos para ordenar as Op no <Cli>).
# BUGFIX: o script original selecionava apenas base_columns nos datasets PPBANK/
# BASE_RUN_OFF, mas o montador de XML referencia Autorzc, PorteCli, Cosif etc.
# -> UNRESOLVED_COLUMN na geracao dos arquivos finais.
XML_FRAGMENT_COLUMNS = [
    'op_pos', 'IniRelactCli', 'CongEcon',
    'Autorzc', 'PorteCli', 'FatAnual', 'ClassCli', 'TpCtrlNorm',
    'IPOC', 'Cosif', 'OrigemRec', 'Indx', 'PercIndx', 'VarCamb', 'CEP', 'TaxEft',
    'DtContr', 'VlrContr', 'NatuOp', 'DtVencOp', 'ClassOp', 'ProvConsttd',
    'DtaProxParcela', 'VlrProxParcela', 'QtdParcelas', 'DiaAtraso', 'DetCli',
    'Ident', 'Valor', 'Perc', 'Qtd',
    'Tp_3', 'Ident_2', 'PercGar', 'VlrOrig', 'VlrData', 'DtReav',
    'ClasAtFin', 'EstInstFin', 'VlrContBr4966', 'TJE', 'CartProvMin', 'RendMes',
    'EstagioMotivo', 'EstagioDtAlocacao',
] + [f'v{code}' for code in INFO_CODES]
available_xml_columns = [
    column for column in XML_FRAGMENT_COLUMNS
    if column in base_df.columns and column not in available_base_columns
]

# Stage Data_Set -> registros nao duplicados.
data_set_df = base_df.filter(F.col('vRegDuplicado') == 0).select(*available_base_columns, *available_value_columns)

# Stage Lnk_cd2_alterados / SF_CD_2_ALTERADOS -> auditoria de operacoes alteradas.
# DSX 2026-06-15: o link agora dispara por VerificaReneg = 1 (operacao renegociada),
# nao mais por filtro = 1 (Cd_2 reescrito, regra que saiu). Cd_2 sai direto.
cd2_alterados_df = base_df.filter(F.coalesce(F.col('VerificaReneg').cast('int'), F.lit(0)) == F.lit(1)).select(
    'dt_data', 'Cd', F.col('Cd_2').alias('CD_2_alterado')
)

# Stage Grava_CadocMensal_Econtrados_temp -> dataset BASE_RUN_OFF encontrado.
base_run_off_df = base_df.filter((F.col('vRegDuplicado') == 0) & (F.col('FiltroTabela') == 1)).select(
    *available_base_columns, *available_xml_columns, *available_value_columns
)

# Stage Grava_CadocMensal_PPBANK_temp -> dataset PPBANK.
# IPOC agora e lido da fonte (atributo _IPOC do Op, layout 2026): no arquivo
# PPBANK os 8 primeiros digitos (CNPJ8 do IPOC) sao substituidos por
# PPBANK_CNPJ8 - o MESMO valor emitido no CNPJ do header (regra S81).
ppbank_df = base_df.filter((F.col('vRegDuplicado') == 0) & (F.col('FiltroTabela') == 0)).withColumn(
    'ipoc_ppbank',
    F.when(
        F.col('IPOC').isNotNull() & (F.length(F.trim(F.col('IPOC'))) > 0),
        F.concat(F.lit(PPBANK_CNPJ8), F.expr('substring(IPOC, 9, 100000)')),
    ).otherwise(F.lit(None).cast('string')),
).withColumn('CEP_PPBANK', F.lit('05317020')).select(
    *available_base_columns, *available_xml_columns, 'ipoc_ppbank', 'CEP_PPBANK', *available_value_columns
)

# Stage RUNOFF/FIS/RUNON -> bases para agregacao de modalidade.
runoff_detail_df = base_df.filter((F.col('vRegDuplicado') == 0) & (F.col('FiltroTabela') == 1)).withColumn('MODALIDADE', F.col('Mod'))
fis_detail_df = base_df.filter(F.col('vRegDuplicado') == 0).withColumn('MODALIDADE', F.col('Mod'))
runon_detail_df = base_df.filter((F.col('vRegDuplicado') == 0) & (F.col('FiltroTabela') == 0)).withColumn('MODALIDADE', F.col('Mod'))


def _aggregate_modalidade(df, dataset_name):
    aggregations = [F.sum(F.col(f'{column}_dec')).alias(column) for column in VALUE_COLUMNS]
    return df.groupBy('dt_data', 'MODALIDADE').agg(*aggregations).withColumn('dataset', F.lit(dataset_name))


# Stage AGG_RUNOFF / AGG_TOTAL / AGG_RUNON -> soma por MODALIDADE.
agg_modalidade_df = (
    _aggregate_modalidade(runoff_detail_df, 'RUNOFF')
    .unionByName(_aggregate_modalidade(fis_detail_df, 'FIS'))
    .unionByName(_aggregate_modalidade(runon_detail_df, 'RUNON'))
)

# No XML PPBANK, IPOC e CEP saem com os valores proprios do PPBANK (paridade com
# as colunas ipoc_ppbank/CEP_PPBANK que o DataStage preparava para esse arquivo).
ppbank_xml_ready_df = (
    ppbank_df.withColumn('xml_target', F.lit('CADOC3040_xml_PPBANK'))
    .withColumn('IPOC', F.coalesce(F.col('ipoc_ppbank'), F.col('IPOC')))
    .withColumn('CEP', F.coalesce(F.col('CEP_PPBANK'), F.col('CEP')))
)
base_run_off_xml_ready_df = base_run_off_df.withColumn('xml_target', F.lit('CADOC3040_xml'))


# =============================================
# 3. SINK - Parquet particionado (agora barato: le do checkpoint, nao do XML)
# =============================================

def _write_parquet(df, relative_path, partition_keys=None):
    partition_keys = partition_keys or ['dt_data']
    output_path = _path_join(S3_OUTPUT_ROOT, relative_path)
    _log_step(f'writing parquet {relative_path} -> {output_path}')
    df.write.mode('overwrite').partitionBy(*partition_keys).parquet(output_path)
    _log_step(f'finished parquet {relative_path}')


def _write_text_final(df, relative_path, value_col='value', partition_keys=None):
    partition_keys = partition_keys or ['dt_data']
    output_path = _path_join(S3_OUTPUT_ROOT, relative_path)
    _log_step(f'writing text {relative_path} -> {output_path}')
    writer_df = df.select(*partition_keys, F.col(value_col).cast('string').alias('value'))
    if FINAL_SINGLE_FILE:
        writer_df = writer_df.coalesce(1)
    writer_df.write.mode('overwrite').partitionBy(*partition_keys).text(output_path)
    _log_step(f'finished text {relative_path}')
    return output_path


_log_step('starting parquet sink writes')
_write_parquet(data_set_df, 'work/data_set')
_write_parquet(ppbank_df, 'work/ppbank')
_write_parquet(base_run_off_df, 'work/base_run_off')
_write_parquet(cd2_alterados_df, 'audit/cd2_alterados')
_write_parquet(ppbank_xml_ready_df, 'xml_ready/ppbank')
_write_parquet(base_run_off_xml_ready_df, 'xml_ready/base_run_off')
_write_parquet(agg_modalidade_df, 'aggregates/soma_modalidade', ['dt_data', 'dataset'])
_log_step('parquet sink writes finished')


# =============================================
# 4. Saidas finais formato fornecedor (XML unico + Alterados.txt + XLSX)
# =============================================

if GENERATE_SUPPLIER_FILES:
    _log_step('starting supplier file generation')

    # Fragments por CLIENTE (um <Cli> com N <Op>); persist para nao recomputar
    # o groupBy entre o count e o write.
    ppbank_fragments_df = _build_xml_fragments(ppbank_xml_ready_df).persist()
    base_fragments_df = _build_xml_fragments(base_run_off_xml_ready_df).persist()

    # TotalCli = numero de clientes (blocos <Cli>) por dt_data em cada arquivo.
    ppbank_counts = {row['dt_data']: row['count'] for row in ppbank_fragments_df.groupBy('dt_data').count().collect()}
    base_counts = {row['dt_data']: row['count'] for row in base_fragments_df.groupBy('dt_data').count().collect()}
    dt_values = sorted(set(ppbank_counts) | set(base_counts))
    if not dt_values:
        dt_values = [row['dt_data'] for row in base_df.select('dt_data').distinct().collect()]
    _log_step(f'dt_data values for final delivery: {dt_values}')

    # Stage SF_CD_2_ALTERADOS -> CADOC3040_Alterados.txt (delimitador ';').
    cd2_lines_df = cd2_alterados_df.select(
        'dt_data',
        _csv_text_line(F.col('Cd'), F.col('CD_2_alterado')).alias('value'),
    )
    cd2_tmp_path = _write_text_final(cd2_lines_df, '_tmp/final/cd2_alterados_lines')
    for dt_value in dt_values:
        _assemble_s3_text_objects(
            _path_join(cd2_tmp_path, f'dt_data={dt_value}'),
            _path_join(S3_OUTPUT_ROOT, f'final/delivery/dt_data={dt_value}/CADOC3040_Alterados.txt'),
            header='Cd;CD_2_alterado\n',
        )

    # Stage Pj_CTCR_0003 -> XML final PPBANK e BASE_RUN_OFF (fragments distribuidos + assembly).
    ppbank_tmp_path = _write_text_final(ppbank_fragments_df, '_tmp/final/xml_ppbank_fragments')
    base_xml_tmp_path = _write_text_final(base_fragments_df, '_tmp/final/xml_base_run_off_fragments')

    for dt_value in dt_values:
        _log_step(f'assembling XML delivery files for dt_data={dt_value}')
        ppbank_attrs = dict(doc_header_attrs)
        # S81: header SEMPRE com o mesmo CNPJ8 aplicado no prefixo dos IPOC.
        # (antes so sobrescrevia se --P_CNPJ fosse passado -> header ficava com
        # o CNPJ da instituicao de origem e o validador rejeitava toda Op)
        ppbank_attrs['CNPJ'] = PPBANK_CNPJ8
        if PPBANK_HEADER_NOME_RESP:
            ppbank_attrs['NomeResp'] = PPBANK_HEADER_NOME_RESP
        if PPBANK_HEADER_EMAIL_RESP:
            ppbank_attrs['EmailResp'] = PPBANK_HEADER_EMAIL_RESP
        if PPBANK_HEADER_TEL_RESP:
            ppbank_attrs['TelResp'] = PPBANK_HEADER_TEL_RESP
        ppbank_attrs['TotalCli'] = str(ppbank_counts.get(dt_value, 0))

        base_attrs = dict(doc_header_attrs)
        base_attrs['TotalCli'] = str(base_counts.get(dt_value, 0))

        ppbank_name = _delivery_name(f'CADOC3040_xml_PPBANK_{dt_value}.xml', FINAL_XML_PPBANK_NAME)
        base_name = _delivery_name(f'CADOC3040_xml_{dt_value}.xml', FINAL_XML_BASE_RUN_OFF_NAME)

        _assemble_s3_text_objects(
            _path_join(ppbank_tmp_path, f'dt_data={dt_value}'),
            _path_join(S3_OUTPUT_ROOT, f'final/delivery/dt_data={dt_value}/{ppbank_name}'),
            header=_doc3040_header(ppbank_attrs),
            footer='</Doc3040>\n',
        )
        _assemble_s3_text_objects(
            _path_join(base_xml_tmp_path, f'dt_data={dt_value}'),
            _path_join(S3_OUTPUT_ROOT, f'final/delivery/dt_data={dt_value}/{base_name}'),
            header=_doc3040_header(base_attrs),
            footer='</Doc3040>\n',
        )

    # Stage Pj_CTCR_0005 (SOMA_MOD) + ramo _F (ex-Pj_CTCR_0008) -> XLSX de soma por
    # modalidade. DSX 15/06: DOIS arquivos com os MESMOS numeros e as MESMAS 3 abas
    # (TOTAL FIS / TOTAL RUNON / TOTAL RUNOFF). Os valores nao mudam pre/pos-reneg
    # (VlrContr/VlrContBr/v20..v330 sao so repassados e somados), entao o ramo _F nao
    # precisa de pipeline duplicado: reaproveitamos o agg_modalidade_df nos dois arquivos.
    soma_sheet_headers = ['MODALIDADE'] + VALUE_COLUMNS
    soma_dataset_tabs = [('TOTAL FIS', 'FIS'), ('TOTAL RUNON', 'RUNON'), ('TOTAL RUNOFF', 'RUNOFF')]
    soma_rows = [row.asDict(recursive=True) for row in agg_modalidade_df.orderBy('dt_data', 'dataset', 'MODALIDADE').collect()]
    for dt_value in dt_values:
        rows = [row for row in soma_rows if row.get('dt_data') == dt_value]
        sheets = [
            (tab_name, soma_sheet_headers, [row for row in rows if row.get('dataset') == dataset_name])
            for tab_name, dataset_name in soma_dataset_tabs
        ]
        _write_xlsx_sheets(
            sheets,
            _path_join(S3_OUTPUT_ROOT, f'final/delivery/dt_data={dt_value}/CADOC3040_CTCR_DSTG_SOMA_MOD_{dt_value}.xlsx'),
        )
        _write_xlsx_sheets(
            sheets,
            _path_join(S3_OUTPUT_ROOT, f'final/delivery/dt_data={dt_value}/CADOC3040_CTCR_SOMA_MOD_{dt_value}.xlsx'),
        )


# =============================================
# 5. Limpeza de intermediarios (politica de retencao fina)
# =============================================
# Roda no fim, depois de todo o assembly -> nenhuma acao Spark depende mais destes
# prefixos, entao NAO ha recomputo (custo = apenas S3 LIST + DELETE, segundos).
#   _tmp/               -> SEMPRE apagado: scratch dos fragments do XML antes do
#                          assembly (~tamanho do XML, ~19GB); valor zero apos a entrega.
#   work/, xml_ready/   -> apagados se KEEP_WORK_PARQUET=false. Mante-los e o "seguro
#                          de re-parse": regenerar o XML sem reparsear os 20GB.
#   aggregates/, audit/ -> apagados so se KEEP_AUDIT=false. Sao a evidencia de
#                          homologacao vs DataStage (soma_modalidade, cd2_alterados)
#                          e sao pequenos -> default = manter.
#   final/delivery/     -> NUNCA tocado (e a entrega).
_log_step('cleanup: removendo intermediarios conforme politica de retencao')
_delete_s3_prefix(_path_join(S3_OUTPUT_ROOT, '_tmp'))
if not KEEP_WORK_PARQUET:
    for _clean_prefix in ['work', 'xml_ready']:
        _delete_s3_prefix(_path_join(S3_OUTPUT_ROOT, _clean_prefix))
if not KEEP_AUDIT:
    for _clean_prefix in ['aggregates', 'audit']:
        _delete_s3_prefix(_path_join(S3_OUTPUT_ROOT, _clean_prefix))
_log_step('cleanup finished')

_log_step('committing Glue job')
job.commit()
_log_step('Glue job finished')
# EOF
