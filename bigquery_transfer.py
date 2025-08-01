from enum import EnumMeta, Enum

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


"""
data_sample = [
    '2025-07-31',
    'x9nrjqa',
    'video',
    'https://guidatv.quotidiano.net/italia2/02-08-2025/',
    'mobile',
    'xa1wi',
    None,
    2,
    2,
    0.04,
    'Leonardo Massa, Vice Presidente Southern Europe Divisione Crociere Gruppo MSC, ai microfoni di MOney Vibez Stories',
    '',
    2410,
    Timestamp('2025-07-29 10:43:04+0200', tz='Europe/Rome'),
    ['original', 'MSC', 'MoneyVibezStories'],
    'https://www.dailymotion.com/video/x9nrjqa',
    'example',
    None
]
"""


DEFAULT_FIELD = "DEFAULT"
REQUIRED_FIELD = "REQUIRED"
REPEATED_FIELD = "REPEATED"


def get_mode(mode):
    if mode is True:
        return REQUIRED_FIELD
    elif mode is list:
        return REPEATED_FIELD
    else:
        return DEFAULT_FIELD


class FieldTypeEnum(Enum):
    STRING = "STRING"
    BYTES = "BYTES"
    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    BOOLEAN = "BOOLEAN"
    TIMESTAMP = "TIMESTAMP"
    DATE = "DATE"
    TIME = "TIME"
    DATETIME = "DATETIME"
    GEOGRAPHY = "GEOGRAPHY"
    NUMERIC = "NUMERIC"
    BIGNUMERIC = "BIGNUMERIC"
    JSON = "JSON"
    RECORD = "RECORD"
    RANGE = "RANGE"


fields = [
    ("day", FieldTypeEnum.DATE, True),
    ("video_id", FieldTypeEnum.STRING, True),
    ("media_type", FieldTypeEnum.STRING, True),
    ("visitor_page_url", FieldTypeEnum.STRING, True),
    ("visitor_device_type", FieldTypeEnum.STRING, True),
    ("player_id", FieldTypeEnum.STRING, True),
    ("playlist_id", FieldTypeEnum.STRING, False),
    ("views", FieldTypeEnum.INTEGER, True),
    ("time_watched_seconds", FieldTypeEnum.INTEGER, True),
    ("view_through_rate", FieldTypeEnum.FLOAT, True),
    ("video_title", FieldTypeEnum.STRING, True),
    ("video_description", FieldTypeEnum.STRING, False),
    ("video_duration", FieldTypeEnum.INTEGER, True),
    ("video_created_time", FieldTypeEnum.TIMESTAMP, True),
    ("video_tags", FieldTypeEnum.STRING, list),
    ("video_url", FieldTypeEnum.STRING, True),
    ("player_label", FieldTypeEnum.STRING, False),
    ("estimated_earings_eur", FieldTypeEnum.NUMERIC, False)
]


WRITE_DISPOSITION="WRITE_TRUNCATE"
TABLE_ID_RAW = "robin_custom.dailymotion_raw_data"
TABLE_ID_DEF = "robin_custom.dailymotion_default_data"
TABLE_SCHEMA = [
    bigquery.schema.SchemaField(name, field.value, mode=get_mode(mode)) for name, field, mode in fields
]
MERGE_QUERY = f"""
INSERT INTO `{TABLE_ID_DEF}` ({", ".join([f[0] for f in fields])})
SELECT {", ".join([f[0] for f in fields])}
FROM `{TABLE_ID_RAW}`
"""


def get_or_create_table(client):
    try:
        return client.get_table(TABLE_ID_DEF), client.get_table(TABLE_ID_RAW)
    except NotFound:
        table_raw = bigquery.Table(TABLE_ID_RAW, schema=TABLE_SCHEMA)
        table_def = bigquery.Table(TABLE_ID_DEF, schema=TABLE_SCHEMA)
        return client.create_table(table_def), client.create_table(table_raw)


def transfer(rows):
    client = bigquery.Client(project="smart-data-platform-dev-401609")
    table_def, table_raw = get_or_create_table(client)
    job_config = bigquery.LoadJobConfig(
        write_disposition=WRITE_DISPOSITION,
        schema=TABLE_SCHEMA,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    )
    raw_job = client.load_table_from_json(rows, table_raw.table_id, job_config=job_config)
    raw_job.result()
    if raw_job.errors:
        raise ValueError(f"Job failed: {raw_job.errors}")
    insert_job = client.query(MERGE_QUERY)
    insert_job.result()
    if insert_job.errors:
        raise ValueError(f"Job failed: {insert_job.errors}")
