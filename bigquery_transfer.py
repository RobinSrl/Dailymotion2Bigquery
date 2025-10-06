import logging
import pandas as pd

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


DEFAULT_FIELD = "NULLABLE"
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


FIELDS = [
    ("day", FieldTypeEnum.DATE, True),
    ("video_id", FieldTypeEnum.STRING, True),
    ("media_type", FieldTypeEnum.STRING, False),
    ("visitor_page_url", FieldTypeEnum.STRING, False),
    ("visitor_device_type", FieldTypeEnum.STRING, False),
    ("player_id", FieldTypeEnum.STRING, False),
    ("playlist_id", FieldTypeEnum.STRING, False),
    ("views", FieldTypeEnum.INTEGER, False),
    ("time_watched_seconds", FieldTypeEnum.INTEGER, False),
    ("view_through_rate", FieldTypeEnum.FLOAT, False),
    ("video_title", FieldTypeEnum.STRING, False),
    ("video_description", FieldTypeEnum.STRING, False),
    ("video_duration", FieldTypeEnum.INTEGER, False),
    ("video_created_time", FieldTypeEnum.TIMESTAMP, False),
    ("video_tags", FieldTypeEnum.STRING, list),
    ("video_url", FieldTypeEnum.STRING, False),
    ("player_label", FieldTypeEnum.STRING, False),
    ("estimated_earnings_eur", FieldTypeEnum.NUMERIC, False),
    ("playlist_name", FieldTypeEnum.STRING, False)
]


WRITE_DISPOSITION="WRITE_TRUNCATE"
PROJECT_NAME = "smart-data-platform-dev-401609"
TABLE_ID_RAW = f"{PROJECT_NAME}.robin_custom.dailymotion_raw_data"
TABLE_ID_DEF = f"{PROJECT_NAME}.robin_custom.dailymotion_default_data"
TABLE_SCHEMA = [
    bigquery.schema.SchemaField(name, field.value, mode=get_mode(mode)) for name, field, mode in FIELDS
]
MERGE_QUERY = f"""
INSERT INTO `{TABLE_ID_DEF}` ({", ".join([f[0] for f in FIELDS])})
SELECT {", ".join([f[0] for f in FIELDS])}
FROM `{TABLE_ID_RAW}`
"""
CHUNK_SIZE = 500


def get_or_create_table(client):
    def_id = TABLE_ID_DEF
    raw_id = TABLE_ID_RAW
    try:
        return client.get_table(def_id), client.get_table(raw_id)
    except NotFound:
        table_raw = bigquery.Table(raw_id, schema=TABLE_SCHEMA)
        table_def = bigquery.Table(def_id, schema=TABLE_SCHEMA)
        return client.create_table(table_def), client.create_table(table_raw)


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def get_rows(df):
    df = df.where(pd.notnull(df), None)
    df = df[[field for field, _, __ in FIELDS]]
    rows = df.to_dict(orient="records")
    return rows


def transfer(df):
    rows = get_rows(df)
    client = bigquery.Client(project=PROJECT_NAME)
    table_def, table_raw = get_or_create_table(client)
    job_config = bigquery.LoadJobConfig(
        write_disposition=WRITE_DISPOSITION,
        schema=TABLE_SCHEMA,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    )
    logging.info("load raw data")
    # ToDo: Individuare i caratteri che rompono l'inserimento
    for chunk in chunks(rows, CHUNK_SIZE):
        try:
            raw_job = client.load_table_from_json(chunk, TABLE_ID_RAW, job_config=job_config)
            try:
                raw_job.result()
            except Exception as e:
                logging.info(f"err: {e}")
                if raw_job.errors:
                    raise ValueError(f"Job failed: {raw_job.errors} data sample: {chunk[:10]}")
            logging.info("merge tables")
            insert_job = client.query(MERGE_QUERY)
            insert_job.result()
            if insert_job.errors:
                logging.info(f"Job failed: {insert_job.errors}")
                raise ValueError(f"Job failed: {insert_job.errors}")
            logging.info("done")
        except Exception as e:
            logging.info(f"error: {e}")
            raise
