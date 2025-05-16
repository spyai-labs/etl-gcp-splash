from typing import List, Literal
from google.cloud import bigquery

__all__ = [
    'BigQuerySchema',
    'BigQueryWriteMode'
]

BigQuerySchema = List[bigquery.SchemaField]
BigQueryWriteMode = Literal["WRITE_TRUNCATE", "WRITE_TRUNCATE_DATA", "WRITE_APPEND", "WRITE_EMPTY"]
