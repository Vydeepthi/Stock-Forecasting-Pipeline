# shared/bq.py

import os
from typing import Optional
import pandas as pd
from google.cloud import bigquery

_client: Optional[bigquery.Client] = None

def get_client() -> bigquery.Client:
    global _client
    if _client is None:
        _client = bigquery.Client()
    return _client

def query_df(sql: str) -> pd.DataFrame:
    client = get_client()
    return client.query(sql).result().to_dataframe()

def load_df(df: pd.DataFrame, table_id: str, write_disposition="WRITE_APPEND"):
    client = get_client()
    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
