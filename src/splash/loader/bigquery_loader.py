import pandas as pd
from typing import Optional, Dict
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from tenacity import retry, stop_after_attempt, wait_fixed

from splash.defined_types import JobStats, BigQuerySchema, BigQueryWriteMode
from splash.config.settings import Settings
from splash.utils.sql_utils import generate_merge_sql, generate_update_sql
from splash.utils.schema_utils import fetch_schemas
from splash.utils.logger import setup_logger

logger = setup_logger(__name__)


class BigQueryLoaderError(Exception):
    """Custom exception for BigQueryLoader-specific failures."""
    pass


class BigQueryLoader:
    """
    Loads and merges pandas DataFrames into Google BigQuery using staging tables, schema enforcement, and retry logic for robustness.
    """
    
    client: bigquery.Client
    project: str
    dataset: str
    schemas: Dict[str, BigQuerySchema]
    
    def __init__(self) -> None:
        self.client = bigquery.Client()
        self.project = Settings.GCP_PROJECT_ID
        self.dataset = Settings.BQ_DATASET_ID
        self.schemas: Dict[str, BigQuerySchema] = fetch_schemas(Settings.GENERATE_SCHEMA)

    def get_staging_table_name(self, table_name: str) -> str:
        """Returns a staging table name using configured prefix/suffix."""
        return f"{Settings.STAGING_PREFIX}{table_name}{Settings.STAGING_SUFFIX}".strip()
    
    def generate_table_id(self, table_name: str) -> str:
        """Constructs a fully qualified BigQuery table ID."""
        return f"{self.project}.{self.dataset}.{table_name}"
    
    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def ensure_table_exists(self, table_name: str, schema: BigQuerySchema) -> None:
        """
        Verifies existence of a BigQuery table. Creates it if not found.
        Retries on transient errors.
        """
        try:
            table_id = self.generate_table_id(table_name)
            self.client.get_table(table_id)
            
        except NotFound:
            logger.info(f"Table `{table_id}` does not exist - creating a new table")
            table = bigquery.Table(table_id, schema=schema)
            self.client.create_table(table)
            
        except Exception as e:
            logger.error(f"Failed to ensure `{table_id}` exist in BigQuery")
            raise BigQueryLoaderError(f"Error ensuring table existence for `{table_id}`") from e
            
            
    def load(
        self, 
        table_name: str, 
        df: pd.DataFrame, 
        schema: Optional[BigQuerySchema] = None, 
        write_mode: Optional[BigQueryWriteMode] = None
    ) -> int:
        """
        Loads a pandas DataFrame into a BigQuery table.
        Returns the number of rows successfully loaded.
        """
        if df.empty:
            return 0
        
        table_id = self.generate_table_id(table_name)
        loaded_rows = 0
        
        try:    
            job_config = bigquery.LoadJobConfig(schema = schema, write_disposition = write_mode)
            job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
            
            if job.result():
                loaded_rows = job.output_rows or 0
                logger.info(f"BigQueryLoad - loaded {loaded_rows} rows into `{table_id}`")
        
        except Exception as e:
            error_msg = f"BigQueryLoadError - failed to load `{table_id}`"
            logger.error(error_msg)
            raise BigQueryLoaderError(error_msg) from e
        
        return loaded_rows
    
    
    def merge(
        self, 
        src_table: str, 
        dest_table: str, 
        df: pd.DataFrame, 
        key_column: str = "id"
    ) -> int:
        """
        Merges data from staging (`src_table`) into target (`dest_table`) based on a key column.
        Returns the number of rows merged.
        """
        src_table_id = self.generate_table_id(src_table)
        dest_table_id = self.generate_table_id(dest_table)
        merged_rows = 0
        
        try:
            if df.empty or df.columns.empty:
                raise ValueError("DataFrame has no columns to merge")
            
            merge_sql = generate_merge_sql(src_table_id, dest_table_id, key_column, list(df.columns))
            merge_job = self.client.query(merge_sql)
            logger.debug(f"Executing merge SQL: \n{merge_sql}")
            
            if merge_job.result():
                merged_rows = merge_job.num_dml_affected_rows or 0
                logger.info(f"BigQueryMerge - upserted {merged_rows} rows in `{dest_table_id}`")
                
        except Exception as e:
            error_msg = f"BigQueryMergeError - failed to merge `{dest_table_id}` from `{src_table_id}`"
            logger.error(error_msg)
            raise BigQueryLoaderError(error_msg) from e
        
        return merged_rows
            
    
    def update(
        self, 
        src_table: str, 
        dest_table: str, 
        df: pd.DataFrame, 
        key_column: str = "id", 
        update_action: str = "generic"
    ) -> int:
        """
        Executes an update query on the destination table, typically to mark deletions for records not present in the source.
        Returns the number of rows updated.
        """
        src_table_id = self.generate_table_id(src_table)
        dest_table_id = self.generate_table_id(dest_table)
        updated_rows = 0
        
        try:
            update_sql = generate_update_sql(src_table_id, dest_table_id, key_column)
            update_job = self.client.query(update_sql)
            logger.debug(f"Executing update SQL: \n{update_sql}")
            
            if update_job.result() and update_job.num_dml_affected_rows:
                updated_rows = update_job.num_dml_affected_rows or 0
                print(f"BigQueryUpdate - action: '{update_action}' - updated {updated_rows} rows in `{dest_table_id}`")
                
        except Exception as e:
            error_msg = f"BigQueryUpdateError - action: '{update_action}' - failed to update `{dest_table_id}`"
            logger.error(error_msg)
            raise BigQueryLoaderError(error_msg) from e
        
        return updated_rows

    
    def load_and_merge(
        self, 
        table_name: str, 
        df: pd.DataFrame, 
        key_column: str = "id", 
        is_full_sync: bool = False, 
        schema: Optional[BigQuerySchema] = None
    ) -> JobStats:
        """
        Full load pipeline:
        1. Loads data into a staging table.
        2. Ensures the destination table exists.
        3. Merges staging table into the destination table.
        4. Optionally marks deletions if full sync is enabled.
        
        Returns a dict with `loaded`, `merged`, and `deleted` row counts.
        """
        table_schema = schema or self.schemas.get(table_name)
        stg_table_name = self.get_staging_table_name(table_name)
        loaded, merged, deleted = 0, 0, 0
        
        if table_schema is None:
            error_msg = f"No schema found for '{table_name}'"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        try:
            # Load table into staging
            loaded = self.load(stg_table_name, df, schema=table_schema, write_mode="WRITE_TRUNCATE")
            
            # Ensure target table exist
            self.ensure_table_exists(table_name, table_schema)
        
            # Merge staging table into target table
            merged = self.merge(stg_table_name, table_name, df, key_column)
        
            if is_full_sync:
                # Upon full sync, mark deletions of records
                deleted = self.update(stg_table_name, table_name, df, key_column, update_action="mark_deletion")
        
        except Exception as e:
            error_msg = f"BigQueryError - failed during load and merge of '{table_name}'"
            logger.error(error_msg)
            raise BigQueryLoaderError(error_msg) from e
        
        job_stats: JobStats = {
            'loaded': loaded,
            'merged': merged,
            'deleted': deleted
        }
        return job_stats
