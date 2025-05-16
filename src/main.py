import pandas as pd
from datetime import datetime
from typing import cast, Dict, List, Any, get_args

from splash.config.settings import Settings
from splash.config.validate import load_env, check_secrets
from splash.defined_types import JobStats, JobStatusLiteral, DataSource, ETLUtil
from splash.metadata import ETLMetaData
from splash.utils.dict_utils import to_list
from splash.utils.string_utils import get_object_name
from splash.utils.job_utils import add_job_stats, get_etl_job_statuses, generate_job_status
from splash.utils.logger import setup_logger, zip_and_upload_logs, log_status_to_gcs, log_status_to_bigquery

from splash.extractor import EventExtractor, GroupContactExtractor
from splash.transformer import TransformerMap, EVENT_TRANSFORMER_MAP, GROUPCONTACT_TRANSFORMER_MAP
from splash.loader import BigQueryLoader

# Load environment and check required secrets
load_env()
check_secrets()

logger = setup_logger('main')

# Valid sources from defined Literal
ACCEPTED_SOURCES = get_args(DataSource)

# Mapping from source name to extractor and transformer config
ETL_UTILS: Dict[DataSource, ETLUtil] = {
    'event': {
        'extractor': EventExtractor,
        'transformer_map': EVENT_TRANSFORMER_MAP
    },
    'group_contact': {
        'extractor': GroupContactExtractor,
        'transformer_map': GROUPCONTACT_TRANSFORMER_MAP
    }
}


def add_system_defaults(df: pd.DataFrame, run_time: datetime) -> pd.DataFrame:
    """Adds internal system metadata fields to the DataFrame."""
    df['_sync_time'] = run_time
    df['_deleted'] = False
    return df


def transform_data(
    metadata: ETLMetaData, 
    data_dict: Dict[str, List[Dict[str, Any]]], 
    transformer_map: TransformerMap
) -> Dict[str, pd.DataFrame]:
    """
    Apply entity-specific transformations to raw data using transformer classes.
    Returns a dictionary of DataFrames keyed by object/entity name.
    """
    objects: Dict[str, pd.DataFrame] = {}
    
    for name, data in data_dict.items():
        transformers = to_list(transformer_map.get(name) or [])
        
        if not transformers:
            logger.info(f"No transformer defined for object '{name}' - Skip processing.")
            continue
            
        for tf in transformers:
            try:
                obj_name = get_object_name(tf)
                obj_df = tf(data).transform_to_df()
                
                if obj_df.empty:
                    continue
                    
                # Add system fields
                obj_df = add_system_defaults(obj_df, metadata.run_time)

                if obj_name in objects:
                    combined_df = pd.concat([objects.get(obj_name), obj_df], ignore_index=True)
                    combined_df.drop_duplicates(subset=['id'], keep='last', ignore_index=True, inplace=True)
                    objects[obj_name] = combined_df
                else:
                    objects[obj_name] = obj_df
                    
            except Exception as e:
                logger.error(f"Transformation failed for '{name}' using {tf.__name__ if hasattr(tf, '__name__') else str(tf)}: {e}")
                continue

    return objects


def load_and_merge_objects(
    metadata: ETLMetaData, 
    source: DataSource, 
    objects_dict: Dict[str, pd.DataFrame], 
    loader: BigQueryLoader
) -> JobStats:
    """
    Loads transformed DataFrames into staging tables and merges into final BigQuery tables.
    Accumulates and returns the job statistics for logging and audit.
    """
    agg_job_stats: JobStats = {'loaded': 0, 'merged': 0, 'deleted': 0}
    
    for obj_name, df in objects_dict.items():
        try:
            job_stats: JobStats = loader.load_and_merge(obj_name, df, key_column="id", is_full_sync=metadata.full_sync)
            status: JobStatusLiteral = "success"
        
        except Exception as e:
            logger.error(f"Load and merge failed for '{obj_name}' from '{source}' data: {e}")
            status = "failure"
            job_stats = {'loaded': 0, 'merged': 0, 'deleted': 0}
        
        finally:
            agg_job_stats = add_job_stats(agg_job_stats, job_stats)
            job_status = generate_job_status(metadata, source, obj_name, status, job_stats)
            get_etl_job_statuses().append(job_status)
    
    return agg_job_stats

        
def run_etl(
    metadata: ETLMetaData, 
    source: DataSource, 
    loader: BigQueryLoader
) -> None:
    """
    Runs the full ETL pipeline for a given data source:
    Extract -> Transform -> Load & Merge -> Log Status
    """
    etl_util = ETL_UTILS.get(source)
    
    if etl_util is None:
        error_msg = f"Unsupported data source: {source}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    extractor = etl_util['extractor']
    transformer_map = etl_util['transformer_map']
    job_stats: JobStats = {'loaded': 0, 'merged': 0, 'deleted': 0}
    
    try:
        logger.info(f"ETL started for '{source}' data")
        object_data = extractor(Settings.SYNC_MODE).extract()  # Extract
        objects = transform_data(metadata, object_data, transformer_map)  # Transform
        job_stats = load_and_merge_objects(metadata, source, objects, loader)  # Load and Merge
        logger.info(f"ETL completed for '{source}' data")
        status: JobStatusLiteral = 'success'
    
    except Exception as e:
        logger.error(f"ETL failed for '{source}' data: {e}")
        status = 'failure'
    
    finally:
        job_status = generate_job_status(metadata, source, 'all', status, job_stats)
        get_etl_job_statuses().append(job_status)
    
    return None


def main() -> None:
    """
    Entry point for Splash ETL process.
    Initializes metadata, validates data sources, runs ETL,
    and uploads logs to GCS/BigQuery if enabled.
    """
    logger.info("Starting Splash ETL...")
    metadata = ETLMetaData()
    logger.info(metadata.to_string())
    loader = BigQueryLoader()
    
    sources_str = Settings.SPLASH_ETL_SOURCES
    sources = sources_str.split(",")
    
    # Parse and validate sources
    for source in sources:
        if source not in ACCEPTED_SOURCES:
            raise ValueError(f"Invalid data source: {source}")
        
        run_etl(metadata, cast(DataSource, source), loader)
    
    job_statuses = get_etl_job_statuses()
    
    # Final logging and GCS upload
    if Settings.ENABLE_GCS_LOGS:
        zip_and_upload_logs(Settings.LOG_BUCKET, archive_name=metadata.log_path)
        log_status_to_gcs(Settings.LOG_BUCKET, job_statuses)
    
    if Settings.ENABLE_BQ_LOGS:
        log_status_to_bigquery(loader, job_statuses)
    
    logger.info("Finished Splash ETL...")

if __name__ == "__main__":
    main()
