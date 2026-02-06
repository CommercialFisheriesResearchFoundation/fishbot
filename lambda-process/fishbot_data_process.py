__author__ = 'Linus Stoltz | Data Manager, CFRF'
__project_team__ = 'Linus Stoltz, Sarah Salois, George Maynard, Mike Morin'
__doc__ = 'FIShBOT program to aggregate regional data into a standarzied daily grid'
__version__ = '1.1'

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
from utils.database_connector import DatabaseConnector
# import utils.spatial_tools as sp
from utils.netcdf_packing import pack_to_netcdf
from utils.s3_connector import S3Connector
import pandas as pd
import polars as pl
import os
import resource
import json
from datetime import datetime, timezone

DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST')
DB = os.getenv('DB')
DB_ARCHIVE_TABLE = os.getenv('DB_ARCHIVE_TABLE')
DB_EVENTS_TABLE = os.getenv('DB_EVENTS_TABLE')
DB_DIAG_TABLE = os.getenv('DB_DIAG_TABLE')
BUCKET_NAME = os.getenv('BUCKET_NAME')
AWS_REGION = os.getenv('REGION')
S3_PREFIX = os.getenv('PREFIX')
S3_ARCHIVE_PREFIX = os.getenv('ARCHIVE_PREFIX')

    
def aggregated_data(df,glider=False) -> pd.DataFrame:
    """ Function to aggregate the standardized data into fishbot format"""
    df.dropna(subset=['id'], inplace=True)
    df['time'] = pd.to_datetime(df['time'])
    df['id'] = df['id'].astype(int)
    df['date'] = df['time'].dt.date
    if not glider:
        try:
            agg_columns = {
                'temperature': ['mean', 'min', 'max', 'std', 'count'],
                'data_provider': 'unique',
                'id': 'first',
                'latitude': 'first',
                'longitude': 'first',
                'fishery_dependent': 'first',
                'stat_area': 'first',
                'depth': 'first' 
            }

            # Check if 'dissolved_oxygen' exists in the dataframe
            if 'dissolved_oxygen' in df.columns:
                agg_columns['dissolved_oxygen'] = [
                    'mean', 'min', 'max', 'std', 'count']

            # Check if 'salinity' exists in the dataframe
            if 'salinity' in df.columns:
                agg_columns['salinity'] = ['mean', 'min', 'max', 'std', 'count']

            df_aggregated = df.groupby(['date', 'id']).agg(
                agg_columns).reset_index()
        except Exception as e:
            logger.error("Error aggregating data: %s", e)
            return None
        df_aggregated.columns = [
            '_'.join(filter(None, col)).strip() if col[1] else col[0]
            for col in df_aggregated.columns.to_flat_index()
        ]

        df_aggregated.rename(columns={'date': 'time',
                                    'temperature_mean': 'temperature',
                                    'dissolved_oxygen_mean': 'dissolved_oxygen',
                                    'salinity_mean': 'salinity',
                                    'data_provider_unique': 'data_provider',
                                    'id_first': 'grid_id',
                                    'latitude_first': 'latitude',
                                    'longitude_first': 'longitude',
                                    'fishery_dependent_first':'fishery_dependent',
                                    'stat_area_first':'stat_area',
                                    'depth_first':'depth'}, inplace=True)

        df_aggregated['data_provider'] = df_aggregated['data_provider'].astype(
            str).str.replace(r"[\[\]']", "", regex=True)

        return df_aggregated
    try:
        agg_columns = {
            'temperature': ['mean', 'min', 'max', 'std'],
            'data_provider': 'unique',
            'id': 'first',
            'latitude': 'first',
            'longitude': 'first',
            'stat_area': 'first',
            'depth': 'first', 
            'temperature_count': 'sum',
            'dissolved_oxygen_count': 'sum',
            'salinity_count': 'sum'
        }
        # Check if 'dissolved_oxygen' exists in the dataframe
        if 'dissolved_oxygen' in df.columns:
            agg_columns['dissolved_oxygen'] = [
                'mean', 'min', 'max', 'std']

        # Check if 'salinity' exists in the dataframe
        if 'salinity' in df.columns:
            agg_columns['salinity'] = ['mean', 'min', 'max', 'std']

        df_aggregated = df.groupby(['date', 'id']).agg(
            agg_columns).reset_index()
        
    except Exception as e:
            logger.error("Error aggregating data: %s", e)
            return None
    df_aggregated.columns = [
        '_'.join(filter(None, col)).strip() if col[1] else col[0]
        for col in df_aggregated.columns.to_flat_index()
    ]

    df_aggregated.rename(columns={'date': 'time',
                                'temperature_mean': 'temperature',
                                'dissolved_oxygen_mean': 'dissolved_oxygen',
                                'salinity_mean': 'salinity',
                                'data_provider_unique': 'data_provider',
                                'id_first': 'grid_id',
                                'latitude_first': 'latitude',
                                'longitude_first': 'longitude',
                                'fishery_dependent_first':'fishery_dependent',
                                'stat_area_first':'stat_area',
                                'depth_first':'depth',
                                'temperature_count_sum': 'temperature_count',
                                'dissolved_oxygen_count_sum': 'dissolved_oxygen_count',
                                'salinity_count_sum': 'salinity_count'}, inplace=True)

    df_aggregated['data_provider'] = df_aggregated['data_provider'].astype(
        str).str.replace(r"[\[\]']", "", regex=True)

    return df_aggregated

def aggregated_data_pl(df: pl.DataFrame) -> pl.DataFrame:
    """Function to aggregate the standardized data into fishbot format"""
    
    # Drop rows with null id
    df = df.drop_nulls(subset=['id'])
    
    # Ensure proper types
    df = df.with_columns([
        pl.col("time").cast(pl.Datetime),
        pl.col("id").cast(pl.Int64),
    ])
    
    # Add date column
    df = df.with_columns(pl.col("time").dt.date().alias("date"))
    
    # Build aggregation expressions
    agg_exprs = [
        pl.col("temperature").mean().alias("temperature"),
        pl.col("temperature").min().alias("temperature_min"),
        pl.col("temperature").max().alias("temperature_max"),
        pl.col("temperature").std().alias("temperature_std"),
        pl.col("temperature").count().alias("temperature_count"),
        pl.col("data_provider").unique().alias("data_provider"),
        pl.col("id").first().alias("grid_id"),
        pl.col("latitude").first().alias("latitude"),
        pl.col("longitude").first().alias("longitude"),
        pl.col("fishery_dependent").first().alias("fishery_dependent"),
        pl.col("stat_area").first().alias("stat_area"),
        pl.col("depth").first().alias("depth"),
    ]
    
    # Add dissolved_oxygen aggregations if column exists
    if "dissolved_oxygen" in df.columns:
        agg_exprs.extend([
            pl.col("dissolved_oxygen").mean().alias("dissolved_oxygen"),
            pl.col("dissolved_oxygen").min().alias("dissolved_oxygen_min"),
            pl.col("dissolved_oxygen").max().alias("dissolved_oxygen_max"),
            pl.col("dissolved_oxygen").std().alias("dissolved_oxygen_std"),
            pl.col("dissolved_oxygen").count().alias("dissolved_oxygen_count"),
        ])
    
    # Add salinity aggregations if column exists
    if "salinity" in df.columns:
        agg_exprs.extend([
            pl.col("salinity").mean().alias("salinity"),
            pl.col("salinity").min().alias("salinity_min"),
            pl.col("salinity").max().alias("salinity_max"),
            pl.col("salinity").std().alias("salinity_std"),
            pl.col("salinity").count().alias("salinity_count"),
        ])
    
    # Perform aggregation
    df_aggregated = df.group_by(["date", "id"]).agg(agg_exprs)
    
    # Rename date to time
    df_aggregated = df_aggregated.rename({"date": "time"})
    
    # Convert data_provider list to string (remove brackets)
    df_aggregated = df_aggregated.with_columns(
        pl.col("data_provider").list.join(", ").alias("data_provider")
    )
    
    return df_aggregated
    
def consolidate_aggregated_pl(df: pl.DataFrame) -> pl.DataFrame:
    has_do = "dissolved_oxygen" in df.columns
    has_sal = "salinity" in df.columns

    # ---------- temperature ----------
    temp_aggs = [
        (pl.col("temperature") * pl.col("temperature_count")).sum().alias("t_wsum"),
        pl.col("temperature_count").sum().alias("temperature_count"),
        pl.col("temperature_min").min(),
        pl.col("temperature_max").max(),
        ((pl.col("temperature_count") - 1) * pl.col("temperature_std") ** 2).sum().alias("t_var1"),
        (pl.col("temperature") * pl.col("temperature_count")).sum().alias("t_wsum2"),
        (pl.col("temperature") ** 2 * pl.col("temperature_count")).sum().alias("t_var2"),
    ]

    # ---------- dissolved oxygen ----------
    do_aggs = []
    if has_do:
        do_aggs = [
            (pl.col("dissolved_oxygen") * pl.col("dissolved_oxygen_count")).sum().alias("do_wsum"),
            pl.col("dissolved_oxygen_count").sum().alias("dissolved_oxygen_count"),
            pl.col("dissolved_oxygen_min").min(),
            pl.col("dissolved_oxygen_max").max(),
            ((pl.col("dissolved_oxygen_count") - 1) * pl.col("dissolved_oxygen_std") ** 2).sum().alias("do_var1"),
            (pl.col("dissolved_oxygen") ** 2 * pl.col("dissolved_oxygen_count")).sum().alias("do_var2"),
        ]

    # ---------- salinity ----------
    sal_aggs = []
    if has_sal:
        sal_aggs = [
            (pl.col("salinity") * pl.col("salinity_count")).sum().alias("s_wsum"),
            pl.col("salinity_count").sum().alias("salinity_count"),
            pl.col("salinity_min").min(),
            pl.col("salinity_max").max(),
            ((pl.col("salinity_count") - 1) * pl.col("salinity_std") ** 2).sum().alias("s_var1"),
            (pl.col("salinity") ** 2 * pl.col("salinity_count")).sum().alias("s_var2"),
        ]

    out = (
        df
        .group_by(["time", "grid_id"])
        .agg(
            # providers
            pl.col("data_provider").unique().alias("data_provider"),

            # metadata
            pl.col("latitude").drop_nulls().first(),
            pl.col("longitude").drop_nulls().first(),
            pl.col("stat_area").drop_nulls().first(),
            pl.col("depth").drop_nulls().first(),

            # temperature
            *temp_aggs,
            *do_aggs,
            *sal_aggs,
        )
    )

    # ---------- finalize derived stats ----------
    out = out.with_columns(
        # temperature
        (pl.col("t_wsum") / pl.col("temperature_count")).alias("temperature"),
        pl.when(pl.col("temperature_count") > 1)
          .then(
              ((pl.col("t_var1") + pl.col("t_var2")
               - (pl.col("t_wsum") ** 2) / pl.col("temperature_count"))
               / (pl.col("temperature_count") - 1)).sqrt()
          )
          .otherwise(None)
          .alias("temperature_std"),

        # provider cleanup
        pl.col("data_provider")
          .cast(pl.Utf8)
          .str.replace_all(r"[\[\]']", "")
    )

    if has_do:
        out = out.with_columns(
            (pl.col("do_wsum") / pl.col("dissolved_oxygen_count")).alias("dissolved_oxygen"),
            pl.when(pl.col("dissolved_oxygen_count") > 1)
              .then(
                  ((pl.col("do_var1") + pl.col("do_var2")
                   - (pl.col("do_wsum") ** 2) / pl.col("dissolved_oxygen_count"))
                   / (pl.col("dissolved_oxygen_count") - 1)).sqrt()
              )
              .otherwise(None)
              .alias("dissolved_oxygen_std")
        )

    if has_sal:
        out = out.with_columns(
            (pl.col("s_wsum") / pl.col("salinity_count")).alias("salinity"),
            pl.when(pl.col("salinity_count") > 1)
              .then(
                  ((pl.col("s_var1") + pl.col("s_var2")
                   - (pl.col("s_wsum") ** 2) / pl.col("salinity_count"))
                   / (pl.col("salinity_count") - 1)).sqrt()
              )
              .otherwise(None)
              .alias("salinity_std")
        )

    return (
        out
        .sort(["time", "grid_id"])
        .drop([
            "t_wsum", "t_var1", "t_var2",
            "do_wsum", "do_var1", "do_var2",
            "s_wsum", "s_var1", "s_var2",
        ], strict=False)
    )

def lambda_handler(event, context):
    """ main function to call all subroutines"""
    logger.info("=============================")
    logger.info("FIShBOT Application started")
    
    # Record start time for diagnostics
    start_time = datetime.now(timezone.utc)
    current_time = start_time.isoformat()

    record = event['Records'][0]
    bucket = record['s3']['bucket']['name']
    s3 = S3Connector(bucket, AWS_REGION)
    intermediate_key = record['s3']['object']['key']
    prefix = os.path.dirname(intermediate_key)
    try:
        logger.info('accesing handoff data from S3')
        full_fleet, database_log, upstream_diagnostics = s3.get_handoff_data(intermediate_key)
        fishbot_ds = s3.get_fishbot_archive_dataset(prefix)
        reload_type = database_log[0].get('reload_type', None) # grab the reload type from the first entry in the log
        doi = database_log[0].get('doi', None) # grab the doi from the first entry in the log
        t_after_s3_retrieval = datetime.now(timezone.utc)
    except Exception as e:
        logger.error("Error accessing handoff data from S3: %s", e)
        raise

    with DatabaseConnector(DB_HOST, DB_USER, DB_PASS, DB) as db:
        db.insert_records(database_log, DB_EVENTS_TABLE)
    t_after_db_events = datetime.now(timezone.utc)

    try:
        logger.info('aggregating data to daily averages...')
        agg_df = aggregated_data_pl(full_fleet)
        agg_df = agg_df.to_pandas() # convert back to pandas for netcdf packing, as xarray doesn't yet support polars
        t_after_aggregation = datetime.now(timezone.utc)
    except Exception as e:
        logger.error("Error processing data: %s", e)
        raise
    logger.info('Data aggregation and metadata assingment complete.')
    logger.info('-----------------------------------------')
    logger.info('Packing data to NetCDF...')
    try:
        files = pack_to_netcdf(
            agg_df, s3, prefix=S3_PREFIX, version=__version__)
        t_after_netcdf = datetime.now(timezone.utc)
    except Exception as e:
        logger.error("Error packing data to NetCDF: %s", e)
        raise
    logger.info('NetCDF packing complete!')
    logger.info('created %s nc files', len(files))
    logger.info('-----------------------------------------')
    logger.info('Archiving fishbot_realtime')
    try:
        archive_file_size = s3.archive_fishbot(fishbot_ds, current_time,
                           version=__version__, prefix=S3_ARCHIVE_PREFIX, doi=doi)
        t_after_archive = datetime.now(timezone.utc)
        logger.info('Fishbot archive created successfully!')
        logger.info('-----------------------------------------')
    except Exception as e:
        logger.error("Error archiving fishbot: %s", e)
        raise
    try:
        logger.info("Pushing fishbot archive to S3")
        archive_key = s3.get_archive_key()
        public_url = s3.get_archive_url()
        logger.info("Archive key: %s", public_url)
        t_after_s3_push = datetime.now(timezone.utc)
    except Exception as e:
        logger.error("Error archiving fishbot: %s", e)
        raise

    logger.info('logging the archive in the database')
    try:
        with DatabaseConnector(DB_HOST, DB_USER, DB_PASS, DB) as db:
            logger.info("Logging archive to DB")
            if isinstance(doi, str):
                citation_url = f"https://zenodo.org/records/{doi.split('.')[-1]}"
            else:
                citation_url = None
            archive_dict = {
                "archive_s3_key": archive_key,
                "archive_public_url": public_url,
                "archive_date": current_time,
                "version": __version__,
                "doi": doi,
                "citation_url": citation_url,
                "reload_type": reload_type,
                "file_size_mb": archive_file_size
            }
            db.insert_records(archive_dict, DB_ARCHIVE_TABLE)
            logger.info("Archive logged to DB successfully")
    except Exception as e:
        logger.error("Error logging archive to DB: %s", e, exc_info=True)
        raise
    logger.info('-----------------------------------------')
    try:
        logger.info('Updating fishbot_archive dataset in S3')
        with DatabaseConnector(DB_HOST, DB_USER, DB_PASS, DB) as db:
            archive_df = db.update_archive_record(DB_ARCHIVE_TABLE)
        archive_file = '/tmp/fishbot_archive.csv'
        archive_df.to_csv(archive_file, index=False)
        s3_key = f'{S3_PREFIX}/fishbot_archive.csv'
        with open(archive_file, 'rb') as file_obj:
            s3.push_file_to_s3(file_obj, s3_key, content_type="text/csv")

        logger.info("Archive dataset %s pushed to S3", archive_file)
    except Exception as e:
        logger.error("Error updating fishbot archive dataset: %s", e, exc_info=True)
        raise

    # Calculate diagnostics
    end_time = datetime.now(timezone.utc)
    total_execution_seconds = (end_time - start_time).total_seconds()

    def _secs(a, b):
        try:
            return (b - a).total_seconds() if (a is not None and b is not None) else None
        except Exception:
            return None

    phase_durations = {
        's3_retrieval_seconds': _secs(start_time, t_after_s3_retrieval),
        'db_events_seconds': _secs(t_after_s3_retrieval, t_after_db_events),
        'aggregation_seconds': _secs(t_after_db_events, t_after_aggregation),
        'netcdf_packing_seconds': _secs(t_after_aggregation, t_after_netcdf),
        'archive_creation_seconds': _secs(t_after_netcdf, t_after_archive),
        's3_push_seconds': _secs(t_after_archive, t_after_s3_push),
        'total_elapsed_seconds': total_execution_seconds
    }

    try:
        mem_peak_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except Exception:
        mem_peak_kb = None

    diagnostics = {
        'runtime': current_time,
        'program': 'fishbot_data_process',
        'version': __version__,
        'total_execution_seconds': total_execution_seconds,
        'phase_durations': json.dumps(phase_durations),  # Serialize dict for DB storage
        'memory_peak_kb': mem_peak_kb,
        'pid': os.getpid(),
        'nc_files_created': len(files)
    }

    logger.info('Diagnostics: total execution (s)=%s, phases=%s', total_execution_seconds, phase_durations)

    try:
        with DatabaseConnector(DB_HOST, DB_USER, DB_PASS, DB) as db:
            # First, insert upstream diagnostics from the intermediate file
            if upstream_diagnostics:
                logger.info("Logging %d upstream diagnostics to DB", len(upstream_diagnostics))
                db.insert_records(upstream_diagnostics, DB_DIAG_TABLE)
            
            # Then insert this script's diagnostics
            logger.info("Logging process diagnostics to DB")
            db.insert_records(diagnostics, DB_DIAG_TABLE)
            logger.info("All diagnostics logged to DB successfully")
    except Exception as e:
        logger.error("Error logging diagnostics to DB: %s", e, exc_info=True)

    logger.info("Application complete!")
    logger.info("=============================")
