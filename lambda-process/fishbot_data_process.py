__author__ = 'Linus Stoltz | Data Manager, CFRF'
__project_team__ = 'Linus Stoltz, Sarah Salois, George Maynard, Mike Morin'
__doc__ = 'FIShBOT program to aggregate regional data into a standarzied daily grid'
__version__ = '0.9'

import logging
# import sys
# Configure logging before any other imports
logger = logging.getLogger()
logger.setLevel(logging.INFO)
# from utils.erddap_connector import ERDDAPClient
from utils.database_connector import DatabaseConnector
import utils.spatial_tools as sp
from utils.netcdf_packing import pack_to_netcdf
from utils.s3_connector import S3Connector
# import asyncio
import pandas as pd
# import requests
import os
from datetime import datetime, timezone
# from dateutil.relativedelta import relativedelta
# from scipy.signal import medfilt
# import gc

DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST')
DB = os.getenv('DB')
DB_ARCHIVE_TABLE = os.getenv('DB_ARCHIVE_TABLE')
DB_EVENTS_TABLE = os.getenv('DB_EVENTS_TABLE')
BUCKET_NAME = os.getenv('BUCKET_NAME')
AWS_REGION = os.getenv('REGION')
S3_PREFIX = os.getenv('PREFIX')
S3_ARCHIVE_PREFIX = os.getenv('ARCHIVE_PREFIX')

    
def aggregated_data(df) -> pd.DataFrame:
    """ Function to aggregate the standardized data into fishbot format"""
    df.dropna(subset=['id'], inplace=True)
    df['time'] = pd.to_datetime(df['time'])
    df['id'] = df['id'].astype(int)
    df['date'] = df['time'].dt.date

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


def lambda_handler(event, context):
    """ main function to call all subroutines"""
    logger.info("=============================")
    logger.info("FIShBOT Application started")
    current_time = datetime.now(timezone.utc).isoformat()

    record = event['Records'][0]
    bucket = record['s3']['bucket']['name']
    s3 = S3Connector(bucket, AWS_REGION)
    intermediate_key = record['s3']['object']['key']
    prefix = os.path.dirname(intermediate_key)
    try:
        logger.info('accesing handoff data from S3')
        full_fleet, database_log = s3.get_handoff_data(intermediate_key)
        fishbot_ds = s3.get_fishbot_archive_dataset(prefix)
        reload_type = database_log[0].get('reload_type', None) # grab the reload type from the first entry in the log
        doi = database_log[0].get('doi', None) # grab the doi from the first entry in the log

    except Exception as e:
        logger.error("Error accessing handoff data from S3: %s", e)
        raise

    with DatabaseConnector(DB_HOST, DB_USER, DB_PASS, DB) as db:
        db.log_data_events(database_log, DB_EVENTS_TABLE)

    try:
        # full_fleet = get_file_from_s3(BUCKET_NAME, AWS_REGION, S3_PREFIX)
        logger.info('aggregating data to daily averages...')
        agg_df = aggregated_data(full_fleet)

    except Exception as e:
        logger.error("Error processing data: %s", e)
        raise
    logger.info('Data aggregation and metadata assingment complete.')
    logger.info('-----------------------------------------')
    logger.info('Packing data to NetCDF...')
    try:
        files = pack_to_netcdf(
            agg_df, s3, prefix=S3_PREFIX, version=__version__)
        
    except Exception as e:
        logger.error("Error packing data to NetCDF: %s", e)
        raise
    logger.info('NetCDF packing complete!')
    logger.info('created %s nc files', len(files))
    logger.info('-----------------------------------------')
    logger.info('Archiving fishbot_realtime')
    try:
        archvie_file_size = s3.archive_fishbot(fishbot_ds, current_time,
                           version=__version__, prefix=S3_ARCHIVE_PREFIX, doi=doi)

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

    except Exception as e:
        logger.error("Error archiving fishbot: %s", e)
        raise

    logger.info('logging the archive in the database')
    try:
        with DatabaseConnector(DB_HOST, DB_USER, DB_PASS, DB) as db:
            logger.info("Logging archive to DB")
            archive_dict = {
                "archive_s3_key": archive_key,
                "archive_public_url": public_url,
                "archive_date": current_time,
                "version": __version__,
                "doi": doi,
                "citation_url": f"https://zenodo.org/records/{doi.split('.')[-1]}",
                "reload_type": reload_type,
                "file_size_mb": archvie_file_size
            }
            db.log_archive(archive_dict, DB_ARCHIVE_TABLE)
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

    logger.info("Application complete!")
    logger.info("=============================")
