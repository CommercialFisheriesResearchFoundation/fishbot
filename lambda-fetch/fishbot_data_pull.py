__author__ = 'Linus Stoltz | Data Manager, CFRF'
__project_team__ = 'Linus Stoltz, Sarah Salois, George Maynard, Mike Morin'
__doc__ = 'FIShBOT program Step 1: Fetches data from ERDDAP and local files, standardizes, and saves to S3.'
__version__ = '0.9.2'

import logging
import sys
logger = logging.getLogger()
logger.setLevel(logging.INFO)
from utils.erddap_connector import ERDDAPClient
import utils.spatial_tools as sp
from utils.zenodo_connector import ZenodoConnector
import asyncio
import pandas as pd
import requests
import os
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from scipy.signal import medfilt
import boto3
import sqlite3
import yaml

BUCKET_NAME = os.getenv('BUCKET_NAME')
AWS_REGION = os.getenv('REGION')
S3_PREFIX = os.getenv('PREFIX')
FULL_RELOAD_FLAG = os.getenv('FULL_RELOAD_FLAG', 'False').lower() == 'true'

def load_datasets():
   """ function to load the datasets.yaml file from github or local"""
   raw_url = os.getenv("DATASETS_URL",
        "https://raw.githubusercontent.com/CommercialFisheriesResearchFoundation/fishbot/refs/heads/main/config/datasets.yaml"
    )
   try:
        resp = requests.get(raw_url, timeout=5)
        resp.raise_for_status()
        return yaml.safe_load(resp.text)
   except Exception as e:
        logger.warning("Failed to load datasets.yaml from GitHub: %s", e)

   logger.info("Loading datasets.yaml from local file instead")
   with open('datasets.yaml', 'r') as f:
        # datasets file loaded to docker image as fall back, may not be current to github
        return yaml.safe_load(f)

def test_erddap_archive() -> bool:
    server = 'https://erddap.ondeckdata.com/erddap/'
    dataset_id = 'fishbot_realtime'
    url = f"{server}tabledap/{dataset_id}.html"
    try:
        response = requests.head(url, timeout=10)
        if response.status_code == 200:
            logger.info("ERDDAP dataset is reachable: %s", url)
            return True
        else:
            logger.warning("ERDDAP dataset is not reachable. Status code: %d", response.status_code)
            return False
    except requests.RequestException as e:
        logger.error("Error connecting to ERDDAP: %s", e)
        return False

def standardize_df(df, dataset_id) -> pd.DataFrame:
    """ Stanfardize the dataframes to a common format. Each dataset slightly different and some need a little pre processing"""
    keepers = ['time', 'latitude', 'longitude', 'temperature',
               'salinity', 'dissolved_oxygen', 'data_provider', 'fishery_dependent']
    logger.info('processing %s', dataset_id)
    if df.empty:
        logger.warning('No data returned for %s', dataset_id)
        return pd.DataFrame(columns=keepers)
    df.name = dataset_id

    logger.info('dataframe has %s rows and %s columns',
                df.shape[0], df.shape[1])
    if dataset_id == 'eMOLT_RT':
        try:
            # eMOLT data just bottom temperature
            df['time'] = pd.to_datetime(df['time'])
            filt = ['latitude', 'longitude', 'temperature']
            df = df.sort_values(['tow_id', 'time'])
            # Set index for efficient resampling - optimizer
            df = df.set_index(['tow_id', 'time'])
            # Resample and aggregate
            df_re = (
                df[filt]
                .groupby('tow_id')
                .resample('h', level='time')
                .mean()
                .reset_index()
            )

            df_re.loc[:, 'data_provider'] = 'eMOLT'
            df_re.loc[:, 'fishery_dependent'] = 1

            existing_columns = [col for col in keepers if col in df_re.columns]
            return df_re[existing_columns]
        except Exception as e:
            logger.error("error processing %s: %s", dataset_id, e)
            # return an empty dataframe if processing fails
            return pd.DataFrame(columns=keepers)

    elif dataset_id == 'eMOLT_RT_LOWELL':
        try:
            df['time'] = pd.to_datetime(df['time'])
            df.reset_index(inplace=True)
            df = df[df['DO'] > 0]

            grouped = df.groupby('tow_id')['DO']
            mean = grouped.transform('mean')
            std = grouped.transform('std')
            df['flag'] = (df['DO'] - mean).abs() > 3 * std
            df = df[~df['flag']]
            
            # find short tow_ids
            df = df[df.groupby('tow_id')['tow_id'].transform('count') >= 10]

            df['DO_filtered'] = df.groupby('tow_id')['DO'].transform(
                lambda x: medfilt(x, kernel_size=5))

            filt = ['latitude', 'longitude', 'temperature',
                    'DO_filtered']
            
            df = df.sort_values(['tow_id', 'time'])
            # Set index for efficient resampling - optimizer
            df = df.set_index(['tow_id', 'time'])

            # hourly average
            df_re = (
                df[filt]
                .groupby('tow_id')
                .resample('h', level='time')
                .mean()
                .reset_index()
            )
            # old slow way to aggreagate
            # df_re = df.groupby('tow_id')[filt].apply(
            #     lambda x: x.set_index('time').resample('h').mean().reset_index()).reset_index(drop=True)

            df_re.rename(
                columns={'DO_filtered': 'dissolved_oxygen'}, inplace=True)
            df_re = df_re[df_re['dissolved_oxygen'] > 0]
            df_re.loc[:, 'data_provider'] = 'eMOLT'
            df_re.loc[:, 'fishery_dependent'] = 1

            existing_columns = [col for col in keepers if col in df_re.columns]
            return df_re[existing_columns]
        except Exception as e:
            logger.error("error processing %s: %s",
                         dataset_id, e, exc_info=True)
            # return an empty dataframe if processing fails
            return pd.DataFrame(columns=keepers)
    elif dataset_id in ["shelf_fleet_profiles_1m_binned", "wind_farm_profiles_1m_binned"]:
        try:
            df = df.loc[df.groupby('profile_id')['sea_pressure'].idxmax()]
            df = df[['conservative_temperature', 'absolute_salinity',
                     'latitude', 'longitude', 'time']]
            df.rename(columns={'conservative_temperature': 'temperature',
                      'absolute_salinity': 'salinity'}, inplace=True)
            df.loc[:, 'data_provider'] = 'CFRF'
            if dataset_id == "shelf_fleet_profiles_1m_binned":
                df.loc[:, 'fishery_dependent'] = 1
            elif dataset_id == "wind_farm_profiles_1m_binned":
                df.loc[:, 'fishery_dependent'] = 0
            df['time'] = pd.to_datetime(df['time'])

            existing_columns = [col for col in keepers if col in df.columns]
            return df[existing_columns]
        except Exception as e:
            logger.error("error processing %s: %s", dataset_id, e)
            return pd.DataFrame(columns=keepers)

    elif dataset_id == 'fixed_gear_oceanography':
        try:

            df['time'] = pd.to_datetime(df['time'])
            df['time'] = df['time'].dt.tz_localize(None)
            
            grouped = df.groupby('tow_id')['dissolved_oxygen']
            mean = grouped.transform('mean')
            std = grouped.transform('std')
            df['flag'] = (df['dissolved_oxygen'] - mean).abs() > 3 * std
            df = df[~df['flag']]
            df.loc[:, 'data_provider'] = 'CFRF'
            df.loc[:, 'fishery_dependent'] = 1

            existing_columns = [col for col in keepers if col in df.columns]
            return df[existing_columns]
        except Exception as e:
            logger.error("error processing %s: %s", dataset_id, e)
            # return an empty dataframe if processing fails

            return pd.DataFrame(columns=keepers)

    elif dataset_id == 'ocdbs_v_erddap1':
        df.rename(columns={'UTC_DATETIME': 'time',
                           'sea_water_temperature': 'temperature',
                           'sea_water_salinity': 'salinity'}, inplace=True)
        df['profile_id'] = df['cruise_id'].astype(
            str) + '_' + df['cast_number'].astype(str)
        df_re = df.loc[df.groupby('profile_id')['pressure_dbars'].idxmax()]
        df_re.loc[:, 'data_provider'] = 'ECOMON'
        df_re.loc[:, 'fishery_dependent'] = 0
        existing_columns = [col for col in keepers if col in df_re.columns]
        return df_re[existing_columns]
    else:
        logger.warning('No processing identified for %s', dataset_id)
        return pd.DataFrame(columns=keepers)

def load_local_studyfleet(gdf_grid, last_runtime) -> pd.DataFrame:
    """ Side load the study fleet data from a local CSV file"""
    logger.info("Loading local studyfleet data")
    try:
        # need to change the location
        study_fleet = pd.read_csv(
            'data/local_data/study_fleet_data_processed.csv')
        study_fleet['time'] = pd.to_datetime(study_fleet['time'])

        study_fleet = study_fleet.merge(
            gdf_grid[['id', 'geometry','stat_area','depth']], on='id', how='left')

        study_fleet = study_fleet[study_fleet['time'] > pd.to_datetime(
            last_runtime).replace(tzinfo=None)]
        if study_fleet.empty:
            logger.info('No recent data in Study Fleet local')
            return pd.DataFrame()
        study_fleet.drop(columns=['geometry'], inplace=True)
    except Exception as e:
        logger.error("Error loading local studyfleet data: %s", e)
        raise
    logger.info("Local studyfleet data loaded")
    return study_fleet

def determine_reload_schedule() -> tuple:
    """Determine the reload schedule based on the current date and return query time"""
    if FULL_RELOAD_FLAG:
        logger.info("Full reload flag is set. Reloading all data.")
        return 'manual_full', '2000-01-01T00:00:00Z'
    try:
        today = datetime.now()
        # today = pd.to_datetime('2025-01-01T00:00:00Z')  # for testing
        # Annual reload
        if today.month == 1 and today.day == 1:
            logger.info("Annual reload scheduled")
            return 'full', '2000-01-01T00:00:00Z'

        # Quarterly reload
        quarterly_months = [4, 7, 10]
        if today.month in quarterly_months and today.day <= 1:
            logger.info("Quarterly reload scheduled")
            fetch_threshold = (today - relativedelta(years=1)
                            ).replace(hour=0, minute=0, second=0, microsecond=0)
            return 'quarterly', fetch_threshold.isoformat()

        # Bi-weekly reload
        if today.day in [1, 15]:
            logger.info("Bi-weekly reload scheduled")
            fetch_threshold = (today - relativedelta(months=1)
                            ).replace(hour=0, minute=0, second=0, microsecond=0)
            return 'bi-weekly', fetch_threshold.isoformat()

        # Daily reload
        fetch_threshold = (today - relativedelta(days=5)
                        ).replace(hour=0, minute=0, second=0, microsecond=0)
        logger.info("Daily reload scheduled")
        return 'daily', fetch_threshold.isoformat()

    except Exception as e:
        logger.error("Error determining reload schedule: %s", e)
        return None, None
    
def save_to_sqlite(df, database_log, db_name, doi) -> None:
    """
    Save the combined dataframe and database log to SQLite tables in one database.

    Args:
        df (pd.DataFrame): The combined dataframe to save.
        database_log (list): A list of dictionaries containing log information.
        db_name (str): The name of the SQLite database file.
    """
    df.dropna(subset=['id'], inplace=True)
    df = df.drop(columns=['geometry'])
    for col in df.columns:
        logger.info('column %s has type %s', col, df[col].dtype)
    try:
        # Connect to SQLite database
        conn = sqlite3.connect(db_name)

        # Save the combined dataframe to a table
        df.to_sql('aggregated_data', conn, if_exists='replace', index=False)
        logger.info('Combined dataframe saved to SQLite database at %s', db_name)

        # Save the database log to a separate table
        db_log_df = pd.DataFrame(database_log)
        db_log_df['doi'] = doi
        db_log_df.to_sql('database_log', conn, if_exists='replace', index=False)
        logger.info('Database log saved to SQLite database at %s', db_name)

    except Exception as e:
        logger.error('Error saving data to SQLite: %s', e)
    finally:
        # Close the database connection
        conn.close()

def push_to_s3(file_path, bucket_name, s3_key) -> bool:
    """
    Push a single file to an S3 bucket.

    Args:
        file_path (str): The local path to the file to upload.
        bucket_name (str): The name of the S3 bucket.
        s3_key (str): The key (path) in the S3 bucket where the file will be stored.

    Returns:
        bool: True if the upload was successful, False otherwise.
    """
    try:
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        s3_client.upload_file(file_path, bucket_name, s3_key)
        logger.info("File %s successfully uploaded to S3 bucket %s with key %s", file_path, bucket_name, s3_key)
        return True
    except Exception as e:
        logger.error("Error uploading file to S3: %s", e)
        return False

def lambda_handler(event, context):
    """ main function to call all subroutines"""
    logger.info("=============================")
    logger.info("FIShBOT Application started")
    DATASETS = load_datasets()
    if not test_erddap_archive():
        logger.error("ERDDAP dataset is not reachable. Exiting program.")
        sys.exit(1)
    logger.info('Determining Reload Schedule...')
    reload_type, query_time = determine_reload_schedule()

    current_time = datetime.now(timezone.utc).isoformat()
    logger.info('reload type: %s fetching data after %s',
                reload_type, query_time)

    logger.info('getting data from erddap...')
    dataset_count = sum(len(info["dataset_id"])
                        for info in DATASETS.values() if "dataset_id" in info)
    logger.info('Number of datasets being aggregated: %s', dataset_count)
    gdf_grid = sp.get_botgrid()
    dat = ERDDAPClient(DATASETS, query_time)
    results = asyncio.run(dat.fetch_all_data())

    logger.info('Data retrieval complete!')
    dataframes = []
    database_log = []  # list to collect dicts for logging to DB

    for data_provider, dataset_info in DATASETS.items():
        if data_provider == 'archive':
            logger.info('Fetching current fishbot_realtime for archive...')
            fishbot_ds = results.pop(0)
            if isinstance(fishbot_ds, Exception):
                logger.error("Error fetching fishbot archive: %s", fishbot_ds)
                sys.exit(1)

        elif data_provider == 'studyfleet':
            studyfleet = load_local_studyfleet(gdf_grid, query_time)
            database_log.append({
                "dataset_id": "studyfleet",
                "observation_count": len(studyfleet),
                "runtime": current_time,
                "version": __version__,
                "data_provider": data_provider,
                "file_protocol": "local",
                "reload_type": reload_type,
                "fetch_erddap_success": 0
            })
        else:
            for i, dataset_id in enumerate(dataset_info["dataset_id"]):
                result = results.pop(0)
                if isinstance(result, Exception):
                    logger.error("Error fetching %s: %s", dataset_id, result)
                elif isinstance(result, pd.DataFrame):
                    num_observations = len(result)
                    database_log.append({
                        "dataset_id": dataset_id,
                        "observation_count": num_observations,
                        "runtime": current_time,
                        "version": __version__,
                        "data_provider": data_provider,
                        "file_protocol": "erddap",
                        "reload_type": reload_type,
                        "fetch_erddap_success": 1
                    })

                    dataframes.append(standardize_df(result, dataset_id))

                else:
                    database_log.append({
                        "dataset_id": dataset_id,
                        "observation_count": "",
                        "runtime": current_time,
                        "version": __version__,
                        "data_provider": data_provider,
                        "file_protocol": "erddap",
                        "reload_type": reload_type,
                        "fetch_erddap_success": 0
                    })

    logger.info('Standardizing dataframes complete!')
    logger.info('-----------------------------------')

    if dataframes:
        try:
            # pop missing dataframes
            dataframes = [df for df in dataframes if not df.empty]
            combined_df = pd.concat(dataframes, ignore_index=True)

            if combined_df.empty and studyfleet.empty:  # ! remove this check once study fleet are on erddap
                logger.info('No data to process, exiting...')
                return
            logger.info('Concatenated %s dataframes into a single dataframe with %s rows and %s columns.',
                        len(dataframes), combined_df.shape[0], combined_df.shape[1])
            logger.info('concat columns: %s', combined_df.columns.tolist())
        except ValueError as e:
            logger.info('%s : exiting ...', e)
            return
    else:
        logger.info('No new data to process, exiting...')
        return
    
    standard_df = sp.gridify_df(combined_df, gdf_grid)
    logger.info('grid assiggment complete!')
    try:
        full_fleet = pd.concat([studyfleet, standard_df], ignore_index=True)
    except Exception as e:
        logger.warning('Could not concat with Study Fleet:%s', e)
        full_fleet = standard_df
    logger.info('-----------------------------------')
    tmp_file_nc = f"/tmp/fishbot_archive_{str(current_time).split('T')[0]}.nc"
    try:
        logger.info('Pushing archive to Zenodo...')
        fishbot_ds.to_netcdf(tmp_file_nc, mode='w')
        zc = ZenodoConnector(tmp_file_nc)
        zc.create_deposition()
        zc.upload_file()
        zc.add_metadata()
        zc.publish()
        doi = zc.get_doi()
        # doi = 'ipsum-doi-placeholder'  #! TESTING
        logger.info("Fishbot archive data successfully pushed to Zenodo with DOI: %s", doi)
    except Exception as e:
        logger.error("Error pushing archive to Zenodo: %s", e)
        logger.warning("Fishbot archive data not pushed to Zenodo, but will be uploaded to S3")
        doi = None
    logger.info('------------------------------------')
    # Save combined_df to a SQLite database
    tmp_file = '/tmp/fishbot_intermediate.db'
    try:
        save_to_sqlite(full_fleet, database_log, tmp_file, doi)
        logger.info('Combined dataframe saved to SQLite database at %s', tmp_file)
    except Exception as e:
        logger.error('Error saving data to SQLite: %s', e)
        raise e
    # Upload the SQLite database to S3
    s3_key = f"{S3_PREFIX}/fishbot_intermediate.db"

    if push_to_s3(tmp_file, BUCKET_NAME, s3_key):
        logger.info("SQLite database successfully uploaded to S3")
    else:
        logger.error("Failed to upload SQLite database to S3")
        raise Exception(f"Failed to upload {tmp_file} to {BUCKET_NAME}/{s3_key}")
    
    
    s3_archive_key = f"{S3_PREFIX}/fishbot_archive_intermediate.nc"
    try:
        
        if push_to_s3(tmp_file_nc, BUCKET_NAME, s3_archive_key):
            logger.info("Fishbot archive data successfully uploaded to S3")
    except Exception as e:
        logger.error("Failed to upload fishbot archive data to S3: %s", e)
        raise Exception(f"Failed to upload {tmp_file_nc} to {BUCKET_NAME}/{s3_archive_key}: {e}")

