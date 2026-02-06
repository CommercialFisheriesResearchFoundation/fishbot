__author__ = 'Linus Stoltz | Data Manager, CFRF'
__project_team__ = 'Linus Stoltz, Sarah Salois, George Maynard, Mike Morin'
__doc__ = 'FIShBOT program Step 1: Fetches data from ERDDAP and local files, standardizes, and saves to S3.'
__version__ = '1.1'

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
import json

BUCKET_NAME = os.getenv('BUCKET_NAME')
AWS_REGION = os.getenv('REGION')
S3_PREFIX = os.getenv('PREFIX')
FULL_RELOAD_FLAG = os.getenv('FULL_RELOAD_FLAG', 'False').lower() == 'true'
PUBLISH_DOI_FLAG = os.getenv('PUBLISH_DOI', 'True').lower() == 'true'
SEAFLOOR_THRESHOLD = int(os.getenv('SEAFLOOR_THRESHOLD', '20'))  # meters
logger.info("SEAFLOOR_THRESHOLD set to %d meters", SEAFLOOR_THRESHOLD)

def load_datasets():
    use_local = os.getenv("USE_LOCAL_YAML", "false").lower() == "true"
    if not use_local:
        try:
            raw_url = os.getenv(
                "DATASETS_URL",
                "https://raw.githubusercontent.com/CommercialFisheriesResearchFoundation/fishbot/refs/heads/main/config/datasets.yaml"
            )
            logger.info("Loading datasets.yaml from %s", raw_url)
            resp = requests.get(raw_url, timeout=5)
            resp.raise_for_status()
            return yaml.safe_load(resp.text)
        except Exception as e:
            logger.warning("Remote load failed: %s", e)

    logger.info("Loading datasets.yaml from local file")
    with open("datasets.yaml") as f:
        return yaml.safe_load(f)

def test_erddap_archive() -> bool:
    """ Test connectivity to the ERDDAP server. If fails, skip daily reload"""
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
    
def plot_filtered_data(depth_diff, dataset_id):
    """ helper dev function used to determine appropriate depth difference filtering thresholds"""
    import matplotlib.pyplot as plt

    thresholds = [1, 2, 5, 10, 20, 30, 50, 75, 100, 150, 200, 300, 400, 500]
    abs_diff = depth_diff.abs()
    total = len(abs_diff)

    results = []
    for t in thresholds:
        cnt = int((abs_diff <= t).sum())
        pct = cnt / total * 100.0
        results.append({'threshold_m': t, 'count': cnt, 'percent': pct})

    results_df = pd.DataFrame(results)
    fig, ax = plt.subplots(figsize=(10, 4.5))
    ax.bar(results_df['threshold_m'].astype(str), results_df['percent'], color='C1', alpha=0.8)
    ax.set_ylim(0, 100)
    ax.set_ylabel('Percent of data retained (%)')
    ax.set_xlabel('Absolute difference (GEBCO v pressure sensor) (m)')
    ax.set_title(f'Percent of observations retained after filter {dataset_id}')

    for i, pct in enumerate(results_df['percent']):
        ax.text(i, min(pct - 5, pct * 0.9), f"{pct:.1f}%", ha='center', va='top', fontsize=10, color='black')

    plt.tight_layout()
    plt.savefig(f"/tmp/depth_difference_filter_{dataset_id}.png", dpi=300)

def standardize_df(df, dataset_id, gdf_grid=None) -> pd.DataFrame:
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
            filt = ['latitude', 'longitude', 'temperature', 'depth']
            df = df.sort_values(['tow_id', 'time'])
            # Set index for efficient resampling
            df = df.set_index(['tow_id', 'time'])
            # Resample and aggregate
            df_re = (
                df[filt]
                .groupby('tow_id')
                .resample('h', level='time')
                .mean()
                .dropna(how='all')
                .reset_index()
            )
            df_re = sp.find_closest_depth(df_re)
            # depth_diff = df_re['inferred_depth'] - df_re['depth']
            # plot_filtered_data(depth_diff, dataset_id)
            # df_re.to_csv('/tmp/emolt_with_depth.csv')
            # df_before_filter = len(df_re)
            df_re = df_re[df_re['depth'].notna() & df_re['inferred_depth'].notna() &
                    ((df_re['depth'] - df_re['inferred_depth']) < SEAFLOOR_THRESHOLD)]
            # df_after_filter = len(df_re)
            # records_filtered = df_before_filter - df_after_filter
            # logger.info('%s: Filtered out %d records (depth check). Before: %d, After: %d',
            # dataset_id, records_filtered, df_before_filter, df_after_filter)
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
            # logger.info('Before resample: %d rows', len(df))
            # logger.info('Unique tow_ids: %d', df.index.get_level_values('tow_id').nunique())
            # logger.info('Time range per tow (sample): %s', 
            # df.groupby(level='tow_id')['temperature'].count().describe())
            # hourly average
            df_re = (
                df[filt]
                .groupby('tow_id')
                .resample('h', level='time')
                .mean()
                .dropna(how='all')
                .reset_index()
            )

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
            df = sp.find_closest_depth(df)
            # depth_diff = df['inferred_depth'] - df['sea_pressure']
            # plot_filtered_data(depth_diff, dataset_id)

            # Track filtering
            # df_before_filter = len(df)
            df = df[df['sea_pressure'].notna() & df['inferred_depth'].notna() &
                    ((df['sea_pressure'] - df['inferred_depth']) < SEAFLOOR_THRESHOLD)]
            # df_after_filter = len(df)
            # records_filtered = df_before_filter - df_after_filter
            # logger.info('%s: Filtered out %d records (depth check). Before: %d, After: %d', 
            # dataset_id, records_filtered, df_before_filter, df_after_filter)

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
        
    elif dataset_id == 'wind_farm_acoustic_receivers':
        try:
            df['time'] = pd.to_datetime(df['time'])
            df.loc[:, 'data_provider'] = 'CFRF'
            df.loc[:, 'fishery_dependent'] = 0

            existing_columns = [col for col in keepers if col in df.columns]
            return df[existing_columns]
        except Exception as e:
            logger.error("error processing %s: %s", dataset_id, e)
            # return an empty dataframe if processing fails
            return pd.DataFrame(columns=keepers)
    
    elif dataset_id =='oleanderXbt':
        df.rename(columns={'temp': 'temperature'}, inplace=True)
        df['time'] = pd.to_datetime(df['time'])
        df['profile_id'] = df['cruise_num'].astype(str) + '_' + df['profile_number'].astype(str)
        df = df.loc[df.groupby('profile_id')['depth'].idxmax()]
        df = sp.find_closest_depth(df)
        # depth_diff = df['inferred_depth'] - df['depth']
        # plot_filtered_data(depth_diff, dataset_id)
        df = df[df['depth'].notna() & df['inferred_depth'].notna() &
                      ((df['depth'] - df['inferred_depth']) < SEAFLOOR_THRESHOLD)]
        df.loc[:, 'data_provider'] = 'Oleander'
        df.loc[:, 'fishery_dependent'] = 0
        existing_columns = [col for col in keepers if col in df.columns]
        return df[existing_columns]
    
    elif dataset_id == 'ocdbs_v_erddap1':
        df.rename(columns={'UTC_DATETIME': 'time',
                           'sea_water_temperature': 'temperature',
                           'sea_water_salinity': 'salinity','pressure_dbars':'sea_pressure'}, inplace=True)
        df['profile_id'] = df['cruise_id'].astype(str) + '_' + df['cast_number'].astype(str)
        df = df[(df["dissolved_oxygen"].isna()) | (df["dissolved_oxygen"] > 0)]
        df_re = df.loc[df.groupby('profile_id')['sea_pressure'].idxmax()]
        df_re = sp.find_closest_depth(df_re)
        # depth_diff = df_re['inferred_depth'] - df_re['sea_pressure']
        # plot_filtered_data(depth_diff, dataset_id)
        # df_before_filter = len(df_re)
        df_re = df_re[df_re['sea_pressure'].notna() & df_re['inferred_depth'].notna() &
                ((df_re['sea_pressure'] - df_re['inferred_depth']) < SEAFLOOR_THRESHOLD)]
        # df_after_filter = len(df_re)
        # records_filtered = df_before_filter - df_after_filter
        # logger.info('%s: Filtered out %d records (depth check). Before: %d, After: %d', 
        # dataset_id, records_filtered, df_before_filter, df_after_filter)
        df_re.loc[:, 'data_provider'] = 'ECOMON'
        df_re.loc[:, 'fishery_dependent'] = 0
        existing_columns = [col for col in keepers if col in df_re.columns]
        return df_re[existing_columns]
    
    elif dataset_id.startswith('ooi-'):
        try:
            df['dissolved_oxygen'] = df['mole_concentration_of_dissolved_molecular_oxygen_in_sea_water'] * 0.0328 # convert from mol/kg to mg/L
            df.rename(columns={
                'sea_water_temperature': 'temperature',
                'sea_water_practical_salinity': 'salinity'
            }, inplace=True)
            df['time'] = pd.to_datetime(df['time'])
            df['time_hour'] = df['time'].dt.floor('h')
            df = df.groupby('time_hour').mean().reset_index()
            df.drop(columns=['time'], inplace=True)
            df.rename(columns={'time_hour': 'time'}, inplace=True)
            df.loc[:, 'data_provider'] = 'OOI'
            df.loc[:, 'fishery_dependent'] = 0

            existing_columns = [col for col in keepers if col in df.columns]
            return df[existing_columns]
        except Exception as e:
            logger.error("error processing %s: %s", dataset_id, e)
            # return an empty dataframe if processing fails
            return pd.DataFrame(columns=keepers)
    elif dataset_id == 'demersal_gliders_aggregated':
        institution_map = {
            "Graduate School of Oceanography, University of Rhode Island": 'glider_URI',
            "Woods Hole Oceanographic Institution": 'glider_WHOI',
            "University of Massachusetts Darmouth": 'glider_UMass',
            "University of Massachusetts Dartmouth": 'glider_UMass',
            "University of Massachussetts Dartmouth, Rutgers University": 'glider_UMass',
            "Virginia Institute of Marine Science - William & Mary": 'glider_VIMS',
            "Virginia Institute of Marine Science \u2013 William & Mary": 'glider_VIMS',
            "Virginia Institute of Marine Science": 'glider_VIMS',
            "University of Maryland": 'glider_UMD',
            "University of Delaware College of Earth Ocean and Environment": 'glider_UDel',
            "University of South Florida College of Marine Science Ocean Technology Group": 'glider_USF',
            "University of Maine,Rutgers University": 'glider_UMaine',
            "University of Maine": 'glider_UMaine',
            "University of Massachusetts Dartmouth,Rutgers University": 'glider_UMass',
            "Virginia Institute of Marine Science - The College of William & Mary": 'glider_VIMS',
            "Helmholtz-Zentrum hereon": 'glider_HZ',
            "Integrated Ocean Observing System,Naval Oceanographic Office,Rutgers University,National Oceanic and Atmospheric Administration,Mid-Atlantic Regional Association of Coastal Ocean Observing Systems,Rutgers University": 'glider_IOOS',
            "Naval Oceanographic Office": 'glider_Navy',
            "NAVOCEANO,Rutgers University": 'glider_NAVOCEANO',
            "NAVOCEANO,University of Delaware": 'glider_NAVOCEANO',
            "NAVOCEANO,William & Mary Virginia Institute of Marine Science": 'glider_NAVOCEANO',
            "OOI Coastal & Global Scale Nodes (CGSN)": 'glider_OOI',
            "Rutgers University": 'glider_RU',
            "Rutgers University,Virginia Institute of Marine Science": 'glider_RU',
            "Skidaway Institute of Oceanography": 'glider_UGA',
            "Stony Brook University": 'glider_SBU',
            "Stony Brook University School of Marine & Atmospheric Sciences": 'glider_SBU',
            "Stony Brook University,Rutgers University": 'glider_SBU',
            "Teledyne Webb Research Corporation": 'glider_RU',
            "Teledyne Webb Research, Rutgers University, PLOCAN": 'glider_RU',
            "UNC Marine Sciences": 'glider_UNC',
            "University of Connecticut": 'glider_UConn',
            "University of Delaware": 'glider_UDel',
        }
        try:
            df['data_provider'] = df['institution'].map(institution_map).fillna('glider_other')
            df = df[(df["dissolved_oxygen"].isna()) | (df["dissolved_oxygen"] > 0)]
            df.rename(columns={'grid_id': 'id'}, inplace=True)
            df = df.merge(
                gdf_grid[['id', 'geometry', 'stat_area', 'depth', 'centroid_lon', 'centroid_lat']],
                on='id', how='left')
            df = df.drop(columns=['institution'])
            df.rename(columns={'centroid_lon': 'longitude', 'centroid_lat': 'latitude'}, inplace=True)
            df['fishery_dependent'] = 0
            existing_columns = [col for col in keepers if col in df.columns]
            return df[existing_columns]
        except Exception as e:
            logger.error("error processing %s: %s", dataset_id, e)
            # return an empty dataframe if processing fails
            return pd.DataFrame(columns=keepers)
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
        gdf_grid[['id', 'geometry','stat_area','depth','centroid_lon','centroid_lat']], on='id', how='left')
        # study_fleet = study_fleet.drop(columns=['latitude', 'longitude'])
        study_fleet.rename(columns={'centroid_lon': 'longitude', 'centroid_lat': 'latitude'}, inplace=True)

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

def load_local_gmgi(gdf_grid, last_runtime) -> pd.DataFrame:
    """ Side load the GMGI data from a local CSV file"""
    logger.info("Loading local GMGI data")
    try:
        # need to change the location
        gmgi = pd.read_csv(
            'data/local_data/gmgi_sbnms_processed.csv')
        gmgi['time'] = pd.to_datetime(gmgi['time'])

        gmgi = gmgi.merge(
        gdf_grid[['id', 'geometry','stat_area','depth','centroid_lon','centroid_lat']], on='id', how='left')
        gmgi = gmgi.drop(columns=['latitude', 'longitude'])
        gmgi.rename(columns={'centroid_lon': 'longitude', 'centroid_lat': 'latitude'}, inplace=True)

        gmgi = gmgi[gmgi['time'] > pd.to_datetime(
            last_runtime).replace(tzinfo=None)]
        if gmgi.empty:
            logger.info('No recent data in GMGI local')
            return pd.DataFrame()
        gmgi.drop(columns=['geometry'], inplace=True)
    except Exception as e:
        logger.error("Error loading local GMGI data: %s", e)
        raise
    logger.info("Local GMGI data loaded")
    return gmgi

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
    
def save_to_sqlite(df, database_log, db_name, doi, diagnostics=None) -> None:
    """
    Save the combined dataframe and database log to SQLite tables in one database.

    Args:
        df (pd.DataFrame): The combined dataframe to save.
        database_log (list): A list of dictionaries containing log information.
        db_name (str): The name of the SQLite database file.
        diagnostics (dict, optional): Diagnostic information (timings, memory, cpu) to save to a separate table.
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

        # Save diagnostics to a separate table if provided
        if diagnostics is not None:
            try:
                # Ensure diagnostics is a flat record; convert nested dicts to strings for storage
                diag_record = diagnostics.copy()
                if 'phase_durations' in diag_record:
                    diag_record['phase_durations'] = str(diag_record['phase_durations'])
                diag_df = pd.DataFrame([diag_record])
                diag_df.to_sql('diagnostics', conn, if_exists='replace', index=False)
                logger.info('Diagnostics saved to SQLite database at %s', db_name)
            except Exception as e:
                logger.error('Error saving diagnostics to SQLite: %s', e)
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
    # Record start time to calculate total execution duration
    start_time = datetime.now(timezone.utc)
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
    # Timestamp: after data retrieval
    t_after_retrieval = datetime.now(timezone.utc)

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

        #* -------------------------------------------
        #* LOADING SIDE-LOADED DATA
        #* -------------------------------------------
        elif data_provider == 'studyfleet':
            studyfleet = load_local_studyfleet(gdf_grid, query_time)
            database_log.append({
                "dataset_id": "studyfleet",
                "observation_count": len(studyfleet),
                "post_filter_count": len(studyfleet),
                "runtime": current_time,
                "version": __version__,
                "data_provider": data_provider,
                "file_protocol": "local",
                "reload_type": reload_type,
                "fetch_erddap_success": 0
            })
        elif data_provider == 'gmgi':
            gmgi = load_local_gmgi(gdf_grid, query_time)
            database_log.append({
                "dataset_id": "gmgi",
                "observation_count": len(gmgi),
                "post_filter_count": len(gmgi),
                "runtime": current_time,
                "version": __version__,
                "data_provider": data_provider,
                "file_protocol": "local",
                "reload_type": reload_type,
                "fetch_erddap_success": 0
            })
        #* -------------------------------------------
        #* LOADING ERDDAP FETCHED DATA
        #* ------------------------------------------- 

        else:
            for i, dataset_id in enumerate(dataset_info["dataset_id"]):
                result = results.pop(0)
                if isinstance(result, Exception):
                    logger.error("Error fetching %s: %s", dataset_id, result)
                elif isinstance(result, pd.DataFrame):
                    num_observations = len(result)
                    post_df = standardize_df(result, dataset_id, gdf_grid)
                    logger.info('dataframe has %s rows and %s columns post standardization',
                                post_df.shape[0], post_df.shape[1])
                    dataframes.append(post_df)
                    database_log.append({
                        "dataset_id": dataset_id,
                        "observation_count": num_observations,
                        "post_filter_count": len(post_df),
                        "runtime": current_time,
                        "version": __version__,
                        "data_provider": data_provider,
                        "file_protocol": "erddap",
                        "reload_type": reload_type,
                        "fetch_erddap_success": 1
                    })

                else:
                    database_log.append({
                        "dataset_id": dataset_id,
                        "observation_count": 0,
                        "post_filter_count": 0,
                        "runtime": current_time,
                        "version": __version__,
                        "data_provider": data_provider,
                        "file_protocol": "erddap",
                        "reload_type": reload_type,
                        "fetch_erddap_success": 0
                    })

    logger.info('Standardizing dataframes complete!')
    # Timestamp: after standardization
    t_after_standardize = datetime.now(timezone.utc)
    logger.info('-----------------------------------')

    if dataframes:
        try:
            # pop missing dataframes
            dataframes = [df for df in dataframes if not df.empty]
            combined_df = pd.concat(dataframes, ignore_index=True)

            if combined_df.empty and studyfleet.empty and gmgi.empty:  # ! remove this check once study fleet are on erddap
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
    # Timestamp: after grid assignment
    t_after_gridify = datetime.now(timezone.utc)
    try:
        full_fleet = pd.concat([gmgi, studyfleet, standard_df], ignore_index=True)
    except Exception as e:
        logger.warning('Could not concat with Study Fleet:%s', e)
        full_fleet = standard_df
    # Timestamp: after concatenation
    t_after_concat = datetime.now(timezone.utc)
    logger.info('-----------------------------------')
    tmp_file_nc = f"/tmp/fishbot_archive_{str(current_time).split('T')[0]}.nc"
    try:
        if PUBLISH_DOI_FLAG:
            logger.info('Pushing archive to Zenodo...')
            fishbot_ds.to_netcdf(tmp_file_nc, mode='w')
            zc = ZenodoConnector(tmp_file_nc)
            zc.create_deposition()
            zc.upload_file()
            zc.add_metadata()
            zc.publish()
            doi = zc.get_doi()
            logger.info("Fishbot archive data successfully pushed to Zenodo with DOI: %s", doi)
        else:
            logger.info('PUBLISH_DOI_FLAG is not set. Skipping push to Zenodo, but will upload to S3')
            doi = None
    except Exception as e:
        logger.error("Error pushing archive to Zenodo: %s", e)
        logger.warning("Fishbot archive data not pushed to Zenodo, but will be uploaded to S3")
        doi = None
    # Sending to s3 before logging diagnostics
    s3_archive_key = f"{S3_PREFIX}/fishbot_archive_intermediate.nc"
    try:
        
        if push_to_s3(tmp_file_nc, BUCKET_NAME, s3_archive_key):
            logger.info("Fishbot archive data successfully uploaded to S3")
    except Exception as e:
        logger.error("Failed to upload fishbot archive data to S3: %s", e)
        raise Exception("Failed to upload %s to %s: %s" % (tmp_file_nc, BUCKET_NAME, s3_archive_key, e))

    t_after_zenodo = datetime.now(timezone.utc)
    logger.info('------------------------------------')
    # Save combined_df to a SQLite database
    tmp_file = '/tmp/fishbot_intermediate.db'

    end_time = datetime.now(timezone.utc)
    total_execution_seconds = (end_time - start_time).total_seconds()

    def _secs(a, b):
        try:
            return (b - a).total_seconds() if (a is not None and b is not None) else None
        except Exception:
            return None

    phase_durations = {
        'data_retrieval_seconds': _secs(start_time, locals().get('t_after_retrieval')),
        'standardize_seconds': _secs(locals().get('t_after_retrieval'), locals().get('t_after_standardize')),
        'gridify_seconds': _secs(locals().get('t_after_standardize'), locals().get('t_after_gridify')),
        'concat_seconds': _secs(locals().get('t_after_gridify'), locals().get('t_after_concat')),
        'zenodo_seconds': _secs(locals().get('t_after_concat'), locals().get('t_after_zenodo')),
        'total_elapsed_seconds': total_execution_seconds
    }

    try:
        import resource
        mem_peak_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except Exception:
        mem_peak_kb = None

    diagnostics = {
        'runtime': current_time,
        'program': 'fishbot_data_pull',
        'version': __version__,
        'total_execution_seconds': total_execution_seconds,
        'phase_durations': json.dumps(phase_durations),
        'memory_peak_kb': mem_peak_kb,
        'pid': os.getpid()
    }

    # attach total execution to each dataset log entry for backwards compatibility
    for entry in database_log:
        entry['execution_time_seconds'] = total_execution_seconds

    logger.info('Diagnostics: total execution (s)=%s, phases=%s', total_execution_seconds, phase_durations)

    try:
        save_to_sqlite(full_fleet, database_log, tmp_file, doi, diagnostics)
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
        raise Exception("Failed to upload %s to %s: %s" % (tmp_file, BUCKET_NAME, s3_key))
    
    

