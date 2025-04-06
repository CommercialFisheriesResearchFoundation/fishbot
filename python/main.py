__author__ = 'Linus Stoltz | Data Manager, CFRF'
__project_team__ = 'Linus Stoltz, Sarah Salois, George Maynard, Mike Morin'
__doc__ = 'FIShBOT program to aggregate regional data collected by NOAA and CFRF'
__version__ = '0.4'

import logging
from logging.handlers import TimedRotatingFileHandler
from utils.erddap_connector import ERDDAPClient
from utils.database_connector import DatabaseConnector
import utils.spatial_tools as sp
from utils.netcdf_packing import pack_to_netcdf
from utils.s3_connector import S3Connector
import asyncio
import pandas as pd
from dotenv import load_dotenv
import os
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from scipy.signal import medfilt
import gc
import argparse
import sys
from utils.file_tools import move_files, reload_erddap, test_erddap_archive

# TRADITIONAL DEPLOYMENT LOGGING
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = TimedRotatingFileHandler(
    'fishbot.log', when='midnight', interval=1, backupCount=7)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
handler.setFormatter(formatter)
logging.basicConfig(level=logging.INFO, handlers=[handler])

# load the env if local deployment
load_dotenv()
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST')
DB = os.getenv('DB')
DB_TABLE = os.getenv('DB_TABLE')
DB_ARCHIVE_TABLE = os.getenv('DB_ARCHIVE_TABLE')
DB_EVENTS_TABLE = os.getenv('DB_EVENTS_TABLE')
BUCKET_NAME = os.getenv('BUCKET_NAME')
AWS_REGION = os.getenv('REGION')
AWS_PROFILE = os.getenv('PROFILE')
ERDDAP_DATA_PATH = os.getenv('ERDDAP_DATA_PATH')

DATASETS = {
    "cfrf": {
        "server": "https://erddap.ondeckdata.com/erddap/",
        "dataset_id": ["fixed_gear_oceanography", "shelf_fleet_profiles_1m_binned", "wind_farm_profiles_1m_binned"],
        "protocol": ["tabledap", "tabledap", "tabledap"],
        "response": ["nc", "nc", "nc"],
    },
    "emolt": {
        "server": "https://erddap.emolt.net/erddap/",
        "dataset_id": ["eMOLT_RT", "eMOLT_RT_LOWELL"],
        "protocol": ["tabledap", "tabledap"],
        "response": ["csv", "nc"],
        "constraints": {
            "eMOLT_RT": {
                "segment_type=": "Fishing"
            }
        }
    },
    # "emolt": {
    #     "server": "https://erddap.emolt.net/erddap/",
    #     "dataset_id": ["eMOLT_RT_LOWELL"],
    #     "protocol": ["tabledap"],
    #     "response": ["nc"],
    #     "constraints": {
    #         "eMOLT_RT": {
    #             "segment_type=": "Fishing"
    #         }
    #     }
    # },
    "studyfleet": {
        "server": "",
        "dataset_id": [],  # This will be loaded locally from a CSV file
        "protocol": [],
        "response": [],
    }
}


def aggregated_data(df) -> pd.DataFrame:
    """ Function to aggregate the standardized data into fishbot format"""
    df.dropna(subset=['id'], inplace=True)
    df['id'] = df['id'].astype(int)
    df['date'] = df['time'].dt.date

    try:
        agg_columns = {
            'temperature': ['mean', 'min', 'max', 'std', 'count'],
            'data_provider': 'unique',
            'id': 'first',
            'geometry': 'first'
        }

        # Check if 'dissolved_oxygen' exists in the dataframe
        if 'dissolved_oxygen' in df.columns:
            agg_columns['dissolved_oxygen'] = ['mean', 'min', 'max', 'std', 'count']

        # Check if 'salinity' exists in the dataframe
        if 'salinity' in df.columns:
            agg_columns['salinity'] = ['mean', 'min', 'max', 'std', 'count']

        df_aggregated = df.groupby(['date', 'centroid']).agg(agg_columns).reset_index()
    except Exception as e:
        logger.error("Error aggregating data: %s", e)
        return None
    df_aggregated.columns = [
        '_'.join(filter(None, col)).strip() if col[1] else col[0]
        for col in df_aggregated.columns.to_flat_index()
    ]
    df_aggregated['latitude'] = df_aggregated['centroid'].apply(
        lambda point: point.y)
    df_aggregated['longitude'] = df_aggregated['centroid'].apply(
        lambda point: point.x)
    df_aggregated = df_aggregated.drop(columns=['centroid'])
    df_aggregated.rename(columns={'date': 'time',
                                  'temperature_mean': 'temperature',
                                  'dissolved_oxygen_mean': 'dissolved_oxygen',
                                  'salinity_mean': 'salinity',
                                  'data_provider_unique': 'data_provider',
                                  'id_first': 'grid_id',
                                  'geometry_first': 'geometry'}, inplace=True)

    df_aggregated['data_provider'] = df_aggregated['data_provider'].astype(
        str).str.replace(r"[\[\]']", "", regex=True)
    
    return df_aggregated

def standardize_df(df, dataset_id) -> pd.DataFrame:
    """ Stanfardize the dataframes to a common format. Each dataset slightly different and some need a little pre processing"""
    keepers = ['time', 'latitude', 'longitude', 'temperature',
               'salinity', 'dissolved_oxygen', 'data_provider']
    logger.info('processing %s', dataset_id)
    if df.empty:
        logger.warning('No data returned for %s', dataset_id)
        return pd.DataFrame(columns=keepers)

    logger.info('dataframe has %s rows and %s columns',
                df.shape[0], df.shape[1])
    if dataset_id == 'eMOLT_RT':
        try:
            df.to_csv(f'raw_{dataset_id}.csv', index=False)
            print(f"{dataset_id} pre filter: {len(df)}")
            # eMOLT data just bottom temperature
            df['time'] = pd.to_datetime(df['time'])
            filt = ['time', 'latitude', 'longitude', 'temperature']

            df_re = df.groupby('tow_id')[filt].apply(
                lambda x: x.set_index('time').resample('h').mean().reset_index()).reset_index(drop=True)
            
            df_re.loc[:,'data_provider'] = 'eMOLT'
            print(f"{dataset_id} post filter: {len(df_re)}")
            df_re.to_csv(f'process_test_{dataset_id}.csv', index=False)
            existing_columns = [col for col in keepers if col in df_re.columns]
            return df_re[existing_columns]
        except Exception as e:
            logger.error("error processing %s: %s", dataset_id, e)
            return pd.DataFrame(columns=keepers)  # return an empty dataframe if processing fails

    elif dataset_id == 'eMOLT_RT_LOWELL':
        # Save raw data for debugging
        try:
            df.to_csv(f'raw_{dataset_id}.csv', index=False)
            print(f"{dataset_id} pre filter: {len(df)}")
            df['time'] = pd.to_datetime(df['time'])
            df.reset_index(inplace=True)
            df = df[(df['water_detect_perc'] > 60) & 
                    (df['DO'] > 0) & 
                    (df['temperature'].between(0, 27))]

            df['flag'] = df.groupby('tow_id')['DO'].transform(
                lambda x: (x - x.mean()).abs() > 3 * x.std())
            df = df.loc[~df['flag']]
            # print("After outlier removal:", len(df))
            # find short tow_ids
            df = df[df.groupby('tow_id')['tow_id'].transform('count') >= 10]
            # print("After short tow_id removal:", len(df))
            df['DO_filtered'] = df.groupby('tow_id')['DO'].transform(
                lambda x: medfilt(x, kernel_size=5))
            # print("After med filter:", len(df))
            filt = ['time', 'latitude', 'longitude', 'temperature',
                    'DO_filtered']

            df_re = df.groupby('tow_id')[filt].apply(
                lambda x: x.set_index('time').resample('h').mean().reset_index()).reset_index(drop=True)
            
            df_re.rename(
                columns={'DO_filtered': 'dissolved_oxygen'}, inplace=True)
            df_re = df_re[df_re['dissolved_oxygen'] > 0]
            df_re.loc[:,'data_provider'] = 'eMOLT'

            existing_columns = [col for col in keepers if col in df_re.columns]
            print(f"{dataset_id} post filter: {len(df_re)}")
            df_re.to_csv(f'process_test_{dataset_id}.csv', index=False)
            return df_re[existing_columns]
        except Exception as e:
            logger.error("error processing %s: %s",
                         dataset_id, e, exc_info=True)
            return pd.DataFrame(columns=keepers)  # return an empty dataframe if processing fails
    elif dataset_id in ["shelf_fleet_profiles_1m_binned", "wind_farm_profiles_1m_binned"]:
        try:
            df.to_csv(f'raw_{dataset_id}.csv', index=False)
            print(f"{dataset_id} pre filter: {len(df)}")
            df = df.loc[df.groupby('profile_id')['sea_pressure'].idxmax()]
            df = df[['conservative_temperature', 'absolute_salinity',
                     'latitude', 'longitude', 'time']]
            df.rename(columns={'conservative_temperature': 'temperature',
                      'absolute_salinity': 'salinity'}, inplace=True)
            df.loc[:,'data_provider'] = 'CFRF'
            df['time'] = pd.to_datetime(df['time'])

            df.to_csv(f'process_test_{dataset_id}.csv', index=False)
            print(f"{dataset_id} post filter: {len(df)}")
            existing_columns = [col for col in keepers if col in df.columns]
            return df[existing_columns]
        except Exception as e:
            logger.error("error processing %s: %s", dataset_id, e)
            return pd.DataFrame(columns=keepers)

    elif dataset_id == 'fixed_gear_oceanography':
        try:
            df.to_csv(f'raw_{dataset_id}.csv', index=False)
            print(f"{dataset_id} pre filter: {len(df)}")
            df['time'] = pd.to_datetime(df['time'])
            df['time'] = df['time'].dt.tz_localize(None)
            df['flag'] = df.groupby('tow_id')['dissolved_oxygen'].transform(
                lambda x: (x - x.mean()).abs() > 3 * x.std())
            df = df.loc[~df['flag']]
            df.loc[:,'data_provider'] = 'CFRF'
            df.to_csv(f'process_test_{dataset_id}.csv', index=False)
            existing_columns = [col for col in keepers if col in df.columns]
            print(f"{dataset_id} post filter: {len(df)}")
            return df[existing_columns]
        except Exception as e:
            logger.error("error processing %s: %s", dataset_id, e)
            return pd.DataFrame(columns=keepers) # return an empty dataframe if processing fails


def load_local_studyfleet(gdf_grid, last_runtime) -> pd.DataFrame:
    """ Side load the study fleet data from a local CSV file"""
    logger.info("Loading local studyfleet data")
    try:
        # need to change the location
        study_fleet = pd.read_csv(
            'data/local_data/SF_7KM_BottomTempData_AllData.csv')
        study_fleet['OBSERVATION_DATE'] = pd.to_datetime(
            study_fleet['OBSERVATION_DATE'], format='%d-%b-%y %H:%M:%S')
        study_fleet.rename(columns={
                           'OBSERVATION_DATE': 'time', 'GRID_ID': 'id', 'TEMP': 'temperature'}, inplace=True)
        study_fleet = study_fleet.merge(
            gdf_grid[['id', 'geometry', 'centroid']], on='id', how='left')
        study_fleet.loc[:,'data_provider'] = 'StudyFleet'
        study_fleet = study_fleet[study_fleet['time'] > pd.to_datetime(
            last_runtime).replace(tzinfo=None)]
        if study_fleet.empty:
            logger.info('No recent data in Study Fleet local')
            return pd.DataFrame()
    except Exception as e:
        logger.error("Error loading local studyfleet data: %s", e)
        raise
    logger.info("Local studyfleet data loaded")
    return study_fleet


def determine_reload_schedule() -> tuple:
    """Determine the reload schedule based on the current date and return query time"""
    try:
        today = datetime.now()
        today = pd.to_datetime('2025-01-01T00:00:00Z')  # for testing
        # Annual reload
        if today.month == 1 and today.day == 1:
            logger.info("Annual reload scheduled")
            return 'full', '1995-01-25T00:00:00Z'

        # Quarterly reload
        quarterly_months = [4, 7, 10]
        if today.month in quarterly_months and today.day <= 1:
            logger.info("Quarterly reload scheduled")
            fetch_threshold = (today - relativedelta(years=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            return 'quarterly', fetch_threshold.isoformat()

        # Bi-weekly reload
        if today.day in [1, 15]:
            logger.info("Bi-weekly reload scheduled")
            fetch_threshold = (today - relativedelta(months=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            return 'bi-weekly', fetch_threshold.isoformat()

        # Daily reload
        fetch_threshold = (today - relativedelta(days=5)).replace(hour=0, minute=0, second=0, microsecond=0)
        logger.info("Daily reload scheduled")
        return 'daily', fetch_threshold.isoformat()

    except Exception as e:
        logger.error("Error determining reload schedule: %s", e)
        return None, None


def main(reload_type, query_time, storage_protocol='s3'):
    """ main function to call all subroutines"""

    # logger.info("=============================")
    # logger.info("FIShBOT Application started")
    
    # reload_type, query_time = determine_reload_schedule()

    current_time = datetime.now(timezone.utc).isoformat()
    logger.info('reload type: %s fetching data after %s',
                reload_type, query_time)

    logger.info('getting data from erddap...')
    gdf_grid = sp.get_botgrid()
    dat = ERDDAPClient(DATASETS, query_time)
    results = asyncio.run(dat.fetch_all_data())

    logger.info('Data retrieval complete')
    dataframes = []
    database_log = []  # list to collect dicts for logging to DB

    for data_provider, dataset_info in DATASETS.items():
        if data_provider == 'studyfleet':
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

    with DatabaseConnector(DB_HOST, DB_USER, DB_PASS, DB) as db:
        db.log_data_events(database_log, DB_EVENTS_TABLE)

    logger.info('Standardizing dataframes complete!')
    logger.info('-----------------------------------')
    
    if dataframes:
        try:
            dataframes = [df for df in dataframes if not df.empty] # pop missing dataframes
            combined_df = pd.concat(dataframes, ignore_index=True)

            if combined_df.empty and studyfleet.empty:  # ! remove this check once study fleet are on erddap
                logger.info('No data to process, exiting...')
                return
            logger.info('Concatenated all dataframes into a single dataframe with %s rows and %s columns. Columns: %s',
                        combined_df.shape[0], combined_df.shape[1], combined_df.columns)
        except Exception as e:
            logger.error("Error concatenating dataframes: %s", e)
            raise
    else:
        logger.info('No new data to process, exiting...')
        return

    standard_df = sp.gridify_df(combined_df, gdf_grid)

    try:
        full_fleet = pd.concat([studyfleet, standard_df], ignore_index=True)
    except Exception as e:
        logger.warning('Could not concat with Study Fleet:%s', e)
        full_fleet = combined_df
    try:
        try:
            dataframes.clear()
            # free up memory before aggregation
            del standard_df, studyfleet, combined_df, dataframes
            gc.collect()
            agg_df = aggregated_data(full_fleet)
            del full_fleet
            gc.collect()
        except Exception as e:
            logger.error("Error freeing memory data: %s", e)
            raise

        # * create new DF of just postions for speed

        gridded_positions = agg_df[['latitude',
                                    'longitude']].drop_duplicates().copy()
        gridded_positions = gridded_positions.pipe(
            sp.assign_statistical_areas).pipe(sp.find_closest_depth)

        # * pipe the positions through the assignment and join back based on coordinates
        agg_df = agg_df.merge(gridded_positions[['latitude', 'longitude', 'depth', 'stat_area']], on=[
            'latitude', 'longitude'], how='left')
        agg_df.drop(columns=['geometry'], inplace=True)
    except Exception as e:
        logger.error("Error processing data: %s", e)
        raise
    logger.info('Data aggregation and metadata assingment complete.')
    logger.info('-----------------------------------------')
    logger.info('Packing data to NetCDF...')
    try:
        files = pack_to_netcdf(
            agg_df, output_path='erddap', version=__version__)

    except Exception as e:
        logger.error("Error packing data to NetCDF: %s", e)
        raise
    logger.info('NetCDF packing complete!')
    logger.info('created %s nc files', len(files))
    logger.info('-----------------------------------------')
    logger.info('Archiving fishbot_realtime')
    try:
        fishbot_archive = dat.archive_fishbot(
            current_time, version=__version__)

        logger.info('Fishbot archive created successfully!')
        logger.info('-----------------------------------------')
    except Exception as e:
        logger.error("Error archiving fishbot: %s", e)
    try:
        s3 = S3Connector(BUCKET_NAME, AWS_REGION, AWS_PROFILE)
        logger.info("Pushing fishbot archive to S3")
        s3.push_to_s3(fishbot_archive, prefix='archive')
        archive_key = s3.get_archive_key()
        public_url = s3.get_archive_url()
        logger.info("Archive key: %s", public_url)
        logger.info("Pushed fishbot archive to S3")
        logger.info("Pushing fishbot_realtime to S3")
        if storage_protocol == 'local':
            move_files(files, ERDDAP_DATA_PATH)
            logger.info("Moved fishbot_realtime to %s", ERDDAP_DATA_PATH)
            reload_erddap(ERDDAP_DATA_PATH, 'fishbot_realtime')
            
        elif storage_protocol == 's3':
            s3.push_to_s3(files)
            logger.info("Pushed fishbot_realtime to S3")
    except Exception as e:
        logger.error("Error archiving fishbot: %s", e)
    logger.info('logging the archive in the database')
    try:
        with DatabaseConnector(DB_HOST, DB_USER, DB_PASS, DB) as db:
            logger.info("Logging archive to DB")
            archive_dict = {
                "archive_s3_key": archive_key,
                "archive_public_url": public_url,
                "archive_date": current_time,
                "version": __version__,
                "doi": "",
                "reload_type": reload_type
            }
            db.log_archive(archive_dict, DB_ARCHIVE_TABLE)
            logger.info("Archive logged to DB successfully")
    except Exception as e:
        logger.error("Error logging archive to DB: %s", e, exc_info=True)
        raise
    logger.info('-----------------------------------------')
    if storage_protocol == 'local':
        logger.info('Updating fishbot_archive dataset in ERDDAP')
        with DatabaseConnector(DB_HOST, DB_USER, DB_PASS, DB) as db:
            archive_df = db.update_archive_record(DB_ARCHIVE_TABLE)
        archive_local = os.path.join(ERDDAP_DATA_PATH, 'archive.csv')
        archive_df.to_csv(archive_local, index=False)
        logger.info("Archive data saved to %s", archive_local)
        # move_files([archive_local], ERDDAP_DATA_PATH)
        reload_erddap(ERDDAP_DATA_PATH, 'fishbot_archive')
        logger.info("Fishbot archive dataset updated in ERDDAP")
        logger.info('-----------------------------------------')

    logger.info("Application complete!")
    logger.info("=============================")


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="FIShBOT Application")
    parser.add_argument('-s', '--storage_protocol', type=str, default='s3', choices=['s3', 'local'],
                        help="Specify the storage protocol to use (default: s3)")
    args = parser.parse_args()

    logger.info("=============================")
    logger.info("FIShBOT Application started")

    if not test_erddap_archive():
        logger.error("ERDDAP archive is not reachable. Exiting...")
        sys.exit(1)
    
    reload_type, query_time = determine_reload_schedule()

    main(reload_type, query_time, storage_protocol=args.storage_protocol)

    logger.info("Application complete!")
    logger.info("=============================")
