from erddapy import ERDDAP
import logging
import pandas as pd
import xarray as xr

logger = logging.getLogger(__name__)

def truncate_at_first_space(col_name):
    return col_name.split(' ')[0]

def get_cfrf_data(start_time) -> xr.Dataset:
    logger.info('Getting data from CFRF ERDDAP server...')
    server = 'https://erddap.ondeckdata.com/erddap/'
    try:
        e = ERDDAP(
            server=server,
            protocol="tabledap",
            response="nc",
        )
        e.dataset_id = 'seafloor_oceanography'
        e.constraints = {'time>=': f'{start_time}',
                         'time<=': '2023-12-31T23:59:59Z'}
        
        data = e.to_pandas()
        data.rename(columns=lambda x: truncate_at_first_space(x), inplace=True)
        data['time'] = pd.to_datetime(data['time'])
        keepers = ['time', 'latitude', 'longitude', 'temperature','tow_id']
    except Exception as e:
        logger.error('Error connecting to ERDDAP server: %s',e)
    logger.info('Data retrieved successfully!')
    return data[keepers]

def get_emolt_data(start_time, end_time='2023-12-31T23:59:59Z') -> xr.Dataset:
    logger.info('Getting data from eMOLT ERDDAP server...')
    server = 'https://erddap.emolt.net/erddap/'
    all_data = []
    try:
        # Split time range into monthly intervals
        date_range = pd.date_range(start=start_time, end=end_time, freq='M')
        date_range = date_range.union([pd.to_datetime(start_time), pd.to_datetime(end_time)]) # Ensure exact start and end are included

        for start, end in zip(date_range[:-1], date_range[1:]):
            e = ERDDAP(
                server=server,
                protocol="tabledap",
                response="nc",
            )
            e.dataset_id = 'eMOLT_RT_QAQC'
            e.constraints = {
                'time>=': start.strftime('%Y-%m-%dT%H:%M:%SZ'),
                'time<=': end.strftime('%Y-%m-%dT%H:%M:%SZ'),
                'segment_type=': 'Fishing',
                'latitude>=': 41,
                'latitude<=': 41.5,
                'longitude>=': -72,
                'longitude<=': -70
            }
            e.requests_kwargs = {'timeout': 600}  # Extended timeout

            # Fetch data for this interval and append it to the list
            try:
                data = e.to_xarray()
                all_data.append(data)
                logger.info('Data for period %s to %s retrieved successfully.', start, end)
            except Exception as e:
                logger.error('Error retrieving data for period %s to %s: %s', start, end, e)

        # Concatenate all chunks
        if all_data:
            combined_data = xr.concat(all_data, dim='time')
            logger.info('All data successfully combined.')
            return combined_data

    except Exception as e:
        logger.error('Error connecting to ERDDAP server: %s', e)

    return None

def get_emolt_local():
    logger.info('Getting data from eMOLT local server...')
    data = xr.open_dataset('data/local_data/eMOLT_RT_QAQC_2023.nc')
    df = data.to_dataframe()
    df['time'] = pd.to_datetime(df['time'])
    keepers = ['time', 'latitude', 'longitude', 'temperature','tow_id']
    return df[keepers]

def dms_to_dd(degrees, minutes):
    return degrees + minutes / 60

def get_study_fleet_data():
    logger.info('Getting data from study fleet ERDDAP server...')
    data = pd.read_csv('data/local_data/sf_botgrid_2023_full.csv')

    data['latitude'] = data.apply(lambda row: dms_to_dd(row['latitude_degrees'], row['latitude_minutes']), axis=1)
    data['longitude'] = data.apply(lambda row: dms_to_dd(row['longitude_degrees'], row['longitude_minutes']), axis=1)
    data.rename(columns={'mean_temp':'temperature','datetime_bin':'time'}, inplace=True)
    data['time'] = pd.to_datetime(data['time'], format='%y-%b-%d')
    keepers = ['time', 'latitude', 'longitude', 'temperature','temp_observations','min_temp','max_temp','std_dev_temp']
    return data[keepers]

def aggregate_data(df, step='daily', agg_cols='tow_id'):
    agg_funcs = {
        'temperature': ['mean', 'std', 'min', 'max', 'count'],
        'latitude': 'first',
        'longitude': 'first'
    }
    
    if step == 'daily':
        df_copy = df.set_index('time').groupby(agg_cols).resample('D').agg(agg_funcs).reset_index()
    elif step == 'monthly':
        df_copy = df.set_index('time').groupby(agg_cols).resample('M').agg(agg_funcs).reset_index()
    else:
        logger.error('Invalid aggregation step: %s', step)
        return None
    # Flatten the MultiIndex columns
    df_copy.columns = ['_'.join(col).strip() if isinstance(col, tuple) else col for col in df_copy.columns.values]
    df_copy.rename(columns={'tow_id_': 'tow_id',
                            'time_':'time',
                            'temperature_mean':'temperature',
                            'temperature_std':'std_dev_temp',
                            'temperature_min':'min_temp',
                            'temperature_max':'max_temp',
                            'temperature_count':'temp_observations',
                            'latitude_first':'latitude',
                            'longitude_first':'longitude'}, inplace=True)
    df_copy['time'] = df_copy['time'].dt.strftime('%Y-%m-%d')
    df_copy.dropna(subset=['temperature'], inplace=True)
    return df_copy.drop(columns=['tow_id'])

def get_latest_data(start_time) -> xr.Dataset:
    cfrf_data = get_cfrf_data(start_time)
    cfrf_data = aggregate_data(cfrf_data,step='daily',agg_cols='tow_id')
    emolt_data = get_emolt_local()
    emolt_data = aggregate_data(emolt_data,step='daily',agg_cols='tow_id')
    study_fleet_data = get_study_fleet_data()
    df = pd.concat([cfrf_data, emolt_data, study_fleet_data])
    df['time'] = pd.to_datetime(df['time'])
    return df