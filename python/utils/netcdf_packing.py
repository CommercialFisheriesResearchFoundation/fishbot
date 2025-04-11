import os
import xarray as xr
import pandas as pd
from datetime import datetime, timedelta
import logging
from utils.s3_connector import S3Connector
from concurrent.futures import ThreadPoolExecutor
import tempfile
logger = logging.getLogger(__name__)


def set_variable_attrs(var_name) -> dict:
    """ Dictionary storing the metadata descritptions"""
    attrs = {
        "temperature": {
            "units": "degrees_Celsius",
            "standard_name": "sea_water_temperature",
            "long_name": "daily_mean_bottom_temperature",
            "comment": "Daily average bottom temperature"
        },
        "temperature_std": {
            "units": "degrees_Celsius",
            "long_name": "temperature_standard_deviation",
            "comment": "Daily standard deviation of bottom temperatures"
        },
        "temperature_min": {
            "units": "degrees_Celsius",
            "long_name": "temperature_minimum",
            "comment": "Daily minimum bottom temperature"
        },
        "temperature_max": {
            "units": "degrees_Celsius",
            "long_name": "temperature_maximum",
            "comment": "Daily maximum bottom temperature"
        },
        "temperature_count": {
            "units": "",
            "long_name": "temperature_observation_count",
            "comment": "Number of hourly averaged observations contributing to the daily average temperature"
        },
        "dissolved_oxygen": {
            "units": "miligrams_per_liter",
            "long_name": "daily_mean_bottom_dissolved_oxygen",
            "comment": "Daily average of bottom dissolved oxygen"
        },
        "dissolved_oxygen_std": {
            "units": "miligrams_per_liter",
            "long_name": "dissolved_oxygen_standard_deviation",
            "comment": "Daily standard deviation of bottom dissolved oxygen observations"
        },
        "dissolved_oxygen_min": {
            "units": "miligrams_per_liter",
            "long_name": "dissolved_oxygen_minimum",
            "comment": "Daily minimum bottom dissolved oxygen"
        },
        "dissolved_oxygen_max": {
            "units": "miligrams_per_liter",
            "long_name": "dissolved_oxygen_maximum",
            "comment": "Daily maximum bottom dissolved oxygen"
        },
        "dissolved_oxygen_count": {
            "units": "",
            "long_name": "dissolved_oxygen_observation_count",
            "comment": "Number of hourly averaged observations contributing to the daily average dissolved oxygen"
        },
        "salinity": {
            "units": "g/kg",
            "long_name": "daily_mean_bottom_salinity",
            "comment": "Daily average bottom salinity"
        },
        "salinity_std": {
            "units": "g/kg",
            "long_name": "salinity_standard_deviation",
            "comment": "Daily standard deviation of bottom salinities"
        },
        "salinity_min": {
            "units": "g/kg",
            "long_name": "salinity_minimum",
            "comment": "Daily minimum bottom salinity"
        },
        "salinity_max": {
            "units": "g/kg",
            "long_name": "salinity_maximum",
            "comment": "Daily maximum bottom salinity"
        },
        "salinity_count": {
            "units": "",
            "long_name": "salinity_observation_count",
            "comment": "Number of hourly averaged observations contributing to the daily average salinity"
        },
        "latitude": {
            "units": "degrees_north",
            "standard_name": "latitude",
            "axis": "Y",
            "comment": "Standardized centroid from predefined 7km grid of US Northeast"
        },
        "longitude": {
            "units": "degrees_east",
            "standard_name": "longitude",
            "axis": "X",
            "comment": "Standardized centroid from predefined 7km grid of US Northeast"
        },
        "time": {
            "standard_name": "time",
            "axis": "T",
            "units": "days since 1970-01-01T00:00:00 UTC",
            "comment": "Time in days since Unix epoch (UTC)"
        },
        "depth": {
            "units": "meters",
            "standard_name": "depth",
            "long_name": "inferred_depth",
            "comment": "Depth inferred from GEBCO 24 bathymetry data"
        },
        "stat_area": {
            "units": "",
            "long_name": "NEFSC_statistical_area",
            "comment": "NMFS definted statistical area used for fisheries management"
        },
        "data_provider": {
            "units": "",
            "long_name": "data_provider",
            "comment": "List of data providers contributing to the daily average for a given day and cell."
        },
        "grid_id": {
            "units": "",
            "long_name": "grid_cell_identifier",
            "comment": "Identifier of the grid cell from the predefined 7km grid of US Northeast"
        },
        "fishery_dependent": {
            "units": "",
            "long_name": "Fishery (In)Dependent Flag",
            "comment": "Boolean flag indicating if the observation was made using fishery dependent (1) or independent (0) data collection methods."
        }
    }
    return attrs.get(var_name, {})

def process_group(day, group, s3_conn, prefix, version):
    """Process a single group, write NetCDF, and  upload to S3."""
    # Check argument types
    if not isinstance(day, int):
        raise TypeError(f"Expected 'day' to be int, got {type(day).__name__}")
    if not isinstance(group, pd.DataFrame):
        raise TypeError(f"Expected 'group' to be pandas DataFrame, got {type(group).__name__}")
    if not isinstance(s3_conn, S3Connector):
        raise TypeError(f"Expected 's3_conn' to be S3Connector, got {type(s3_conn).__name__}")
    if not isinstance(prefix, str):
        raise TypeError(f"Expected 'prefix' to be str, got {type(prefix).__name__}")
    if not isinstance(version, str):
        raise TypeError(f"Expected 'version' to be str, got {type(version).__name__}")

    # Create dataset
    exclude_vars = {"time", "latitude", "longitude"}
    data_vars = {
        var: ("time", group[var].values)
        for var in group.columns if var not in exclude_vars
    }

    ds = xr.Dataset(
        data_vars,
        coords={
            "time": ("time", [day] * len(group)),
            "latitude": ("time", group["latitude"].values),
            "longitude": ("time", group["longitude"].values),
        },
    )
    ds.encoding["unlimited_dims"] = {"time"}

    # Set attributes
    for var in ds.data_vars:
        ds[var].attrs = set_variable_attrs(var)
    for coord in ds.coords:
        ds[coord].attrs = set_variable_attrs(coord)

    ds.attrs.update({
        "title": "Fishing Industry Shared Bottom Oceanographic Timeseries",
        "description": "Gridded daily observations of demersal oceanographic observations and related metrics.",
        "institution": "CFRF | NOAA NEFSC",
        "version": version,
    })

    # Cast variables
    cast_map = {
        "temperature": "float32",
        "temperature_std": "float32",
        "temperature_min": "float32",
        "temperature_max": "float32",
        "temperature_count": "uint32",
        "dissolved_oxygen": "float32",
        "dissolved_oxygen_std": "float32",
        "dissolved_oxygen_min": "float32",
        "dissolved_oxygen_max": "float32",
        "dissolved_oxygen_count": "uint32",
        "salinity": "float32",
        "salinity_std": "float32",
        "salinity_min": "float32",
        "salinity_max": "float32",
        "salinity_count": "uint32",
        "depth": "uint32",
        "latitude": "float32",
        "longitude": "float32",
        "time": "uint32",
        "stat_area": "uint32",
        "grid_id": "uint32",
        "data_provider": "S32",
        "fishery_dependent": "uint8",
    }
    for var, dtype in cast_map.items():
        if var in ds:
            ds[var] = ds[var].astype(dtype)

    # Output path setup
    date = datetime(1970, 1, 1) + timedelta(days=day)
    year, month = date.year, date.month
    s3_key = f"{prefix}/{year}/{month}/fishbot_{day}.nc"

    try:
        logger.info("Dataset columns: %s", list(ds.data_vars.keys()))
        with tempfile.NamedTemporaryFile(suffix=".nc", dir="/tmp", delete=False) as tmp:
            ds.to_netcdf(tmp.name, mode="w", engine="scipy")

        # Re-open the file to pass it as a file-like object to push_buffer_to_s3
        with open(tmp.name, 'rb') as f:
            s3_conn.push_file_to_s3(f, s3_key, content_type="application/netcdf")

    except Exception as e:
        logger.error("Failed to upload to S3: %s", e)
        raise
    finally:
        try:
            os.remove(tmp.name)  # clean up
        except Exception as cleanup_error:
            logger.warning("Could not remove temp file: %s", cleanup_error)

    return s3_key


def pack_to_netcdf(df_out, s3_conn, prefix='development', version="0.1") -> list:
    """Tool to create daily nc files for output of the entire grid and upload to S3."""
    # Validate arguments
    if not isinstance(df_out, pd.DataFrame):
        raise TypeError(f"Expected 'df_out' to be pandas DataFrame, got {type(df_out).__name__}")
    if not isinstance(s3_conn, S3Connector):
        raise TypeError(f"Expected 's3_conn' to be S3Connector, got {type(s3_conn).__name__}")
    if not isinstance(prefix, str):
        raise TypeError(f"Expected 'prefix' to be str, got {type(prefix).__name__}")
    if not isinstance(version, str):
        raise TypeError(f"Expected 'version' to be str, got {type(version).__name__}")
    
    try:
        df_out['time'] = pd.to_datetime(df_out['time'])
        epoch = datetime(1970, 1, 1)
        df_out['time'] = (df_out['time'] - epoch).dt.days
    except Exception as e:
        logger.error('could not convert timestamp to days since 1970-01-01: %s', e)
        raise

    try:
        logger.info('filtering unreasonable positions and values...')
        df_out = df_out[(df_out['depth'] < 900) & (df_out['depth'] > 1)]
        df_out = df_out[(df_out['temperature'] > 0) & (df_out['temperature'] < 27)]
    except Exception as e:
        logger.error('could not filter out invalid data: %s', e)
        raise

    s3_keys = []
    grouped = df_out.groupby("time")

    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(process_group, day, group, s3_conn, prefix, version)
            for day, group in grouped
        ]
        for future in futures:
            try:
                s3_keys.append(future.result())
            except Exception as e:
                logger.error("Error processing group: %s", e)
                raise

    return s3_keys
