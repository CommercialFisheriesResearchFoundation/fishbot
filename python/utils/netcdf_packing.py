import os
import xarray as xr
import pandas as pd
from datetime import datetime, timedelta
import logging
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
        }
    }
    return attrs.get(var_name, {})


def pack_to_netcdf(df_out, output_path="data/nc_out_full", version="0.1") -> list:
    """ tool to create daily nc files for output of the entire grid. Function also returns a list of all created file names for pushing to S3."""
    try:
        df_out['time'] = pd.to_datetime(df_out['time'])
        epoch = datetime(1970, 1, 1)
        df_out['time'] = (df_out['time'] - epoch).dt.days
    except Exception as e:
        logger.error(
            'could not covert time stamp to days since 1970-01-01: %s', e)
        raise

    try:
        logger.info('filtering un reasonable positions and values...')
        df_out = df_out[(df_out['depth'] < 900) & (df_out['depth'] > 1)]
        df_out = df_out[(df_out['temperature'] > 0) &
                        (df_out['temperature'] < 27)]
    except Exception as e:
        logger.error('could not filter out invalid data: %s', e)
        raise

    grouped = df_out.groupby("time")
    filenames = []
    try:
        for day, group in grouped:
            ds = xr.Dataset(
                {
                    var: ("time", group[var].values)
                    for var in group.columns if var not in ["time", "latitude", "longitude"]
                },
                coords={
                    "time": ("time", [day] * len(group)),
                    "latitude": ("time", group["latitude"].values),
                    "longitude": ("time", group["longitude"].values),
                },
            )
            ds.encoding["unlimited_dims"] = {"time"}

            for var in ds.data_vars:
                ds[var].attrs = set_variable_attrs(var)

            for coord in ds.coords:
                ds[coord].attrs = set_variable_attrs(coord)

            ds.attrs["title"] = "Fishing Industry Shared Bottom Oceanographic Timeseries"
            ds.attrs["description"] = "Gridded daily observations of demersal oceanographic observations and related metrics."
            ds.attrs["institution"] = "CFRF | NOAA NEFSC"
            ds.attrs["version"] = version

            ds["temperature"] = ds["temperature"].astype("float32")
            ds["temperature_std"] = ds["temperature_std"].astype("float32")
            ds["temperature_min"] = ds["temperature_min"].astype("float32")
            ds["temperature_max"] = ds["temperature_max"].astype("float32")
            ds["temperature_count"] = ds["temperature_count"].astype("uint32")

            ds["dissolved_oxygen"] = ds["dissolved_oxygen"].astype("float32")
            ds["dissolved_oxygen_std"] = ds["dissolved_oxygen_std"].astype(
                "float32")
            ds["dissolved_oxygen_min"] = ds["dissolved_oxygen_min"].astype(
                "float32")
            ds["dissolved_oxygen_max"] = ds["dissolved_oxygen_max"].astype(
                "float32")
            ds["dissolved_oxygen_count"] = ds["dissolved_oxygen_count"].astype(
                "uint32")

            ds["salinity"] = ds["salinity"].astype("float32")
            ds["salinity_std"] = ds["salinity_std"].astype("float32")
            ds["salinity_min"] = ds["salinity_min"].astype("float32")
            ds["salinity_max"] = ds["salinity_max"].astype("float32")
            ds["salinity_count"] = ds["salinity_count"].astype("uint32")
            ds["depth"] = ds["depth"].astype("uint32")
            ds["latitude"] = ds["latitude"].astype("float32")
            ds["longitude"] = ds["longitude"].astype("float32")
            ds["time"] = ds["time"].astype("uint32")
            ds["stat_area"] = ds["stat_area"].astype("uint32")
            ds["grid_id"] = ds["grid_id"].astype("uint32")
            ds["data_provider"] = ds["data_provider"].astype("S32")

            date = datetime(1970, 1, 1) + timedelta(days=day)
            year = date.year
            month = date.month

            directory = f"{output_path}/{year}/{month}"
            if not os.path.exists(directory):
                os.makedirs(directory)
            filename = f"{directory}/fishbot_{day}.nc"
            ds.to_netcdf(filename)
            filenames.append(filename)

        return filenames
    except Exception as e:
        logger.error('could not write to netcdf: %s', e)
        raise
