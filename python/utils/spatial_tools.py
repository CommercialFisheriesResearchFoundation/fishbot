import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, box
import numpy as np
import logging

logger = logging.getLogger(__name__)
import numpy as np
import pandas as pd
import geopandas as gpd
import xarray as xr
from shapely.geometry import Point, box
from datetime import timedelta
from scipy.interpolate import griddata
from datetime import datetime


# Step 1: Define the Latitude and Longitude Coordinate Grid
lat_min, lat_max = 41.0, 42.0  # Adjust bounds as needed
lon_min, lon_max = -72.0, -70.0  # Adjust bounds as needed
grid_resolution = 0.00416667  # 15 arc seconds in degrees

latitudes = np.arange(lat_min, lat_max, grid_resolution)
longitudes = np.arange(lon_min, lon_max, grid_resolution)
print(len(latitudes), len(longitudes))
# grid_temp = np.full((len(latitudes), len(longitudes)), np.nan)
def combine_to_grid(gdf):
    # Create an empty grid to store gridded temperatures
    grid_temp = np.full((len(latitudes), len(longitudes)), np.nan)
    gdf_clean = gdf.dropna(subset=['latitude', 'longitude'])
    # Interpolate field data to the grid
    points = np.array([gdf_clean['latitude'], gdf_clean['longitude']]).T
    values = gdf_clean['temperature'].values
    grid_lon, grid_lat = np.meshgrid(longitudes, latitudes)
    grid_points = np.array([grid_lon.flatten(), grid_lat.flatten()]).T

    # Use griddata for interpolation
    # Check for NaN or infinite values in the latitude, longitude, and temperature columns
    print("NaN or inf values in 'latitude':", gdf_clean['latitude'].isna().sum(), np.isinf(gdf_clean['latitude']).sum())
    print("NaN or inf values in 'longitude':", gdf_clean['longitude'].isna().sum(), np.isinf(gdf_clean['longitude']).sum())
    print("NaN or inf values in 'temperature':", gdf_clean['temperature'].isna().sum(), np.isinf(gdf_clean['temperature']).sum())
    
    gridded_temp = griddata(points, values, grid_points, method='nearest')

    # Reshape the gridded data to match grid shape
    grid_temp = gridded_temp.reshape(len(latitudes), len(longitudes))

    # Create an xarray dataset for the gridded product
    ds = xr.Dataset(
        {'temperature': (['lat', 'lon'], grid_temp)},
        coords={'lat': latitudes, 'lon': longitudes}
    )
    return ds

# # Save the result as a NetCDF file
# ds.to_netcdf('gridded_bottom_temperature.nc')



grid_cells = []
for lat in latitudes:
    for lon in longitudes:
        cell = box(lon, lat, lon + grid_resolution, lat + grid_resolution)
        grid_cells.append(cell)

grid = gpd.GeoDataFrame(geometry=grid_cells, crs="EPSG:4326")

def create_shell_xr(grid):

    centroids = grid.centroid
    centroid_latitudes = centroids.y.values
    centroid_longitudes = centroids.x.values
    # Step 1: Create the coordinate arrays for xarray
    # latitudes_2d, longitudes_2d = np.meshgrid(latitudes, longitudes, indexing='ij')
    time_range = pd.date_range(start='2023-01-01', end='2023-12-31', freq='D')
    # Step 2: Create the xarray dataset
    # The temperature column is a dummy filled with 0 values
    temperature_data = np.full((len(time_range), len(centroid_latitudes), len(centroid_longitudes)),np.nan)

    # Step 3: Create the xarray Dataset
    ds = xr.Dataset(
        {
            "temperature": (["time", "latitude", "longitude"], temperature_data)
        },
        coords={
            "time": time_range,
            "latitude": centroid_latitudes,
            "longitude": centroid_longitudes
        }
    )
    ds["temperature"] = ds["temperature"].astype("float32")
    ds["latitude"] = ds["latitude"].astype("float32")
    ds["longitude"] = ds["longitude"].astype("float32")

    return ds

def package_data_xr(df):
    df_copy = df.copy()
    df_copy = df_copy.drop(['index', 'index_right'], axis=1)
    ds = df_copy.set_index(['time', 'latitude', 'longitude']).to_xarray()
    ds.coords['latitude'].attrs['units'] = 'decimal_degrees'
    ds.coords['longitude'].attrs['units'] = 'decimal_degrees'
    ds.coords['latitude'].attrs['standard_name'] = 'latitude'
    ds.coords['longitude'].attrs['standard_name'] = 'longitude'
    ds['temperature'].attrs['units'] = 'degrees_celsius'
    ds['temperature'].attrs['standard_name'] = 'sea_water_temperature'


    ds["temperature"] = ds["temperature"].astype("float32")
    ds["latitude"] = ds["latitude"].astype("float32")
    ds["longitude"] = ds["longitude"].astype("float32")

    return ds

def get_shell():
    ds = create_shell_xr(latitudes, longitudes)
    return ds

# Convert data frames to GeoDataFrames and aggregate daily if needed
def create_geodataframe(df, lon_col='longitude', lat_col='latitude'):
    return gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df[lon_col], df[lat_col]), crs="EPSG:4326")


# Function to assign data to grid and aggregate
def spatial_bin_and_aggregate(data_gdf, grid_gdf):
    # Perform spatial join
    joined = gpd.sjoin(data_gdf, grid_gdf, how='left', predicate='within')
    
    # Define aggregation functions for temperature
    agg_funcs = {
        'temperature': 'mean'
    }
    
    # Aggregate by grid cell (index_right) and time
    aggregated = joined.groupby(['index_right', 'time']).agg(agg_funcs).reset_index()
    
    # Flatten the MultiIndex columns if necessary
    aggregated.columns = ['_'.join(col).strip() if isinstance(col, tuple) else col for col in aggregated.columns.values]
    
    # Merge with grid to get coordinates
    aggregated = aggregated.merge(grid_gdf.reset_index(), left_on='index_right', right_index=True, how='left')
    
    # Extract coordinates from geometry
    aggregated['longitude'] = aggregated['geometry'].apply(lambda geom: geom.centroid.x)
    aggregated['latitude'] = aggregated['geometry'].apply(lambda geom: geom.centroid.y)
    
    # Drop the geometry column if not needed
    aggregated.drop(columns=['geometry'], inplace=True)
    
    return aggregated


def gridify_data(df):
    # Convert DataFrame to GeoDataFrame
    # gdf = create_geodataframe(df)
    df.dropna(subset=['latitude', 'longitude'], inplace=True)
    output_path = "data/nc_out"
    for date, daily_data in df.groupby('time'):
        latitudes, longitudes, grid_temp = interpolate_to_grid(daily_data, grid)
        save_daily_to_netcdf(latitudes, longitudes, grid_temp, date, output_path)
    # gdf.to_csv('intermediate_data.csv',index=False)
    # ds = combine_to_grid(gdf)
    return None
    # # Apply spatial binning and aggregation
    # binned_data = spatial_bin_and_aggregate(gdf, grid)
    # ds_data = package_data_xr(binned_data)
    # ds = create_shell_xr(grid)
    # # ds_data = ds_data.reindex_like(ds)
    # merged_ds = ds.combine_first(ds_data)
    
    # return merged_ds

def create_xarray_dataset(grid_gdf, aggregated_data):
    # Pivot the aggregated data to ensure proper reshaping for xarray
    pivot_table = aggregated_data.pivot_table(
        index=['latitude', 'longitude'],
        columns='time',
        values='temperature'
    )
    
    # Convert pivot table to numpy array (ensuring NaNs for missing values)
    temperature_data = pivot_table.values
    
    # Create xarray dataset
    ds = xr.Dataset(
        {
            'temperature': (['latitude', 'longitude', 'time'], temperature_data)
        },
        coords={
            'latitude': pivot_table.index.get_level_values('latitude').unique(),
            'longitude': pivot_table.index.get_level_values('longitude').unique(),
            'time': pivot_table.columns
        }
    )
    
    return ds

import numpy as np
import pandas as pd
import geopandas as gpd
import xarray as xr
from scipy.interpolate import griddata

def interpolate_to_grid(daily_data, grid_gdf, grid_resolution=0.00416667):
    # Prepare arrays for interpolation
    lat_min, lat_max = grid_gdf.total_bounds[1], grid_gdf.total_bounds[3]
    lon_min, lon_max = grid_gdf.total_bounds[0], grid_gdf.total_bounds[2]
    
    latitudes = np.arange(lat_min, lat_max, grid_resolution)
    longitudes = np.arange(lon_min, lon_max, grid_resolution)
    
    # Create a mesh grid for the interpolation
    lon_grid, lat_grid = np.meshgrid(longitudes, latitudes)
    
    # Perform interpolation using 'griddata'
    points = daily_data[['longitude', 'latitude']].values
    values = daily_data['temperature'].values
    
    # Interpolate temperature data to the regular grid
    grid_temp = griddata(points, values, (lon_grid, lat_grid), method='linear')
    
    return latitudes, longitudes, grid_temp

def save_daily_to_netcdf(latitudes, longitudes, grid_temp, date, output_path):
    # Create an xarray DataArray
    formatted_date = date.strftime('%Y-%m-%d')

    epoch = datetime(1970, 1, 1)
    days_since_epoch = (datetime.strptime(formatted_date, '%Y-%m-%d') - epoch).days

    da = xr.DataArray(
        grid_temp,
        coords={'latitude': latitudes, 'longitude': longitudes},
        dims=('latitude', 'longitude'),
        name='temperature'
    )
    
    # Create an xarray Dataset
    ds = xr.Dataset({'temperature': da}).expand_dims(time=[days_since_epoch])
    # ds = ds.assign_coords(time=days_since_epoch)

    ds['temperature'].attrs['units'] = 'degrees Celsius'
    ds['temperature'].attrs['standard_name'] = 'sea_water_temperature'
    ds['temperature'].attrs['long_name'] = 'Bottom temperature'
    ds['temperature'].attrs['_fillvalue'] = np.nan

    ds['latitude'].attrs['units'] = 'degrees_north'
    ds['latitude'].attrs['standard_name'] = 'latitude'
    ds['latitude'].attrs['axis'] = 'Y'

    ds['longitude'].attrs['units'] = 'degrees_east'
    ds['longitude'].attrs['standard_name'] = 'longitude'
    ds['longitude'].attrs['axis'] = 'X'

    ds['time'].attrs['standard_name'] = 'time'
    ds['time'].attrs['axis'] = 'T'
    ds['time'].attrs['units'] = 'days since 1970-01-01'
    
    # Set global attributes
    ds.attrs['title'] = f'BotGrid - {formatted_date}'
    ds.attrs['description'] = 'Gridded bottom temperature data collected by NOAA eMOLT, NOAA Study Fleet, and CFRF'
    ds.attrs['conventions'] = 'CF-1.8'
    ds.attrs['institution'] = 'Commercial Fisheries Research Foundation | National Oceanic and Atmospheric Administration'
    ds.attrs['geospatial_lat_min'] = ds['latitude'].min().values
    ds.attrs['geospatial_lat_max'] = ds['latitude'].max().values
    ds.attrs['geospatial_lon_min'] = ds['longitude'].min().values
    ds.attrs['geospatial_lon_max'] = ds['longitude'].max().values


    ds["temperature"] = ds["temperature"].astype("float32")
    ds["latitude"] = ds["latitude"].astype("float32")
    ds["longitude"] = ds["longitude"].astype("float32")
    # Define output file name
    filename = f"{output_path}/botgrid_{formatted_date}.nc"


    
    # Write to NetCDF
    ds.to_netcdf(filename)
    print(f"Saved: {filename}")
