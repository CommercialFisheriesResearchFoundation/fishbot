import logging
import geopandas as gpd
from shapely.geometry import Point
import pandas as pd
import xarray as xr

logger = logging.getLogger(__name__)


def gridify_df(df, gdf_grid=None) -> gpd.GeoDataFrame:
    """Takes the standardized df and assigns grid values, returning a GeoDataFrame"""

    if not isinstance(gdf_grid, gpd.GeoDataFrame):
        gdf_grid = get_botgrid()

    try:
        unique_points = df[['latitude', 'longitude']].drop_duplicates().copy()
        unique_points['geometry'] = unique_points.apply(
            lambda row: Point(row['longitude'], row['latitude']), axis=1)
        unique_gdf = gpd.GeoDataFrame(
            unique_points, geometry='geometry', crs="EPSG:4326")
        unique_joined = gpd.sjoin(unique_gdf, gdf_grid[[
                                  'geometry', 'id', 'centroid']], how='left', predicate='within')

        # Use the geometry from gdf_grid
        coop_fleet = df.merge(unique_joined[['latitude', 'longitude', 'id', 'centroid']], on=[
                              'latitude', 'longitude'], how='left')
        coop_fleet = coop_fleet.merge(
            gdf_grid[['id', 'geometry']], on='id', how='left')
        coop_fleet = coop_fleet.drop(columns=['latitude', 'longitude'])
    except Exception as e:
        logger.error("Error gridifying the data: %s", e, exc_info=True)
        raise
    return coop_fleet


def get_botgrid() -> gpd.GeoDataFrame:
    """
    Load the grid shapefile and reproject it to WGS84. The grid is used to
    standarize the data to a common grid.
    """
    logger.info("Loading grid shapefile")
    fid = 'botgrid/clipped_NE_grid_7km_reprojected.shp'
    try:
        gdf_grid = gpd.read_file(fid)
        projected_crs = "EPSG:5070"  # Albers Equal Area Conic
        # projected_crs = "EPSG:4326"  # WSG84. Cannot use to find grid centroids
        gdf_grid = gdf_grid.to_crs(projected_crs)
        gdf_grid['centroid'] = gdf_grid.geometry.centroid
        gdf_grid['centroid'] = gdf_grid['centroid'].to_crs("EPSG:4326")
        gdf_grid['geometry'] = gdf_grid['geometry'].to_crs("EPSG:4326")
        gdf_grid['id'] = gdf_grid['id'].astype(int)
    except Exception as e:
        logger.error("Error loading grid shapefile: %s", e)
        raise
    logger.info("Grid shapefile loaded and reprojected")
    return gdf_grid


def find_nearest_stat_area(row, stat_gdf) -> int:
    """Finds the nearest statistical area to a given point"""
    try:
        point_wgs84 = Point(row['longitude'], row['latitude'])

        projected_crs = "EPSG:5070"  # Albers Equal Area Conic
        point_proj = gpd.GeoSeries([point_wgs84], crs="EPSG:4326").to_crs(
            projected_crs).geometry[0]

        # Compute distances using projected centroids
        distances = stat_gdf['centroid'].distance(point_proj)

        # Get the nearest stat_area
        nearest_idx = distances.idxmin()
    except Exception as e:
        logger.error("Error finding nearest statistical area: %s",
                     e, exc_info=True)
        raise
    return stat_gdf.loc[nearest_idx, 'stat_area']


def assign_statistical_areas(df) -> pd.DataFrame:

    logger.info("Loading stat area shapefile")
    # Read and process stat_area polygons
    try:
        fid = 'data/statistical_areas/Statistical_Areas_2010_withNames.shp'
        stat = gpd.read_file(fid)
        stat.rename(columns={'Id': 'stat_area'}, inplace=True)

        # Use a projected CRS for accurate centroid calculation
        projected_crs = "EPSG:5070"  # Albers Equal Area Conic
        stat = stat.to_crs(projected_crs)
        stat['centroid'] = stat.geometry.centroid  # Compute centroids
    except Exception as e:
        logger.error("Error loading stat area shapefile: %s", e, exc_info=True)
        raise

    # Ensure full_fleet_aggregated is in EPSG:4326 and apply the function
    try:
        df['stat_area'] = df.apply(
            find_nearest_stat_area, stat_gdf=stat, axis=1)
    except Exception as e:
        logger.error("Error assigning statistical areas: %s", e, exc_info=True)
        raise
    return df


def find_closest_depth(df) -> pd.DataFrame:
    '''Find the closest depth value from the bathymetry file'''
    try:
        logger.info('Assinging depth values to the data')
        fid = 'data/bathymetry/gebco_2024_n46.0_s36.0_w-76.0_e-64.0.nc'
        ds = xr.open_dataset(fid)
    except Exception as e:
        logger.error("Error loading bathymetry file: %s", e, exc_info=True)
        raise
    # grouped = df.groupby('tow_id').agg({'latitude': 'first', 'longitude': 'first'}).reset_index()
    # Initialize an empty list to store depth values
    try:
        depths = ds.sel(
            lat=xr.DataArray(df['latitude'], dims='z'),
            lon=xr.DataArray(df['longitude'], dims='z'),
            method='nearest'
        )['elevation'].values

        # Add inferred depth as a new column, converting to absolute values
        df['depth'] = abs(depths).astype(int)
    except Exception as e:
        logger.error("Error assigning depth values: %s", e, exc_info=True)
        raise
    return df
