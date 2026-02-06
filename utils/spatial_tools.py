import logging
import geopandas as gpd
from shapely.geometry import Point
import pandas as pd
import xarray as xr

logger = logging.getLogger()


def gridify_df(df, gdf_grid=None) -> gpd.GeoDataFrame:
    """Takes the standardized df and assigns grid values, returning a GeoDataFrame"""
    # logger.info("Gridifying the data")
    if not isinstance(gdf_grid, gpd.GeoDataFrame):
        gdf_grid = get_botgrid()

    try:
        unique_points = df[['latitude', 'longitude']].drop_duplicates().copy()
        unique_points['geometry'] = unique_points.apply(
            lambda row: Point(row['longitude'], row['latitude']), axis=1)
        unique_gdf = gpd.GeoDataFrame(
            unique_points, geometry='geometry', crs="EPSG:4326")
        unique_joined = gpd.sjoin(unique_gdf, gdf_grid[[
                                  'geometry', 'id']], how='left', predicate='within')

        # Use the geometry from gdf_grid
        coop_fleet = df.merge(unique_joined[['latitude', 'longitude', 'id']], on=[
                              'latitude', 'longitude'], how='left')
        coop_fleet = coop_fleet.merge(
            gdf_grid[['id', 'geometry','stat_area','depth','centroid_lon','centroid_lat']], on='id', how='left')
        coop_fleet = coop_fleet.drop(columns=['latitude', 'longitude'])
        coop_fleet.rename(columns={'centroid_lon': 'longitude', 'centroid_lat': 'latitude'}, inplace=True)
        coop_fleet = coop_fleet[coop_fleet['latitude'].notna() & coop_fleet['longitude'].notna()] # drop any missing lat/lon after gridify
    except Exception as e:
        logger.error("Error gridifying the data: %s", e, exc_info=True)
        raise
    return coop_fleet


def get_botgrid() -> gpd.GeoDataFrame:
    """
    Load the geo package shape file with the stat area and depth pre-loaded for the grid.
    Reprojected into to WGS84 from Albsers equal Conic.
    """
    logger.info("Loading botgrid")
    fid = 'botgrid/botgrid.gpkg'
    try:
        gdf_grid = gpd.read_file(fid, layer="botgrid") #IYKYK 
        # gdf_grid['centroid'] = gdf_grid.geometry.centroid
        # gdf_grid['centroid'] = gdf_grid['centroid'].to_crs("EPSG:4326")
        gdf_grid['geometry'] = gdf_grid['geometry'].to_crs("EPSG:4326")
        gdf_grid['id'] = gdf_grid['id'].astype(int)
    except Exception as e:
        logger.error("Error loading grid: %s", e)
        raise
    logger.info("Grid loaded and reprojected")
    return gdf_grid


# def find_nearest_stat_area(row, stat_gdf) -> int:
#     """Finds the nearest statistical area to a given point"""
#     try:
#         point_wgs84 = Point(row['longitude'], row['latitude'])

#         projected_crs = "EPSG:5070"  # Albers Equal Area Conic
#         point_proj = gpd.GeoSeries([point_wgs84], crs="EPSG:4326").to_crs(
#             projected_crs).geometry[0]

#         # Compute distances using projected centroids
#         distances = stat_gdf['centroid'].distance(point_proj)

#         # Get the nearest stat_area
#         nearest_idx = distances.idxmin()
#     except Exception as e:
#         logger.error("Error finding nearest statistical area: %s",
#                      e, exc_info=True)
#         raise
#     return stat_gdf.loc[nearest_idx, 'stat_area']


# def assign_statistical_areas(df) -> pd.DataFrame:

#     logger.info("Loading stat area shapefile")
#     # Read and process stat_area polygons
#     try:
#         fid = 'data/statistical_areas/Statistical_Areas_2010_withNames.shp'
#         stat = gpd.read_file(fid)
#         stat.rename(columns={'Id': 'stat_area'}, inplace=True)

#         # Use a projected CRS for accurate centroid calculation
#         projected_crs = "EPSG:5070"  # Albers Equal Area Conic
#         stat = stat.to_crs(projected_crs)
#         stat['centroid'] = stat.geometry.centroid  # Compute centroids
#     except Exception as e:
#         logger.error("Error loading stat area shapefile: %s", e, exc_info=True)
#         raise

#     # Ensure full_fleet_aggregated is in EPSG:4326 and apply the function
#     try:
#         df['stat_area'] = df.apply(
#             find_nearest_stat_area, stat_gdf=stat, axis=1)
#     except Exception as e:
#         logger.error("Error assigning statistical areas: %s", e, exc_info=True)
#         raise
#     return df


def find_closest_depth(df: pd.DataFrame) -> pd.DataFrame:
    """Assign closest bathymetric depth based on lat/lon."""
    logger.info("Assigning depth values to the data")

    fid = "data/bathymetry/gebco_2024_n46.0_s36.0_w-76.0_e-64.0.nc"
    try:
        ds = xr.open_dataset(fid)
    except Exception as e:
        logger.error("Error loading bathymetry file: %s", e, exc_info=True)
        raise

    unique_locs = (
        df[["latitude", "longitude"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    try:
        # 2. Vectorized nearest-neighbor lookup
        depths = ds["elevation"].sel(
            lat=xr.DataArray(unique_locs["latitude"], dims="points"),
            lon=xr.DataArray(unique_locs["longitude"], dims="points"),
            method="nearest",
        ).values

        unique_locs["inferred_depth"] = abs(depths).astype(int)

    except Exception as e:
        logger.error("Error assigning depth values: %s", e, exc_info=True)
        raise

    # 3. Merge back to full dataframe
    df = df.merge(
        unique_locs,
        on=["latitude", "longitude"],
        how="left",
    )

    return df

