import os
import pandas as pd
import geopandas as gpd
import xarray as xa
import pyproj
import verde as vd
import rioxarray

def interpolate_model_to_stations(df_model_daily_data, df_ISD_data, gdf_ISD_data, start_day, end_day):
    # Load geospatial reference data
    gdf_ne_country = gpd.read_file(f"{os.path.dirname(os.path.realpath(__file__))}/references/ne_10m_admin_1_states_provinces.shp")
    gdf_ne_country = gdf_ne_country[gdf_ne_country["admin"] == "United States of America"]

    # Standardize station IDs
    df_model_daily_data['station'] = df_model_daily_data['station'].astype(str).str.zfill(11)
    df_ISD_data['station'] = df_ISD_data['station'].astype(str).str.zfill(11)
    gdf_ISD_data['station'] = gdf_ISD_data['station'].astype(str).str.zfill(11)

    # Print debug information
    #print("df_model_daily_data columns:", df_model_daily_data.columns)
    #print("df_ISD_data columns:", df_ISD_data.columns)
    #print("gdf_ISD_data columns:", gdf_ISD_data.columns)

    # Print uni que stations and date ranges for debugging
    #print("Unique stations in df_model_daily_data:", df_model_daily_data['station'].unique())
    #print("Unique stations in df_ISD_data:", df_ISD_data['station'].unique())
    #print("Date range in df_model_daily_data:", df_model_daily_data['forecast_date'].min(), "-", df_model_daily_data['forecast_date'].max())
    #print("Date range in df_ISD_data:", df_ISD_data['date_x'].min(), "-", df_ISD_data['date_x'].max())

    # Merge model and ISD data
    df_join = df_ISD_data.merge(
        df_model_daily_data,
        left_on=["station", "date_x"],
        right_on=["station", "forecast_date"],
    )

    # Print the shape and head of df_join to debug
    #print("df_join shape:", df_join.shape)
    #print("df_join head:\n", df_join.head())

    df_join = df_join.merge(
        gdf_ISD_data[["station", "state", "LATITUDE", "LONGITUDE"]],
        on="station",
    )

    # Print the shape and head of df_join after the second merge to debug
    #print("df_join after second merge shape:", df_join.shape)
    #print("df_join after second merge head:\n", df_join.head())

    mask = (df_join["forecast_day"] >= start_day) & (df_join["forecast_day"] <= end_day)
    df_plot = (
        df_join[mask]
        .groupby(["station", "LONGITUDE", "LATITUDE"], as_index=False)
        .agg(
            {
                "precipitation": "sum",
                "model_precipitation": "sum",
                "temperature_maximum": "mean",
                "clim_temperature_maximum": "mean",
                "model_temperature_maximum": "mean",
            }
        )
    )

    # Print the shape and head of df_plot to debug
    #print("df_plot shape:", df_plot.shape)
    #print("df_plot head:\n", df_plot.head())

    data_verde = df_plot.drop(columns=["station"])

    # Print the shape and head of data_verde to debug
    #print("data_verde shape:", data_verde.shape)
    #print("data_verde head:\n", data_verde.head())

    # Additional debug information
    #print("data_verde LATITUDE isna sum:", data_verde["LATITUDE"].isna().sum())
    #print("data_verde LONGITUDE isna sum:", data_verde["LONGITUDE"].isna().sum())

    data_precip = data_verde[~data_verde.precipitation.isna()]
    data_tmax = data_verde[~data_verde.temperature_maximum.isna()]

    coordinates_precip = (data_precip.LONGITUDE.values, data_precip.LATITUDE.values)
    coordinates_tmax = (data_tmax.LONGITUDE.values, data_tmax.LATITUDE.values)

    region = (-124.8, -80.4, 29.2, 50.0)
    spacing = 0.1
    damping = 1e-9

    chain_precipitation = vd.Chain([("spline", vd.Spline(damping=damping))])
    chain_precipitation_model = vd.Chain([("spline", vd.Spline(damping=damping))])
    chain_temperature_maximum = vd.Chain([("spline", vd.Spline(damping=damping))])
    chain_temperature_maximum_model = vd.Chain([("spline", vd.Spline(damping=damping))])

    # Ensure data_verde is not empty and contains valid latitude values
    if data_verde.empty or data_verde.LATITUDE.isna().all():
        raise ValueError("data_verde is empty or contains all NaN latitude values.")
    else:
        projection = pyproj.Proj(proj="merc", lat_ts=data_verde.LATITUDE.mean())

    chain_precipitation.fit(projection(*coordinates_precip), data_precip.precipitation)
    chain_precipitation_model.fit(projection(*coordinates_precip), data_precip.model_precipitation)
    chain_temperature_maximum.fit(projection(*coordinates_tmax), data_tmax.temperature_maximum)
    chain_temperature_maximum_model.fit(projection(*coordinates_tmax), data_tmax.model_temperature_maximum)

    grid_precipitation = chain_precipitation.grid(
        region=region,
        spacing=spacing,
        projection=projection,
        dims=["latitude", "longitude"],
        data_names="precipitation",
    )

    grid_precipitation_model = chain_precipitation_model.grid(
        region=region,
        spacing=spacing,
        projection=projection,
        dims=["latitude", "longitude"],
        data_names="model_precipitation",
    )

    grid_tmax = chain_temperature_maximum.grid(
        region=region,
        spacing=spacing,
        projection=projection,
        dims=["latitude", "longitude"],
        data_names="tmax",
    )

    grid_tmax_model = chain_temperature_maximum_model.grid(
        region=region,
        spacing=spacing,
        projection=projection,
        dims=["latitude", "longitude"],
        data_names="model_tmax",
    )

    grid_tmax.rio.write_crs("EPSG:4326", inplace=True)
    grid_tmax = grid_tmax.rio.clip(gdf_ne_country.geometry.values)

    grid_tmax_model.rio.write_crs("EPSG:4326", inplace=True)
    grid_tmax_model = grid_tmax_model.rio.clip(gdf_ne_country.geometry.values)

    grid_precipitation.rio.write_crs("EPSG:4326", inplace=True)
    grid_precipitation = grid_precipitation.rio.clip(gdf_ne_country.geometry.values)

    grid_precipitation_model.rio.write_crs("EPSG:4326", inplace=True)
    grid_precipitation_model = grid_precipitation_model.rio.clip(
        gdf_ne_country.geometry.values
    )

    return (
        coordinates_precip,
        coordinates_tmax,
        xa.merge(
            [grid_tmax, grid_tmax_model, grid_precipitation, grid_precipitation_model]
        ),
    )

# Test function
def test_interpolate_model_to_stations():
    import pandas as pd
    from fetch_gfs_forecast import get_gfs_operational_forecasts
    from fetch_isd_data import get_ISD_data
    from sample_model_isd_stations import sample_model_ISD_stations

    # Set the run date and ISD start date
    run_date = (pd.to_datetime("now") - pd.DateOffset(days=3)).strftime("%Y-%m-%d")
    ISD_start_date = (pd.to_datetime("now") - pd.DateOffset(days=11)).strftime("%Y-%m-%d")

    # Fetch GFS operational forecasts
    da_gfs = get_gfs_operational_forecasts(run_date)

    # Fetch ISD data
    gdf_ISD_stations, df_ISD_data = get_ISD_data(ISD_start_date)

    if gdf_ISD_stations is not None and df_ISD_data is not None:
        # Sample model data at ISD station locations
        df_sampled_gfs = sample_model_ISD_stations(da_gfs, gdf_ISD_stations)

        # Print debug information
        #print("df_sampled_gfs station and forecast_date columns:\n", df_sampled_gfs[['station', 'forecast_date']].head())
        #print("df_ISD_data station and date_x columns:\n", df_ISD_data[['station', 'date_x']].head())

        # Interpolate model data to stations
        coordinates_precip, coordinates_tmax, da_grid = interpolate_model_to_stations(
            df_sampled_gfs, df_ISD_data, gdf_ISD_stations, 0, 5
        )
        print("Interpolation successful")

if __name__ == '__main__':
    test_interpolate_model_to_stations()
