import pandas as pd
import xarray as xa
import geopandas as gpd

def sample_model_ISD_stations(da_model: xa.Dataset, gdf_ISD_state_stations: gpd.GeoDataFrame) -> pd.DataFrame:
    variables = ["precipitation", "temperature_minimum", "temperature_maximum"]
    df_list = []

    for index, row in gdf_ISD_state_stations.iterrows():
        df_station = (
            da_model.sel(
                longitude=row["LONGITUDE"], latitude=row["LATITUDE"], method="nearest"
            )
            .to_dataframe()[variables]
            .reset_index()
            .rename(
                columns={
                    "precipitation": "model_precipitation",
                    "temperature_minimum": "model_temperature_minimum",
                    "temperature_maximum": "model_temperature_maximum",
                }
            )
        )

        df_station["station"] = row["station"]
        df_station["model_temperature_maximum"] -= 273.15  # Convert from Kelvin to Celsius
        df_station["model_temperature_minimum"] -= 273.15  # Convert from Kelvin to Celsius

        df_list.append(df_station)

    df_model_daily_data = pd.concat(df_list)
    df_model_daily_data["forecast_day"] = (df_model_daily_data["forecast_date"] - df_model_daily_data["run_datetime"]).dt.days

    return df_model_daily_data

# Test function
def test_sample_model_ISD_stations():
    import fetch_gfs_forecast
    import fetch_isd_data

    run_date = '2024-05-27'
    ISD_start_date = '2024-05-01'

    da_gfs = fetch_gfs_forecast.get_gfs_operational_forecasts(run_date)
    gdf_ISD_stations, df_ISD_data = fetch_isd_data.get_ISD_data(ISD_start_date)

    if da_gfs is not None and gdf_ISD_stations is not None:
        df_sampled = sample_model_ISD_stations(da_gfs, gdf_ISD_stations)
        print(df_sampled.head())

if __name__ == '__main__':
    test_sample_model_ISD_stations()
