import pandas as pd
import xarray as xa
from db_connection import get_engine

def get_gfs_operational_forecasts(run_datetime: str) -> xa.Dataset:
    df_gfs = pd.read_sql(
        f"""
        SELECT *
        FROM "DATASET"."dnexr-structured-nws-ags-soame-weather_fcst_GFS_0Z-daily"
        WHERE "run_datetime" = '{run_datetime}'
        """,
        get_engine(),
    )
    df_gfs["forecast_date"] = pd.to_datetime(df_gfs["forecast_date"])

    columns = [
        "run_datetime",
        "forecast_date",
        "latitude",
        "longitude",
        "precipitation",
        "temperature_minimum",
        "temperature_maximum",
    ]

    da_gfs = (
        df_gfs[columns]
        .set_index(["run_datetime", "forecast_date", "latitude", "longitude"])
        .to_xarray()
    )
    return da_gfs

# Test function
def test_gfs_forecast_fetch():
    run_datetime = '2024-05-27'
    dataset = get_gfs_operational_forecasts(run_datetime)
    assert dataset is not None
    print(dataset)

if __name__ == '__main__':
    test_gfs_forecast_fetch()
