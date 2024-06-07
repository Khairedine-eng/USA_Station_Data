import pandas as pd
import xarray as xa
from db_connection import get_engine

def get_ecmwf_operational_forecasts(run_datetime: str) -> xa.Dataset:
    df_ecmwf = pd.read_sql(
        f"""
        SELECT *
        FROM "DATASET"."dnexr-ecmwf-ags-usa-weather_fcst_ecmwf_oper_0z-daily"
        WHERE "run_datetime" = '{run_datetime}'
        """,
        get_engine(),
    )

    df_ecmwf["forecast_date"] = pd.to_datetime(df_ecmwf["forecast_date"])

    if "precipitation_unit multiplier" in df_ecmwf.columns:
        df_ecmwf["precipitation"] *= df_ecmwf["precipitation_unit multiplier"]
    else:
        print("Warning: 'precipitation_unit_multiplier' column not found, using raw precipitation values.")

    columns = [
        "run_datetime",
        "forecast_date",
        "latitude",
        "longitude",
        "precipitation",
        "temperature_minimum",
        "temperature_maximum",
    ]

    da_ecmwf = (
        df_ecmwf[columns]
        .set_index(["run_datetime", "forecast_date", "latitude", "longitude"])
        .to_xarray()
    )

    return da_ecmwf

# Test function
def test_ecmwf_forecast_fetch():
    run_datetime = '2024-05-27'
    dataset = get_ecmwf_operational_forecasts(run_datetime)
    assert dataset is not None
    print(dataset)

if __name__ == '__main__':
    test_ecmwf_forecast_fetch()
