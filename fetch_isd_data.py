import os
import pandas as pd
import geopandas as gpd
from db_connection import get_engine
from sqlalchemy.exc import SQLAlchemyError
import sqlalchemy as sa

def get_ISD_data(ISD_start_date: str):
    engine = get_engine()
    dir_path = os.path.dirname(os.path.realpath(__file__))

    # Load the shapefile for US states
    gdf_ne_states = gpd.read_file(
        f"{dir_path}/references/ne_10m_admin_1_states_provinces.shp"
    )
    gdf_ne_states = gdf_ne_states[gdf_ne_states["admin"] == "United States of America"]

    # Connect to the PostgreSQL database
    pg_connection_string = "postgresql://postgres:1312@localhost:5432/USA_Project"
    pg_engine = sa.create_engine(pg_connection_string, connect_args={"connect_timeout": 10})

    # Function to read weather stations from PostgreSQL
    def read_stations_from_pg(pg_engine):
        query = 'SELECT * FROM "USA_Data"."Unique_Weather_Stations"'
        df = pd.read_sql_query(query, con=pg_engine)
        return df

    df_ISD_stations = read_stations_from_pg(pg_engine)

    # Convert to GeoDataFrame
    gdf_ISD_stations = gpd.GeoDataFrame(
        df_ISD_stations,
        geometry=gpd.points_from_xy(
            x=df_ISD_stations.LONGITUDE, y=df_ISD_stations.LATITUDE
        ),
    )

    gdf_ISD_stations = gdf_ISD_stations.set_crs(gdf_ne_states.crs)
    gdf_ISD_stations = gdf_ISD_stations.sjoin(
        gdf_ne_states[["name", "name_en", "geometry"]], predicate="within"
    ).rename(columns={"name_en": "state"})

    try:
        df_ISD_data = pd.read_sql(
            f"""
                SELECT
                "STATION" AS "station",
                "DATE" AS "date",
                (CAST(MAX("TEMPERATURE_MAXIMUM") AS FLOAT)-32)*(5/9) AS "temperature_maximum",
                (CAST(MIN("TEMPERATURE_MINIMUM") AS FLOAT)-32)*(5/9) AS "temperature_minimum",
                (CAST(SUM("PRECIPITATION") AS FLOAT))*25.4 AS precipitation
                FROM "LAB"."ISD_DAILY_CLEANED"
                WHERE "DATE" >= '{ISD_start_date}' AND "TEMPERATURE_MAXIMUM" <> 9999.9 AND "PRECIPITATION" <> 99.9
                GROUP BY "STATION", "DATE"
                ORDER BY "STATION", "DATE"
                """,
            engine,
        )
    except SQLAlchemyError as e:
        print(f"SQLAlchemyError: {e}")
        return None, None

    df_ISD_data["date"] = pd.to_datetime(df_ISD_data["date"])

    try:
        df_ISD_data_climat = pd.read_sql(
            f"""
                WITH daily AS (
                SELECT
                "STATION" AS "station",
                "DATE" AS "date",
                CAST(MAX("TEMPERATURE_MAXIMUM") AS FLOAT) AS "temperature_maximum",
                CAST(MIN("TEMPERATURE_MINIMUM") AS FLOAT) AS "temperature_minimum",
                CAST(SUM("PRECIPITATION") AS FLOAT) AS precipitation
                FROM "LAB"."ISD_DAILY_CLEANED"
                GROUP BY "STATION", "DATE"
                ORDER BY "STATION", "DATE")
                SELECT
                "station",
                EXTRACT(DOY FROM "date") AS "doy",
                AVG("temperature_maximum") AS "clim_temperature_maximum"
                FROM daily
                GROUP BY "station", EXTRACT(DOY FROM "date")
                """,
            engine,
        )
    except SQLAlchemyError as e:
        print(f"SQLAlchemyError: {e}")
        return None, None

    df_ISD_data_climat["date"] = pd.to_datetime(
        "2023-" + df_ISD_data_climat["doy"].astype("str"), format="%Y-%j"
    )

    df_ISD_data = df_ISD_data.merge(df_ISD_data_climat, on=["station"])

    return gdf_ISD_stations, df_ISD_data


# Test function
def test_isd_data_fetch():
    ISD_start_date = '2024-05-01'
    gdf_ISD_stations, df_ISD_data = get_ISD_data(ISD_start_date)
    if gdf_ISD_stations is None or df_ISD_data is None:
        print("Failed to fetch ISD data.")
    else:
        print("gdf_ISD_stations :")
        print(list(gdf_ISD_stations))
        print("df_ISD_data :")
        print(list(df_ISD_data))


if __name__ == '__main__':
    test_isd_data_fetch()