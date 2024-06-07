import os
import pandas as pd
import numpy as np
import xarray as xa
import rasterio
import rioxarray as rxa
import geopandas as gpd
import verde as vd
import pyproj
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import matplotlib.colors as colors
import matplotlib.image as image
import sqlalchemy as sa
from sqlalchemy import create_engine

local_folder = "assets"


def get_engine():
    engine = sa.create_engine(
        "snowflake://{user}:{password}@{account}/{database}".format(
            user="DNEXR-ABDELJAWED",
            password="1312Acab@",
            account="MQJSHRJ.GQ42038",
            database="DNEXR",
        )
    )

    return engine


def get_gfs_operational_forecasts(run_datetime: str) -> xa.Dataset:
    df_gfs = pd.read_sql(
        f"""
        SELECT
        *
        FROM "DATASET"."dnexr-nws-ags-usa-weather_fcst_GFS_0Z-daily"
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


def get_ecmwf_operational_forecasts(run_datetime: str) -> xa.Dataset:
    local_folder = "assets"
    df_ecmwf = pd.read_sql(
        f"""
        SELECT
        *
        FROM "DATASET"."dnexr-ecmwf-ags-usa-weather_fcst_ecmwf_oper_0z-daily"
        WHERE "run_datetime" = '{run_datetime}'
        """,
        get_engine(),
    )

    df_ecmwf["forecast_date"] = pd.to_datetime(df_ecmwf["forecast_date"])
    df_ecmwf["precipitation"] *= df_ecmwf["precipitation_unit multiplier"]

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


def get_ISD_data(ISD_start_date: str):
    local_folder = "assets"
    engine = get_engine()
    dir_path = os.path.dirname(os.path.realpath(__file__))
    # load geospatial reference data
    gdf_ne_states = gpd.read_file(
        f"{dir_path}/references/ne_10m_admin_1_states_provinces.shp"
    )
    gdf_ne_states = gdf_ne_states[gdf_ne_states["admin"] == "USA"]

    def read_stations_from_csv(csv_file_path):
        df = pd.read_csv(csv_file_path)
        return df

    csv_file_path = r'C:\Users\Khair-Edine\Desktop\USA_Weather _Station\data-1717068590224.csv'
    df_ISD_stations = read_stations_from_csv(csv_file_path)
    print(df_ISD_stations)

    gdf_ISD_stations = gdf = gpd.GeoDataFrame(
        df_ISD_stations,
        geometry=gpd.points_from_xy(
            x=df_ISD_stations.LONGITUDE, y=df_ISD_stations.LATITUDE
        ),
    )

    gdf_ISD_stations = gdf_ISD_stations.set_crs(gdf_ne_states.crs)
    gdf_ISD_stations = gdf_ISD_stations.sjoin(
        gdf_ne_states[["name", "name_en", "geometry"]], predicate="within"
    ).rename(columns={"name_en": "state"})

    df_ISD_data = pd.read_sql(
        f"""
        select
        "STATION_ID",
        "date",
        (CAST(MAX("MaxTemp") AS FLOAT)-32)*(5/9) AS "temperature_maximum",
        (CAST(MIN("MinTemp") AS FLOAT)-32)*(5/9) AS "temperature_minimum",
        (CAST(SUM("PRCIPITATION") AS FLOAT))*25.4 AS precipitation
        from
        "LAB"."isd_daily_cleaned"
        where "date" >= '2023-12-07' and "MaxTemp" <> 9999.9 and PRCIPITATION <> 99.9
        group by "STATION_ID", "date"
        order by "STATION_ID", "date"
        """,
        engine,
    )

    df_ISD_data["date"] = pd.to_datetime(df_ISD_data["date"])

    df_ISD_data_climat = pd.read_sql(
        f"""
        with daily as (
        select
        "STATION_ID",
        "date",
        CAST(MAX("MaxTemp") AS FLOAT) AS "temperature_maximum",
        CAST(MIN("MinTemp") AS FLOAT) AS "temperature_minimum",
        CAST(SUM("PRCIPITATION") AS FLOAT) AS precipitation
        from
        "LAB"."isd_daily_cleaned"
        group by "STATION_ID", "date"
        order BY "STATION_ID", "date")
        select
        "STATION_ID",
        extract(doy from "date") as "doy",
        avg("temperature_maximum") as "clim_temperature_maximum"
        from daily
        group by "STATION_ID", extract(doy from "date")
        """,
        engine,
    )

    df_ISD_data_climat["date"] = pd.to_datetime(
        "2023-" + df_ISD_data_climat["doy"].astype("str"), format="%Y-%j"
    )
    df_ISD_data = df_ISD_data.merge(df_ISD_data_climat, on=["station_id", "date"])

    return gdf_ISD_stations, df_ISD_data


def sample_model_ISD_stations(
        da_model: xa.Dataset, gdf_ISD_state_stations: gpd.GeoDataFrame
) -> pd.DataFrame:
    local_folder = "assets"
    variables = ["precipitation", "temperature_minimum", "temperature_maximum"]

    df_list = []

    for index, row in gdf_ISD_state_stations.iterrows():
        df_station = (
            da_model.sel(
                longitude=row["longitude"], latitude=row["latitude"], method="nearest"
            )
            .to_dataframe()[variables]
            .reset_index()
            .rename(
                columns=
                {
                    "precipitation": "model_precipitation",
                    "temperature_minimum": "model_temperature_minimum",
                    "temperature_maximum": "model_temperature_maximum",
                }
            )
        )

        df_station["station_id"] = row["station_id"]

        df_station["model_temperature_maximum"] -= 273.15
        df_station["model_temperature_minimum"] -= 273.15

        df_list.append(df_station)

    df_model_daily_data = pd.concat(df_list)

    df_model_daily_data["forecast_day"] = (
            df_model_daily_data["forecast_date"] - df_model_daily_data["run_datetime"]
    ).dt.days

    return df_model_daily_data


def interpolate_model_to_stations(
        df_model_daily_data, df_ISD_data, gdf_ISD_data, start_day, end_day
):
    local_folder = "assets"
    dir_path = os.path.dirname(os.path.realpath(__file__))
    # load geospatial reference data
    gdf_ne_country = gpd.read_file(f"{dir_path}/references/ne_10m_admin_1_states_provinces.shp")
    gdf_ne_country = gdf_ne_country[gdf_ne_country["admin"] == "USA"]

    df_join = df_ISD_data.merge(
        df_model_daily_data,
        left_on=["station_id", "date"],
        right_on=["station_id", "forecast_date"],
    )

    df_join = df_join.merge(
        gdf_ISD_data[["station_id", "state", "latitude", "longitude"]],
        on="station_id",
    )

    mask = (df_join["forecast_day"] >= start_day) & (df_join["forecast_day"] <= end_day)

    df_plot = (
        df_join[mask]
        .groupby(["station_id", "longitude", "latitude"], as_index=False)
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

    data_verde = df_plot.drop(columns=["station_id"])

    data_precip = data_verde[~data_verde.precipitation.isna()]
    data_tmax = data_verde[~data_verde.temperature_maximum.isna()]

    coordinates_precip = (data_precip.longitude.values, data_precip.latitude.values)
    coordinates_tmax = (data_tmax.longitude.values, data_tmax.latitude.values)

    region = (-74.5, -35.0, -53.77, 6.0)
    spacing = 0.1
    damping = 1e-9

    chain_precipitation = vd.Chain(
        [
            ("spline", vd.Spline(damping=damping)),
        ]
    )

    chain_precipitation_model = vd.Chain(
        [
            ("spline", vd.Spline(damping=damping)),
        ]
    )

    chain_temperature_maximum = vd.Chain(
        [
            ("spline", vd.Spline(damping=damping)),
        ]
    )

    chain_temperature_maximum_model = vd.Chain(
        [
            ("spline", vd.Spline(damping=damping)),
        ]
    )

    projection = pyproj.Proj(proj="merc", lat_ts=data_verde.latitude.mean())

    chain_precipitation.fit(projection(*coordinates_precip), data_precip.precipitation)

    chain_precipitation_model.fit(
        projection(*coordinates_precip), data_precip.model_precipitation
    )

    chain_temperature_maximum.fit(
        projection(*coordinates_tmax), data_tmax.temperature_maximum
    )
    chain_temperature_maximum_model.fit(
        projection(*coordinates_tmax), data_tmax.model_temperature_maximum
    )

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


def plot_gridded_data(da_grid: xa.DataArray, coordinates, title: str, **kwargs):
    local_folder = "assets"
    dir_path = os.path.dirname(os.path.realpath(__file__))
    # load geospatial reference data
    gdf_ne_states = gpd.read_file(
        f"{dir_path}/references/ne_10m_admin_1_states_provinces.shp"
    )
    gdf_ne_states = gdf_ne_states[gdf_ne_states["admin"] == "USA"]

    gdf_ne_country = gpd.read_file(f"{dir_path}/references/ne_10m_admin_1_states_provinces.shp")

    fig, ax = plt.subplots(1, 1, figsize=(8, 8))
    plt.tight_layout()

    da_grid.plot.contourf(ax=ax, **kwargs)
    ax.plot(*coordinates, ".k", markersize=1)
    gdf_ne_states.plot(ax=ax, facecolor="none")
    ax.set_title(title)
    ax.set_ylabel("")
    ax.set_xlabel("")
    gdf_ne_country.plot(facecolor="none", ax=ax, linestyle="dotted")

    xlim = [gdf_ne_states.total_bounds[0], gdf_ne_states.total_bounds[2]]
    ylim = [gdf_ne_states.total_bounds[1], gdf_ne_states.total_bounds[3]]

    ax.set_xlim(xlim)
    ax.set_ylim(ylim)

    return fig, ax


def make_gfs_temp_plots(co_tmax, da_grid, day, run_date):
    local_folder = "assets"
    fig, ax = plot_gridded_data(
        da_grid["model_tmax"],
        co_tmax,
        f"GFS operational - {run_date}\n2m maximum temperature  -{day} days",
        cmap="viridis",
        levels=np.arange(15, 42.5, 2.5),
        cbar_kwargs={"orientation": "horizontal", "label": "Temperature [C]"},
    )

    file = f"GFS-operational_{run_date}_USA_tmax_0-{day}.png"
    logo = image.imread("assets/Logo-H-Grey.png")
    fig.figimage(logo, 475, 280, zorder=3, alpha=0.5)

    local_file_path = os.path.join(local_folder, file)
    fig.savefig(local_file_path)

    fig, ax = plot_gridded_data(
        da_grid["tmax"],
        co_tmax,
        f"ISD - {run_date}\n2m maximum temperature  -{day} days",
        cmap="viridis",
        levels=np.arange(15, 42.5, 2.5),
        cbar_kwargs={"orientation": "horizontal", "label": "Temperature [C]"},
    )
    logo = image.imread("assets/Logo-H-Grey.png")
    fig.figimage(logo, 475, 280, zorder=3, alpha=0.5)
    file = f"ISD_{run_date}_USA_tmax_0-{day}.png"
    local_file_path = os.path.join(local_folder, file)
    fig.savefig(local_file_path)
    fig, ax = plot_gridded_data(
        da_grid["model_tmax"] - da_grid["tmax"],
        co_tmax,
        f"GFS operational-ISD - {run_date}\n2m maximum temperature  -{day} days",
        cmap="RdBu_r",
        levels=np.arange(-10, 11, 1),
        cbar_kwargs={"orientation": "horizontal", "label": "Temperature [C]"},
    )
    logo = image.imread("assets/Logo-H-Grey.png")
    fig.figimage(logo, 475, 280, zorder=3, alpha=0.5)

    file = f"GFS-operational-ISD_{run_date}_USA_tmax_0-{day}.png"
    local_file_path = os.path.join(local_folder, file)
    fig.savefig(local_file_path)


def make_ISD_precip_plots(da_grid, co_precip, day):
    pass


def make_gfs_operational_precip_plots(co_precip, da_grid, day, run_date):
    local_folder = "assets"
    vmax_diff = round(
        (da_grid["model_precipitation"] - da_grid["precipitation"]).max().values[()], -1
    )
    bounds = np.array([0, 5, 10, 20, 30, 40, 50, 70, 90, 120, 150, 200, 250])
    norm = colors.BoundaryNorm(boundaries=bounds, ncolors=256)
    cmap = "terrain_r"

    fig, ax = plot_gridded_data(
        da_grid["model_precipitation"],
        co_precip,
        f"GFS operational - {run_date}\nCumulative Precipitation  -{day} days",
        cmap=cmap,
        norm=norm,
        cbar_kwargs={
            "orientation": "horizontal",
            "label": "Precipitation [mm]",
            "ticks": bounds,
        },
    )
    logo = image.imread("assets/Logo-H-Grey.png")
    fig.figimage(logo, 475, 280, zorder=3, alpha=0.5)

    file = f"GFS-operational_{run_date}_USA_precip_0-{day}.png"
    local_file_path = os.path.join(local_folder, file)
    fig.savefig(local_file_path)

    fig, ax = plot_gridded_data(
        da_grid["precipitation"],
        co_precip,
        f"ISD - {run_date}\nCumulative Precipitation  -{day} days",
        cmap=cmap,
        norm=norm,
        cbar_kwargs={
            "orientation": "horizontal",
            "label": "Precipitation [mm]",
            "ticks": bounds,
        },
    )
    logo = image.imread("assets/Logo-H-Grey.png")
    fig.figimage(logo, 475, 280, zorder=3, alpha=0.5)

    file = f"ISD_{run_date}_USA_precip_0-{day}.png"
    local_file_path = os.path.join(local_folder, file)
    fig.savefig(local_file_path)

    fig, ax = plot_gridded_data(
        da_grid["model_precipitation"] - da_grid["precipitation"],
        co_precip,
        f"GFS operational-ISD - {run_date}\nCumulative Precipitation  -{day} days",
        cmap="RdBu",
        levels=np.arange(-vmax_diff, vmax_diff + 10, 10),
        cbar_kwargs={"orientation": "horizontal", "label": "Precipitation [mm]"},
    )
    logo = image.imread("assets/Logo-H-Grey.png")
    fig.figimage(logo, 475, 280, zorder=3, alpha=0.5)

    file = f"GFS-operational-ISD_{run_date}_USA_precip_0-{day}.png"
    local_file_path = os.path.join(local_folder, file)
    fig.savefig(local_file_path)


def make_ecmwf_operational_precip_plots(co_precip, da_grid, day, run_date):
    local_folder = "assets"
    bounds = np.array([0, 5, 10, 20, 30, 40, 50, 70, 90, 120, 150, 200, 250])
    norm = colors.BoundaryNorm(boundaries=bounds, ncolors=256)
    cmap = "terrain_r"

    vmax_diff = round(
        (da_grid["model_precipitation"] - da_grid["precipitation"]).max().values[()], -1
    )

    fig, ax = plot_gridded_data(
        da_grid["model_precipitation"],
        co_precip,
        f"ECMWF operational - {run_date}\nCumulative Precipitation  -{day} days",
        norm=norm,
        cmap=cmap,
        cbar_kwargs={
            "orientation": "horizontal",
            "label": "Precipitation [mm]",
            "ticks": bounds,
        },
    )
    logo = image.imread("assets/Logo-H-Grey.png")
    fig.figimage(logo, 475, 280, zorder=3, alpha=0.5)

    file = f"ECMWF-operational_{run_date}_USA_precip_0-{day}.png"
    local_file_path = os.path.join(local_folder, file)
    fig.savefig(local_file_path)
    # plt.close(fig)

    fig, ax = plot_gridded_data(
        da_grid["precipitation"],
        co_precip,
        f"ISD - {run_date}\nCumulative Precipitation  -{day} days",
        norm=norm,
        cmap=cmap,
        cbar_kwargs={
            "orientation": "horizontal",
            "label": "Precipitation [mm]",
            "ticks": bounds,
        },
    )
    logo = image.imread("assets/Logo-H-Grey.png")
    fig.figimage(logo, 475, 280, zorder=3, alpha=0.5)

    file = f"ISD_{run_date}_USA_precip_0-{day}.png"
    local_file_path = os.path.join(local_folder, file)
    fig.savefig(local_file_path)

    fig, ax = plot_gridded_data(
        da_grid["model_precipitation"] - da_grid["precipitation"],
        co_precip,
        f"ECMWF operational-ISD - {run_date}\nCumulative Precipitation  -{day} days",
        cmap="RdBu",
        levels=np.arange(-vmax_diff, vmax_diff + 10, 10),
        cbar_kwargs={"orientation": "horizontal", "label": "Precipitation [mm]"},
    )
    logo = image.imread("assets/Logo-H-Grey.png")
    fig.figimage(logo, 475, 280, zorder=3, alpha=0.5)

    file = f"ECMWF-operational-ISD_{run_date}_USA_precip_0-{day}.png"
    local_file_path = os.path.join(local_folder, file)
    fig.savefig(local_file_path)


def make_ecmwf_temp_plots(co_tmax, da_grid, day, run_date):
    local_folder = "assets"
    fig, ax = plot_gridded_data(
        da_grid["model_tmax"],
        co_tmax,
        f"ECMWF operational - {run_date}\n2m maximum temperature  -{day} days",
        cmap="viridis",
        levels=np.arange(15, 42.5, 2.5),
        cbar_kwargs={"orientation": "horizontal", "label": "Temperature [C]"},
    )
    logo = image.imread("assets/Logo-H-Grey.png")
    fig.figimage(logo, 475, 280, zorder=3, alpha=0.5)

    file = f"ECMWF-operational_{run_date}_USA_tmax_0-{day}.png"
    local_file_path = os.path.join(local_folder, file)
    fig.savefig(local_file_path)

    fig, ax = plot_gridded_data(
        da_grid["model_tmax"] - da_grid["tmax"],
        co_tmax,
        f"ECMWF operational-ISD - {run_date}\n2m maximum temperature  -{day} days",
        cmap="RdBu_r",
        levels=np.arange(-10, 11, 1),
        cbar_kwargs={"orientation": "horizontal", "label": "Temperature [C]"},
    )
    logo = image.imread("assets/Logo-H-Grey.png")
    fig.figimage(logo, 475, 280, zorder=3, alpha=0.5)

    file = f"ECMWF-operational-ISD_{run_date}_USA_tmax_0-{day}.png"
    local_file_path = os.path.join(local_folder, file)
    fig.savefig(local_file_path)


if __name__ == "__main__":
    day_ranges = [1, 2, 5, 7, 10, 15]
    date_with_time = pd.to_datetime("now") - pd.DateOffset(days=3)
    analysis_date = date_with_time.date()

    ISD_start_date = (
            pd.to_datetime(analysis_date) - pd.DateOffset(days=11)
    ).strftime("%Y-%m-%d")
    gdf_ISD_data, df_ISD_data = get_ISD_data(ISD_start_date)

    for day in day_ranges:
        run_date = (
                pd.to_datetime(analysis_date) - pd.DateOffset(days=day)
        ).strftime("%Y-%m-%d")

        print(f"Date: {analysis_date} - {run_date} - {day}")

        da_gfs = get_gfs_operational_forecasts(run_date)
        da_ecmwf = get_ecmwf_operational_forecasts(run_date)

        df_gfs_station = sample_model_ISD_stations(da_gfs, gdf_ISD_data)

        df_ecmwf_station = sample_model_ISD_stations(da_ecmwf, gdf_ISD_data)

        co_gfs_precip, co_gfs_tmax, da_gfs_grid = interpolate_model_to_stations(
            df_gfs_station, df_ISD_data, gdf_ISD_data, 0, day
        )
        (
            co_ecmwf_precip,
            co_ecmwf_tmax,
            da_ecmwf_grid,
        ) = interpolate_model_to_stations(
            df_ecmwf_station, df_ISD_data, gdf_ISD_data, 0, day
        )
        make_gfs_temp_plots(co_gfs_tmax, da_gfs_grid, day, analysis_date)

        make_ecmwf_temp_plots(co_ecmwf_tmax, da_ecmwf_grid, day, analysis_date)

        make_gfs_operational_precip_plots(
            co_gfs_precip, da_gfs_grid, day, analysis_date
        )

        make_ecmwf_operational_precip_plots(
            co_ecmwf_precip, da_ecmwf_grid, day, analysis_date
        )

        plt.close("all")