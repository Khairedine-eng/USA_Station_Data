import os
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import colors, image
import geopandas as gpd


def plot_gridded_data(da_grid, coordinates, title, **kwargs):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    gdf_ne_states = gpd.read_file(f"{dir_path}/references/ne_10m_admin_1_states_provinces.shp")
    gdf_ne_states = gdf_ne_states[gdf_ne_states["admin"] == "United States of America"]
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


def make_temp_plots(co_tmax, da_grid, day, run_date, model):
    local_folder = "assets"

    fig, ax = plot_gridded_data(
        da_grid["model_tmax"],
        co_tmax,
        f"{model} operational - {run_date}\n2m maximum temperature - {day} days",
        cmap="viridis",
        levels=np.arange(15, 42.5, 2.5),
        cbar_kwargs={"orientation": "horizontal", "label": "Temperature [C]"},
    )
    file = f"{model}-operational_{run_date}_USA_tmax_0-{day}.png"
    logo = image.imread("assets/Logo-H-Grey.png")
    fig.figimage(logo, 475, 280, zorder=3, alpha=0.5)
    local_file_path = os.path.join(local_folder, file)
    fig.savefig(local_file_path)

    fig, ax = plot_gridded_data(
        da_grid["tmax"],
        co_tmax,
        f"ISD - {run_date}\n2m maximum temperature - {day} days",
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
        f"{model} operational-ISD - {run_date}\n2m maximum temperature - {day} days",
        cmap="RdBu_r",
        levels=np.arange(-10, 11, 1),
        cbar_kwargs={"orientation": "horizontal", "label": "Temperature [C]"},
    )
    logo = image.imread("assets/Logo-H-Grey.png")
    fig.figimage(logo, 475, 280, zorder=3, alpha=0.5)
    file = f"{model}-operational-ISD_{run_date}_USA_tmax_0-{day}.png"
    local_file_path = os.path.join(local_folder, file)
    fig.savefig(local_file_path)


def make_precip_plots(co_precip, da_grid, day, run_date, model):
    local_folder = "assets"
    bounds = np.array([0, 5, 10, 20, 30, 40, 50, 70, 90, 120, 150, 200, 250])
    norm = colors.BoundaryNorm(boundaries=bounds, ncolors=256)
    cmap = "terrain_r"
    vmax_diff = round((da_grid["model_precipitation"] - da_grid["precipitation"]).max().values[()], -1)

    fig, ax = plot_gridded_data(
        da_grid["model_precipitation"],
        co_precip,
        f"{model} operational - {run_date}\nCumulative Precipitation - {day} days",
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
    file = f"{model}-operational_{run_date}_USA_precip_0-{day}.png"
    local_file_path = os.path.join(local_folder, file)
    fig.savefig(local_file_path)

    fig, ax = plot_gridded_data(
        da_grid["precipitation"],
        co_precip,
        f"ISD - {run_date}\nCumulative Precipitation - {day} days",
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
        f"{model} operational-ISD - {run_date}\nCumulative Precipitation - {day} days",
        cmap="RdBu",
        levels=np.arange(-vmax_diff, vmax_diff + 10, 10),
        cbar_kwargs={"orientation": "horizontal", "label": "Precipitation [mm]"},
    )
    logo = image.imread("assets/Logo-H-Grey.png")
    fig.figimage(logo, 475, 280, zorder=3, alpha=0.5)
    file = f"{model}-operational-ISD_{run_date}_USA_precip_0-{day}.png"
    local_file_path = os.path.join(local_folder, file)
    fig.savefig(local_file_path)
