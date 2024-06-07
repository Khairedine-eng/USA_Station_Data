from fetch_gfs_forecast import get_gfs_operational_forecasts
from fetch_ecmwf_forecast import get_ecmwf_operational_forecasts
from fetch_isd_data import get_ISD_data
from sample_model_isd_stations import sample_model_ISD_stations
from interpolate_model_to_stations import interpolate_model_to_stations
from Plt import make_temp_plots, make_precip_plots
import pandas as pd
import matplotlib.pyplot as plt

if __name__ == "__main__":
    day_ranges = [1, 2, 5, 7, 10, 15]
    run_date = (pd.to_datetime("now") - pd.DateOffset(days=3)).strftime("%Y-%m-%d")
    ISD_start_date = (pd.to_datetime("now") - pd.DateOffset(days=11)).strftime("%Y-%m-%d")

    # Fetch ISD data
    gdf_ISD_stations, df_ISD_data = get_ISD_data(ISD_start_date)

    for day in day_ranges:
        analysis_date = (pd.to_datetime("now") - pd.DateOffset(days=day)).strftime("%Y-%m-%d")

        # Fetch GFS and ECMWF forecasts
        da_gfs = get_gfs_operational_forecasts(run_date)
        da_ecmwf = get_ecmwf_operational_forecasts(run_date)

        # Sample model data at ISD station locations
        df_sampled_gfs = sample_model_ISD_stations(da_gfs, gdf_ISD_stations)
        df_sampled_ecmwf = sample_model_ISD_stations(da_ecmwf, gdf_ISD_stations)

        # Interpolate GFS model data to stations
        co_gfs_precip, co_gfs_tmax, da_gfs_grid = interpolate_model_to_stations(
            df_sampled_gfs, df_ISD_data, gdf_ISD_stations, 0, day
        )

        # Interpolate ECMWF model data to stations
        co_ecmwf_precip, co_ecmwf_tmax, da_ecmwf_grid = interpolate_model_to_stations(
            df_sampled_ecmwf, df_ISD_data, gdf_ISD_stations, 0, day
        )

        # Create temperature plots
        make_temp_plots(co_gfs_tmax, da_gfs_grid, day, analysis_date, "GFS")
        make_temp_plots(co_ecmwf_tmax, da_ecmwf_grid, day, analysis_date, "ECMWF")

        # Create precipitation plots
        make_precip_plots(co_gfs_precip, da_gfs_grid, day, analysis_date, "GFS")
        make_precip_plots(co_ecmwf_precip, da_ecmwf_grid, day, analysis_date, "ECMWF")

        plt.close("all")
