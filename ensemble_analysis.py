import os
import glob
import xarray as xr
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

def analyze_and_plot():
    grib_dirs = {
        "GFS": "gfs_gribs",
        "ICON": "icon_gribs",
        "CMC": "cmc_gribs",
        "HRRR": "hrrr_gribs"
    }
    colors = {
        "GFS": "#1f77b4",
        "ICON": "#ff7f0e",
        "CMC": "#2ca02c",
        "HRRR": "#d62728"
    }
    data = {model: [] for model in grib_dirs.keys()}
    models_with_data = set()

    for model, grib_dir in grib_dirs.items():
        # Only process .grib2 files, ignore .idx files
        grib_files = sorted([f for f in glob.glob(os.path.join(grib_dir, "*.grib2")) if not f.endswith('.idx')])
        if not grib_files:
            logging.warning(f"No GRIB files found for {model} in {grib_dir}")
            continue

        for grib in grib_files:
            try:
                # Process temperature at 2m
                try:
                    ds_temp = xr.open_dataset(grib, engine="cfgrib", filter_by_keys={'typeOfLevel': 'heightAboveGround', 'level': 2})
                except Exception as e:
                    logging.warning(f"Could not process temperature for {grib}: {e}")
                    continue

                # Process wind at 10m
                try:
                    ds_wind = xr.open_dataset(grib, engine="cfgrib", filter_by_keys={'typeOfLevel': 'heightAboveGround', 'level': 10})
                except Exception as e:
                    logging.warning(f"Could not process wind for {grib}: {e}")
                    continue

                # Extract valid time robustly
                valid_time = None
                try:
                    if "valid_time" in ds_temp:
                        valid_time = pd.to_datetime(ds_temp["valid_time"].values)
                    elif "time" in ds_temp and "step" in ds_temp:
                        base_time = pd.to_datetime(ds_temp["time"].values)
                        steps = ds_temp["step"].values
                        if not hasattr(steps, '__len__') or isinstance(steps, str):
                            steps = [steps]
                        valid_time = base_time + pd.to_timedelta(steps, unit='s')
                    elif "time" in ds_temp:
                        valid_time = pd.to_datetime(ds_temp["time"].values)
                    else:
                        logging.warning(f"No valid time found in {grib}, skipping file. Keys: {list(ds_temp.keys())}")
                        continue
                except Exception as e:
                    logging.warning(f"Error extracting valid time from {grib}: {e}. Keys: {list(ds_temp.keys())}")
                    continue

                # Always make valid_time a 1D numpy array of pandas Timestamps
                if not hasattr(valid_time, '__len__') or isinstance(valid_time, str):
                    valid_time = [valid_time]
                valid_time = np.array([pd.to_datetime(vt) for vt in valid_time])

                def get_var_array(ds, keys, convert=None, offset=0):
                    for key in keys:
                        if key in ds:
                            arr = ds[key].values
                            # If 0-dim, make it 1-dim
                            if not hasattr(arr, '__len__') or isinstance(arr, str):
                                arr = [arr]
                            arr = np.array(arr)
                            if convert:
                                arr = convert(arr)
                            if offset:
                                arr = arr + offset
                            # If still scalar, make 1D
                            if arr.ndim == 0:
                                arr = arr[None]
                            # If more than 1D, flatten
                            arr = arr.flatten()
                            # If still more than needed, take first N
                            if len(arr) < len(valid_time):
                                arr = np.pad(arr, (0, len(valid_time)-len(arr)), constant_values=np.nan)
                            elif len(arr) > len(valid_time):
                                arr = arr[:len(valid_time)]
                            return arr
                    return np.full(len(valid_time), np.nan)

                # Get temperature at 2m
                temp_arr = get_var_array(ds_temp, ["t2m", "2t", "TMP_TGL_2"], convert=lambda x: x - 273.15 if (x > 200).any() else x)
                
                # Get wind at 10m
                u_arr = get_var_array(ds_wind, ["u10", "UGRD_TGL_10", "u"])
                v_arr = get_var_array(ds_wind, ["v10", "VGRD_TGL_10", "v"])
                speed_arr = np.sqrt(u_arr**2 + v_arr**2)
                direction_arr = (np.arctan2(u_arr, v_arr) * 180 / np.pi) % 360

                # Get precipitation (usually at surface level)
                try:
                    ds_precip = xr.open_dataset(grib, engine="cfgrib", filter_by_keys={'typeOfLevel': 'surface'})
                    prate_arr = get_var_array(ds_precip, ["prate", "PRATE_SFC_0", "pr"], convert=lambda x: x * 3600 if (x < 1).all() else x)
                except Exception as e:
                    logging.warning(f"Could not process precipitation for {grib}: {e}")
                    prate_arr = np.full(len(valid_time), np.nan)

                # Convert valid_time to pandas Timestamp objects before using as dictionary keys
                for i, vt in enumerate(valid_time):
                    if isinstance(vt, np.datetime64):
                        vt = pd.Timestamp(vt)
                    # Always extract a scalar for each variable
                    def safe_scalar(arr, i):
                        try:
                            return float(arr[i])
                        except Exception:
                            return np.nan
                    data[model].append({
                        "time": vt,
                        "hour": vt.hour,
                        "temperature_C": safe_scalar(temp_arr, i),
                        "precip_mm_hr": safe_scalar(prate_arr, i),
                        "wind_speed_m_s": safe_scalar(speed_arr, i),
                        "wind_dir_deg": safe_scalar(direction_arr, i)
                    })
                models_with_data.add(model)
            except Exception as e:
                logging.error(f"Failed to process {grib} for {model}: {e}")

    if not models_with_data:
        logging.error("No data available from any model. Please check downloads.")
        return

    dfs = {}
    for model in models_with_data:
        if data[model]:
            dfs[model] = pd.DataFrame(data[model]).sort_values("time")
        else:
            logging.warning(f"No valid data entries for {model}.")

    fig, axs = plt.subplots(4, 1, figsize=(15, 20), sharex=True)
    violin_handles = []
    violin_labels = []
    for idx, (model, df) in enumerate(dfs.items()):
        color = colors.get(model, "gray")
        if len(df) == 0:
            continue
        # Temperature
        v0 = sns.violinplot(data=df, x="hour", y="temperature_C", ax=axs[0], color=color, alpha=0.7)
        # Precipitation
        v1 = sns.violinplot(data=df, x="hour", y="precip_mm_hr", ax=axs[1], color=color, alpha=0.7)
        # Wind Speed
        v2 = sns.violinplot(data=df, x="hour", y="wind_speed_m_s", ax=axs[2], color=color, alpha=0.7)
        # Wind Direction
        v3 = sns.violinplot(data=df, x="hour", y="wind_dir_deg", ax=axs[3], color=color, alpha=0.7)
        # For manual legend
        violin_handles.append(plt.Line2D([0], [0], color=color, lw=8))
        violin_labels.append(model)

    axs[0].set_ylabel("Temperature (°C)")
    axs[0].set_title("Multi-Model Forecast Distribution: Halifax Harbour", pad=20)
    axs[0].grid(True, alpha=0.3)
    axs[0].legend(violin_handles, violin_labels, loc='upper right', bbox_to_anchor=(1.15, 1))
    axs[1].set_ylabel("Precipitation (mm/hr)")
    axs[1].grid(True, alpha=0.3)
    axs[1].legend(violin_handles, violin_labels, loc='upper right', bbox_to_anchor=(1.15, 1))
    axs[2].set_ylabel("Wind Speed (m/s)")
    axs[2].grid(True, alpha=0.3)
    axs[2].legend(violin_handles, violin_labels, loc='upper right', bbox_to_anchor=(1.15, 1))
    axs[3].set_ylabel("Wind Direction (°)")
    axs[3].set_xlabel("Hour of Day")
    axs[3].grid(True, alpha=0.3)
    axs[3].legend(violin_handles, violin_labels, loc='upper right', bbox_to_anchor=(1.15, 1))
    axs[3].set_ylim(0, 360)
    axs[3].set_xticks(range(24))
    axs[3].set_xticklabels([f"{h:02d}:00" for h in range(24)])
    plt.figtext(0.99, 0.01, f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", ha='right', va='bottom', fontsize=8)
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    analyze_and_plot() 