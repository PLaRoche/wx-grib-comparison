import os
import glob
import xarray as xr
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def analyze_and_plot():
    # Directories where GRIB2 files are stored
    grib_dirs = {
        "GFS": "gfs_gribs",
        "ICON": "icon_gribs",
        "CMC": "cmc_gribs",
        "HRRR": "hrrr_gribs"
    }

    # Lists to collect data
    data = {model: [] for model in grib_dirs.keys()}

    for model, grib_dir in grib_dirs.items():
        grib_files = sorted(glob.glob(os.path.join(grib_dir, "*.grib2")))
        for grib in grib_files:
            try:
                ds = xr.open_dataset(grib, engine="cfgrib")
                t = pd.to_datetime(ds.time.values)
                if "t2m" in ds:
                    temp = ds["t2m"].mean().item() - 273.15
                else:
                    temp = np.nan
                if "prate" in ds:
                    prate = ds["prate"].mean().item() * 3600
                else:
                    prate = np.nan
                if "u10" in ds and "v10" in ds:
                    u = ds["u10"].mean().item()
                    v = ds["v10"].mean().item()
                    speed = np.sqrt(u**2 + v**2)
                    direction = (np.arctan2(u, v) * 180 / np.pi) % 360
                else:
                    speed = np.nan
                    direction = np.nan
                data[model].append({
                    "time": t,
                    "temperature_C": temp,
                    "precip_mm_hr": prate,
                    "wind_speed_m_s": speed,
                    "wind_dir_deg": direction
                })
            except Exception as e:
                print(f"Failed to process {grib}: {e}")

    # Build DataFrames for each model
    dfs = {}
    for model, records in data.items():
        if records:
            dfs[model] = pd.DataFrame(records).sort_values("time")

    # Plotting
    fig, axs = plt.subplots(3, 1, figsize=(12, 10), sharex=True)
    for model, df in dfs.items():
        axs[0].plot(df["time"], df["temperature_C"], marker="o", label=model)
        axs[1].plot(df["time"], df["precip_mm_hr"], marker="o", label=model)
        axs[2].plot(df["time"], df["wind_speed_m_s"], marker="o", label=model)

    axs[0].set_ylabel("Temperature (°C)")
    axs[0].set_title("Multi-Model Forecast: Halifax Harbour")
    axs[0].grid(True)
    axs[0].legend()

    axs[1].set_ylabel("Precipitation (mm/hr)")
    axs[1].grid(True)
    axs[1].legend()

    axs[2].set_ylabel("Wind Speed (m/s)")
    axs[2].set_xlabel("Time")
    axs[2].grid(True)
    axs[2].legend()

    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    analyze_and_plot() 