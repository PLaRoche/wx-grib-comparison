import os
import glob
import xarray as xr
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import matplotlib.dates as mdates

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

    # Track which models have data
    models_with_data = set()

    for model, grib_dir in grib_dirs.items():
        grib_files = sorted(glob.glob(os.path.join(grib_dir, "*.grib2")))
        if not grib_files:
            print(f"No GRIB files found for {model}")
            continue
            
        for grib in grib_files:
            try:
                ds = xr.open_dataset(grib, engine="cfgrib")
                t = pd.to_datetime(ds.time.values)
                
                # Extract data with error handling
                try:
                    temp = ds["t2m"].mean().item() - 273.15
                except:
                    temp = np.nan
                    
                try:
                    prate = ds["prate"].mean().item() * 3600  # Convert to mm/hr
                except:
                    prate = np.nan
                    
                try:
                    u = ds["u10"].mean().item()
                    v = ds["v10"].mean().item()
                    speed = np.sqrt(u**2 + v**2)
                    direction = (np.arctan2(u, v) * 180 / np.pi) % 360
                except:
                    speed = np.nan
                    direction = np.nan
                    
                data[model].append({
                    "time": t,
                    "temperature_C": temp,
                    "precip_mm_hr": prate,
                    "wind_speed_m_s": speed,
                    "wind_dir_deg": direction
                })
                models_with_data.add(model)
            except Exception as e:
                print(f"Failed to process {grib}: {e}")

    if not models_with_data:
        print("No data available from any model")
        return

    # Build DataFrames for each model
    dfs = {}
    for model in models_with_data:
        if data[model]:
            dfs[model] = pd.DataFrame(data[model]).sort_values("time")

    # Create figure with 4 subplots
    fig, axs = plt.subplots(4, 1, figsize=(15, 12), sharex=True)
    
    # Color scheme for models
    colors = {
        "GFS": "#1f77b4",
        "ICON": "#ff7f0e",
        "CMC": "#2ca02c",
        "HRRR": "#d62728"
    }

    # Plot each variable
    for model, df in dfs.items():
        color = colors.get(model, "gray")
        
        # Temperature
        axs[0].plot(df["time"], df["temperature_C"], 
                   marker="o", label=model, color=color, alpha=0.7)
        
        # Precipitation
        axs[1].bar(df["time"], df["precip_mm_hr"], 
                  label=model, color=color, alpha=0.7)
        
        # Wind Speed
        axs[2].plot(df["time"], df["wind_speed_m_s"], 
                   marker="o", label=model, color=color, alpha=0.7)
        
        # Wind Direction
        axs[3].plot(df["time"], df["wind_dir_deg"], 
                   marker="o", label=model, color=color, alpha=0.7)

    # Format temperature plot
    axs[0].set_ylabel("Temperature (°C)")
    axs[0].set_title("Multi-Model Forecast: Halifax Harbour", pad=20)
    axs[0].grid(True, alpha=0.3)
    axs[0].legend(loc='upper right', bbox_to_anchor=(1.15, 1))

    # Format precipitation plot
    axs[1].set_ylabel("Precipitation (mm/hr)")
    axs[1].grid(True, alpha=0.3)
    axs[1].legend(loc='upper right', bbox_to_anchor=(1.15, 1))

    # Format wind speed plot
    axs[2].set_ylabel("Wind Speed (m/s)")
    axs[2].grid(True, alpha=0.3)
    axs[2].legend(loc='upper right', bbox_to_anchor=(1.15, 1))

    # Format wind direction plot
    axs[3].set_ylabel("Wind Direction (°)")
    axs[3].set_xlabel("Time")
    axs[3].grid(True, alpha=0.3)
    axs[3].legend(loc='upper right', bbox_to_anchor=(1.15, 1))
    
    # Set y-axis limits for wind direction
    axs[3].set_ylim(0, 360)
    
    # Format x-axis
    axs[3].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
    plt.xticks(rotation=45)

    # Add timestamp
    plt.figtext(0.99, 0.01, f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", 
                ha='right', va='bottom', fontsize=8)

    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    analyze_and_plot() 