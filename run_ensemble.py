import os
import sys
from download_ensemble import (
    download_gfs_gribs,
    download_icon_gribs,
    download_cmc_gribs,
    download_hrrr_gribs
)
from ensemble_analysis import analyze_and_plot

def main():
    # Halifax Harbour region
    lat_min, lat_max = 44.5, 44.8
    lon_min, lon_max = -63.6, -63.4
    variables = ["u10", "v10", "t2m", "prate"]

    print("Step 1: Downloading forecast data...")
    try:
        download_gfs_gribs(lat_min, lat_max, lon_min, lon_max, variables)
        download_icon_gribs(lat_min, lat_max, lon_min, lon_max, variables)
        download_cmc_gribs(lat_min, lat_max, lon_min, lon_max, variables)
        download_hrrr_gribs(lat_min, lat_max, lon_min, lon_max, variables)
        print("Download complete!")
    except Exception as e:
        print(f"Error during download: {e}")
        sys.exit(1)

    print("\nStep 2: Analyzing and plotting data...")
    try:
        analyze_and_plot()
        print("Analysis complete!")
    except Exception as e:
        print(f"Error during analysis: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 