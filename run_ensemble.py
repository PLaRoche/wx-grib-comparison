import os
import sys
import logging
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import argparse
from download_ensemble import download_hrrr_gribs, download_gfs_gribs, download_icon_gribs, download_cmc_gribs
from process_ensemble import process_hrrr_data, process_gfs_data, process_icon_data, process_cmc_data
from analyze_ensemble import analyze_ensemble_data
from visualize_ensemble import create_ensemble_visualization

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_ensemble_analysis(skip_download=False):
    """
    Main function to run the ensemble analysis workflow.
    """
    try:
        # Get current time in UTC
        current_time = datetime.utcnow()
        
        # Round down to nearest hour for base time
        base_time = current_time.replace(minute=0, second=0, microsecond=0)
        
        # Halifax Harbour region
        lat_min, lat_max = 44.5, 44.8
        lon_min, lon_max = -63.6, -63.4
        variables = ["u10", "v10", "t2m", "prate"]
        
        # HRRR is available up to 18 hours ahead
        hrrr_hours = 18
        # GFS is available up to 384 hours ahead (but let's use 72 for speed)
        gfs_hours = 72
        # ICON and CMC are also available up to 72 hours ahead
        icon_hours = 72
        cmc_hours = 72
        
        # Create output directory if it doesn't exist
        output_dir = "ensemble_output"
        os.makedirs(output_dir, exist_ok=True)
        
        if not skip_download:
            # Download HRRR data
            logger.info("Downloading HRRR data...")
            download_hrrr_gribs(lat_min, lat_max, lon_min, lon_max, variables, hours=hrrr_hours)
            
            # Download GFS data
            logger.info("Downloading GFS data...")
            download_gfs_gribs(lat_min, lat_max, lon_min, lon_max, variables, hours=gfs_hours)
            
            # Download ICON data
            logger.info("Downloading ICON data...")
            download_icon_gribs(lat_min, lat_max, lon_min, lon_max, variables, hours=icon_hours)
            
            # Download CMC data
            logger.info("Downloading CMC data...")
            download_cmc_gribs(lat_min, lat_max, lon_min, lon_max, variables, hours=cmc_hours)
        else:
            logger.info("Skipping download step as requested.")
        
        # Process HRRR data
        logger.info("Processing HRRR data...")
        hrrr_files = [os.path.join("hrrr_gribs", f) for f in os.listdir("hrrr_gribs") if f.endswith(".grib2")]
        hrrr_data = process_hrrr_data(hrrr_files)
        
        # Process GFS data
        logger.info("Processing GFS data...")
        gfs_files = [os.path.join("gfs_gribs", f) for f in os.listdir("gfs_gribs") if f.endswith(".grib2")]
        gfs_data = process_gfs_data(gfs_files)
        
        # Process ICON data
        logger.info("Processing ICON data...")
        icon_files = [os.path.join("icon_gribs", f) for f in os.listdir("icon_gribs") if f.endswith(".grib2")]
        icon_data = process_icon_data(icon_files)
        
        # Process CMC data
        logger.info("Processing CMC data...")
        cmc_files = [os.path.join("cmc_gribs", f) for f in os.listdir("cmc_gribs") if f.endswith(".grib2")]
        cmc_data = process_cmc_data(cmc_files)
        
        # Combine data
        ensemble_data = pd.concat([hrrr_data, gfs_data, icon_data, cmc_data], axis=0)
        
        # Analyze ensemble data
        logger.info("Analyzing ensemble data...")
        analysis_results = analyze_ensemble_data(ensemble_data)
        
        # Create visualization
        logger.info("Creating visualization...")
        create_ensemble_visualization(analysis_results, output_dir)
        
        logger.info("Ensemble analysis completed successfully!")
        
    except Exception as e:
        logger.error(f"Error in ensemble analysis: {str(e)}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run ensemble analysis workflow.")
    parser.add_argument('--skip-download', action='store_true', help='Skip the download step and only process and visualize existing data')
    args = parser.parse_args()
    run_ensemble_analysis(skip_download=args.skip_download) 