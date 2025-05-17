import os
import sys
import logging
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from download_ensemble import download_hrrr_gribs, download_gfs_gribs
from process_ensemble import process_hrrr_data, process_gfs_data
from analyze_ensemble import analyze_ensemble_data
from visualize_ensemble import create_ensemble_visualization

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_ensemble_analysis():
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
        
        # Create output directory if it doesn't exist
        output_dir = "ensemble_output"
        os.makedirs(output_dir, exist_ok=True)
        
        # Download HRRR data
        logger.info("Downloading HRRR data...")
        download_hrrr_gribs(lat_min, lat_max, lon_min, lon_max, variables, hours=hrrr_hours)
        
        # Download GFS data
        logger.info("Downloading GFS data...")
        download_gfs_gribs(lat_min, lat_max, lon_min, lon_max, variables, hours=gfs_hours)
        
        # Process HRRR data
        logger.info("Processing HRRR data...")
        hrrr_files = [os.path.join("hrrr_gribs", f) for f in os.listdir("hrrr_gribs") if f.endswith(".grib2")]
        hrrr_data = process_hrrr_data(hrrr_files)
        
        # Process GFS data
        logger.info("Processing GFS data...")
        gfs_files = [os.path.join("gfs_gribs", f) for f in os.listdir("gfs_gribs") if f.endswith(".grib2")]
        gfs_data = process_gfs_data(gfs_files)
        
        # Combine data
        ensemble_data = pd.concat([hrrr_data, gfs_data], axis=0)
        
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
    run_ensemble_analysis() 