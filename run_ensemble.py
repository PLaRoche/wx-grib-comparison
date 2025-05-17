import os
import sys
import logging
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import argparse
from download_ensemble import download_hrrr_gribs, download_gfs_gribs, download_icon_gribs, download_cmc_gribs, download_nam_gribs
from process_ensemble import process_hrrr_data, process_gfs_data, process_icon_data, process_cmc_data, process_nam_data
from analyze_ensemble import analyze_ensemble_data
from visualize_ensemble import create_ensemble_visualization

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_ensemble_analysis(skip_download=False, hours=72, models=None):
    """
    Main function to run the ensemble analysis workflow.
    
    Args:
        skip_download (bool): Whether to skip the download step
        hours (int): Number of forecast hours to download
        models (list): List of model names to download. If None, download all models.
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
        
        # Set forecast hours for each model
        hrrr_hours = min(18, hours)  # HRRR is available up to 18 hours ahead
        gfs_hours = min(72, hours)   # GFS is available up to 384 hours ahead (but let's use 72 for speed)
        icon_hours = min(72, hours)  # ICON is available up to 72 hours ahead
        cmc_hours = min(72, hours)   # CMC is available up to 72 hours ahead
        nam_hours = min(84, hours)   # NAM is available up to 84 hours ahead
        
        # Create output directory if it doesn't exist
        output_dir = "ensemble_output"
        os.makedirs(output_dir, exist_ok=True)
        
        # Download GRIB files if not skipped
        if not skip_download:
            if models is None or 'gfs' in models:
                logger.info("Downloading GFS data...")
                download_gfs_gribs(lat_min, lat_max, lon_min, lon_max, variables, hours=gfs_hours)
            
            if models is None or 'icon' in models:
                logger.info("Downloading ICON data...")
                download_icon_gribs(lat_min, lat_max, lon_min, lon_max, variables, hours=icon_hours)
            
            if models is None or 'cmc' in models:
                logger.info("Downloading CMC data...")
                download_cmc_gribs(lat_min, lat_max, lon_min, lon_max, variables, hours=cmc_hours)
            
            if models is None or 'hrrr' in models:
                logger.info("Downloading HRRR data...")
                download_hrrr_gribs(lat_min, lat_max, lon_min, lon_max, variables, hours=hrrr_hours)
            
            if models is None or 'nam' in models:
                logger.info("Downloading NAM data...")
                download_nam_gribs(lat_min, lat_max, lon_min, lon_max, variables, hours=nam_hours)
        
        # Process downloaded data
        logger.info("Processing downloaded data...")
        data_frames = []
        if models is None or 'hrrr' in models:
            hrrr_files = [os.path.join("gribs", "hrrr_gribs", f) for f in os.listdir("gribs/hrrr_gribs") if f.endswith(".grib2")]
            hrrr_data = process_hrrr_data(hrrr_files)
            data_frames.append(hrrr_data)
        if models is None or 'gfs' in models:
            gfs_files = [os.path.join("gribs", "gfs_gribs", f) for f in os.listdir("gribs/gfs_gribs") if f.endswith(".grib2")]
            gfs_data = process_gfs_data(gfs_files)
            data_frames.append(gfs_data)
        if models is None or 'icon' in models:
            icon_files = [os.path.join("gribs", "icon_gribs", f) for f in os.listdir("gribs/icon_gribs") if f.endswith(".grib2")]
            icon_data = process_icon_data(icon_files)
            data_frames.append(icon_data)
        if models is None or 'cmc' in models:
            cmc_files = [os.path.join("gribs", "cmc_gribs", f) for f in os.listdir("gribs/cmc_gribs") if f.endswith(".grib2")]
            cmc_data = process_cmc_data(cmc_files)
            data_frames.append(cmc_data)
        if models is None or 'nam' in models:
            nam_files = [os.path.join("gribs", "nam_gribs", f) for f in os.listdir("gribs/nam_gribs") if f.endswith(".grib2")]
            nam_data = process_nam_data(nam_files)
            data_frames.append(nam_data)
        
        # Combine all data
        if data_frames:
            ensemble_data = pd.concat(data_frames, ignore_index=True)
        else:
            ensemble_data = pd.DataFrame()
        
        # Analyze ensemble data
        logger.info("Analyzing ensemble data...")
        analysis_results = analyze_ensemble_data(ensemble_data)
        
        # Create visualizations
        logger.info("Creating visualizations...")
        create_ensemble_visualization(analysis_results, output_dir)
        
        logger.info("Ensemble analysis complete!")
        
    except Exception as e:
        logger.error(f"Error in ensemble analysis: {str(e)}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run ensemble analysis workflow.")
    parser.add_argument('--skip-download', action='store_true', help='Skip the download step and only process and visualize existing data')
    parser.add_argument('--hours', type=int, default=72, help='Number of hours of weather data to download (default: 72)')
    parser.add_argument('--grib', type=str, help='Comma-separated list of models to download (e.g., "gfs,icon,nam"). If not specified, all models will be downloaded.')
    args = parser.parse_args()
    
    # Parse the models argument
    models = None
    if args.grib:
        models = [model.strip().lower() for model in args.grib.split(',')]
        valid_models = ['gfs', 'icon', 'cmc', 'hrrr', 'nam']
        invalid_models = [model for model in models if model not in valid_models]
        if invalid_models:
            logger.error(f"Invalid model(s) specified: {', '.join(invalid_models)}")
            logger.error(f"Valid models are: {', '.join(valid_models)}")
            sys.exit(1)
    
    run_ensemble_analysis(skip_download=args.skip_download, hours=args.hours, models=models) 