import os
import requests
from datetime import datetime, timedelta
import bz2
import re
from bs4 import BeautifulSoup
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Model resolution configurations
MODEL_RESOLUTIONS = {
    'gfs': {
        '0.25': {
            'url': 'https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl',
            'file_pattern': 'gfs.t{run_str}z.pgrb2.0p25.f{fh:03d}',
            'dir_pattern': '/gfs.{date_str}/{run_str}/atmos'
        },
        '0.5': {
            'url': 'https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p50.pl',
            'file_pattern': 'gfs.t{run_str}z.pgrb2.0p50.f{fh:03d}',
            'dir_pattern': '/gfs.{date_str}/{run_str}/atmos'
        },
        '1.0': {
            'url': 'https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_1p00.pl',
            'file_pattern': 'gfs.t{run_str}z.pgrb2.1p00.f{fh:03d}',
            'dir_pattern': '/gfs.{date_str}/{run_str}/atmos'
        }
    },
    'icon': {
        '13km': {
            'url': 'https://opendata.dwd.de/weather/nwp/icon/grib/global/icon_global_icosahedral_single-level',
            'file_pattern': 'icon_global_icosahedral_single-level_{date_str}{run_str}_{fh:03d}_{var_name}.grib2.bz2'
        },
        '7km': {
            'url': 'https://opendata.dwd.de/weather/nwp/icon/grib/regional/icon_eu_icosahedral_single-level',
            'file_pattern': 'icon_eu_icosahedral_single-level_{date_str}{run_str}_{fh:03d}_{var_name}.grib2.bz2'
        }
    },
    'cmc': {
        '25km': {
            'url': 'https://dd.weather.gc.ca/model_gem_global/25km/grib2/lat_lon',
            'file_pattern': 'CMC_glb_{var_name}_latlon.24x.24_{date_str}{run_str}_P{fh:03d}.grib2'
        },
        '15km': {
            'url': 'https://dd.weather.gc.ca/model_gem_regional/15km/grib2/lat_lon',
            'file_pattern': 'CMC_reg_{var_name}_latlon.15x.15_{date_str}{run_str}_P{fh:03d}.grib2'
        }
    },
    'hrrr': {
        '3km': {
            'url': 'https://nomads.ncep.noaa.gov/cgi-bin/filter_hrrr_2d.pl',
            'file_pattern': 'hrrr.t{run_str}z.wrfsfcf{fh:02d}.grib2',
            'dir_pattern': '/hrrr.{date_str}/conus'
        },
        '13km': {
            'url': 'https://nomads.ncep.noaa.gov/cgi-bin/filter_rap_2d.pl',
            'file_pattern': 'rap.t{run_str}z.wrfsfcf{fh:02d}.grib2',
            'dir_pattern': '/rap.{date_str}'
        }
    }
}

def get_current_run_time():
    """Get the current time rounded down to the nearest model run hour"""
    now = datetime.utcnow()
    # Round down to nearest 6 hours (00, 06, 12, 18)
    hour = (now.hour // 6) * 6
    return now.replace(hour=hour, minute=0, second=0, microsecond=0)

def get_latest_gfs_run():
    """Find the latest available GFS run"""
    base_url = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod"
    current_run = get_current_run_time()
    
    # Try the last 3 days
    for days_back in range(3):
        date = current_run - timedelta(days=days_back)
        date_str = date.strftime("%Y%m%d")
        
        # Try each run hour (00, 06, 12, 18)
        for hour in [18, 12, 6, 0]:
            run_str = f"{hour:02d}"
            url = f"{base_url}/gfs.{date_str}/{run_str}/atmos"
            try:
                r = requests.get(url, timeout=10)
                if r.status_code == 200:
                    # Check if the run files exist
                    test_file = f"gfs.t{run_str}z.pgrb2.0p25.f000"
                    if test_file in r.text:
                        run_time = date.replace(hour=hour, minute=0, second=0, microsecond=0)
                        logger.info(f"Found GFS run from {date_str} {run_str}Z")
                        return run_time
                    else:
                        logger.debug(f"GFS run {date_str} {run_str}Z exists but test file not found")
                else:
                    logger.debug(f"GFS run {date_str} {run_str}Z not found (status {r.status_code})")
            except Exception as e:
                logger.error(f"Error checking GFS run {date_str} {run_str}Z: {str(e)}")
    logger.warning("No available GFS runs found in the last 3 days")
    return None

def get_latest_icon_run():
    """Find the latest available ICON run"""
    base_url = "https://opendata.dwd.de/weather/nwp/icon/grib"
    current_run = get_current_run_time()
    
    # Try the last 3 days
    for days_back in range(3):
        date = current_run - timedelta(days=days_back)
        date_str = date.strftime("%Y%m%d")
        
        # Try each run hour (00, 06, 12, 18)
        for hour in [18, 12, 6, 0]:
            run_str = f"{hour:02d}"
            
            # Check for global model availability
            url = f"{base_url}/global/{date_str}{run_str}"
            try:
                r = requests.get(url, timeout=10)
                if r.status_code == 200:
                    soup = BeautifulSoup(r.text, 'html.parser')
                    # Look for links to forecast hours
                    links = soup.find_all('a')
                    for link in links:
                        if 'icon_global_regular' in link.get('href', ''):
                            run_time = date.replace(hour=hour, minute=0, second=0, microsecond=0)
                            logger.info(f"Found ICON run from {date_str} {run_str}Z")
                            return run_time
                    logger.debug(f"ICON global run {date_str} {run_str}Z exists but no model files found")
                else:
                    logger.debug(f"ICON global run {date_str} {run_str}Z not found (status {r.status_code})")
            except Exception as e:
                logger.error(f"Error checking ICON run {date_str} {run_str}Z: {str(e)}")
    
    logger.warning("No available ICON runs found in the last 3 days")
    return None

def get_latest_cmc_run():
    """Find the latest available CMC run"""
    base_url = "https://dd.weather.gc.ca/model_gem_global/15km/grib2"
    current_run = get_current_run_time()
    
    # Try the last 3 days
    for days_back in range(3):
        date = current_run - timedelta(days=days_back)
        date_str = date.strftime("%Y%m%d")
        
        # Try each run hour (00, 06, 12, 18)
        for hour in [18, 12, 6, 0]:
            run_str = f"{hour:02d}"
            
            # Check the main run page first
            try:
                url = f"{base_url}/{hour:02d}"
                r = requests.get(url, timeout=10)
                if r.status_code == 200:
                    # Check for any forecast file for this run
                    soup = BeautifulSoup(r.text, 'html.parser')
                    links = soup.find_all('a')
                    
                    # Look for .grib2 files
                    for link in links:
                        if link.get('href', '').endswith('.grib2') and date_str in link.get('href', ''):
                            run_time = date.replace(hour=hour, minute=0, second=0, microsecond=0)
                            logger.info(f"Found CMC run from {date_str} {run_str}Z")
                            return run_time
                    
                    logger.debug(f"CMC run {date_str} {run_str}Z directory exists but no forecast files found")
                else:
                    logger.debug(f"CMC run hour {hour:02d} not found (status {r.status_code})")
            except Exception as e:
                logger.error(f"Error checking CMC run {date_str} {run_str}Z: {str(e)}")
    
    logger.warning("No available CMC runs found in the last 3 days")
    return None

def get_latest_hrrr_run():
    """Find the latest available HRRR run"""
    base_url = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/hrrr/prod"
    current_run = get_current_run_time()
    
    # Try the last 2 days (HRRR is more frequent)
    for days_back in range(2):
        date = current_run - timedelta(days=days_back)
        date_str = date.strftime("%Y%m%d")
        
        # Try each run hour (23 to 0)
        for hour in range(23, -1, -1):
            run_str = f"{hour:02d}"
            url = f"{base_url}/hrrr.{date_str}/conus"
            try:
                r = requests.get(url, timeout=10)
                if r.status_code == 200:
                    # Check if the run files exist
                    test_file = f"hrrr.t{run_str}z.wrfsfcf00.grib2"
                    if test_file in r.text:
                        run_time = date.replace(hour=hour, minute=0, second=0, microsecond=0)
                        logger.info(f"Found HRRR run from {date_str} {run_str}Z")
                        return run_time
                    else:
                        logger.debug(f"HRRR run {date_str} {run_str}Z exists but test file not found")
                else:
                    logger.debug(f"HRRR run {date_str} {run_str}Z not found (status {r.status_code})")
            except Exception as e:
                logger.error(f"Error checking HRRR run {date_str} {run_str}Z: {str(e)}")
    logger.warning("No available HRRR runs found in the last 2 days")
    return None

def download_gfs_gribs(lat_min, lat_max, lon_min, lon_max, variables, hours=72, out_dir="gfs_gribs", resolution='0.25'):
    os.makedirs(out_dir, exist_ok=True)
    config = MODEL_RESOLUTIONS['gfs'][resolution]
    base_url = config['url']
    
    # Get latest available run
    run_time = get_latest_gfs_run()
    if run_time is None:
        logger.error("No available GFS runs found in the last 3 days")
        return
        
    run_date = run_time.strftime("%Y%m%d")
    run_str = run_time.strftime("%H")
    logger.info(f"Using GFS {resolution}° run from {run_time.strftime('%Y-%m-%d %H:%M UTC')}")
    
    for fh in range(0, hours+1, 1):  # hourly steps
        params = {
            "file": config['file_pattern'].format(run_str=run_str, fh=fh),
            "lev_2_m_above_ground": "on",
            "lev_10_m_above_ground": "on",
            "var_UGRD": "on" if "u10" in variables else "off",
            "var_VGRD": "on" if "v10" in variables else "off",
            "var_TMP": "on" if "t2m" in variables else "off",
            "var_PRATE": "on" if "prate" in variables else "off",
            "subregion": "",
            "leftlon": lon_min,
            "rightlon": lon_max,
            "toplat": lat_max,
            "bottomlat": lat_min,
            "dir": config['dir_pattern'].format(date_str=run_date, run_str=run_str)
        }
        url = base_url + "?" + "&".join(f"{k}={v}" for k, v in params.items())
        out_path = os.path.join(out_dir, f"gfs_{resolution}_{run_date}_{run_str}_f{fh:03d}.grib2")
        logger.info(f"Downloading GFS {resolution}° forecast hour {fh}: {url} ...")
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                with open(out_path, "wb") as f:
                    f.write(r.content)
                logger.info(f"Saved {out_path}")
            else:
                logger.error(f"Failed to download GFS {resolution}° forecast hour {fh}: {url} (Status code: {r.status_code})")
        except Exception as e:
            logger.error(f"Error downloading GFS {resolution}° forecast hour {fh}: {str(e)}")

def download_icon_gribs(lat_min, lat_max, lon_min, lon_max, variables, hours=72, out_dir="icon_gribs", resolution='13km'):
    os.makedirs(out_dir, exist_ok=True)
    
    # Get latest available run
    run_time = get_latest_icon_run()
    if run_time is None:
        logger.error("No available ICON runs found in the last 3 days")
        return
        
    run_date = run_time.strftime("%Y%m%d")
    run_str = run_time.strftime("%H")
    logger.info(f"Using ICON run from {run_time.strftime('%Y-%m-%d %H:%M UTC')}")
    
    # Map our variable names to ICON variable names
    variable_map = {
        "u10": "U_10M",
        "v10": "V_10M",
        "t2m": "T_2M",
        "prate": "TOT_PREC"  # Note: This is accumulated precipitation, not rate
    }
    
    # Determine the correct base URL and patterns based on resolution
    if resolution == '13km':
        # Global model
        base_url = f"https://opendata.dwd.de/weather/nwp/icon/grib/global/{run_date}{run_str}"
        file_pattern = "icon_global_regular_0.25_{param}_{level}_{fh:03d}_{time_str}.grib2.bz2"
    else:
        # Europe model with 7km resolution
        base_url = f"https://opendata.dwd.de/weather/nwp/icon-eu/grib/{run_date}{run_str}"
        file_pattern = "icon-eu_europe_regular-lat-lon_{param}_{level}_{fh:03d}_{time_str}.grib2.bz2"
    
    # First, list the directory to find available files
    try:
        r = requests.get(base_url, timeout=10)
        if r.status_code != 200:
            logger.error(f"Failed to access ICON directory: {base_url}")
            return
            
        soup = BeautifulSoup(r.text, 'html.parser')
        links = [a.get('href') for a in soup.find_all('a') if a.get('href').endswith('.bz2')]
        
        # Time string in file pattern (model run time)
        time_str = f"{run_date}{run_str}"
        
        # Download each file that matches our criteria
        for fh in range(0, hours+1, 1):  # hourly steps
            for var in variables:
                if var not in variable_map:
                    continue
                    
                icon_var = variable_map[var]
                
                # Determine level based on variable
                if var in ["u10", "v10"]:
                    level = "L1"  # Surface level for winds
                elif var == "t2m":
                    level = "L1"  # Surface level for temperature
                elif var == "prate":
                    level = "L1"  # Surface level for precipitation
                
                # Find matching file pattern
                pattern = file_pattern.format(param=icon_var, level=level, fh=fh, time_str=time_str)
                matching_files = [link for link in links if pattern in link]
                
                if not matching_files:
                    logger.warning(f"No matching ICON files found for {var} at hour {fh}")
                    continue
                
                # Get the first matching file
                file_url = f"{base_url}/{matching_files[0]}"
                out_path = os.path.join(out_dir, f"icon_{resolution}_{run_date}_{run_str}_f{fh:03d}_{var}.grib2")
                
                logger.info(f"Downloading ICON {resolution} {var} forecast hour {fh}: {file_url}")
                try:
                    r = requests.get(file_url, timeout=30)
                    if r.status_code == 200:
                        # Decompress bz2 data and save
                        decompressed = bz2.decompress(r.content)
                        with open(out_path, "wb") as f:
                            f.write(decompressed)
                        logger.info(f"Saved {out_path}")
                    else:
                        logger.error(f"Failed to download ICON file: {file_url} (Status: {r.status_code})")
                except Exception as e:
                    logger.error(f"Error downloading/processing ICON file: {str(e)}")
    
    except Exception as e:
        logger.error(f"Error accessing ICON directory: {str(e)}")

def download_cmc_gribs(lat_min, lat_max, lon_min, lon_max, variables, hours=72, out_dir="cmc_gribs", resolution='15km'):
    os.makedirs(out_dir, exist_ok=True)
    
    # Get latest available run
    run_time = get_latest_cmc_run()
    if run_time is None:
        logger.error("No available CMC runs found in the last 3 days")
        return
        
    run_date = run_time.strftime("%Y%m%d")
    run_str = run_time.strftime("%H")
    logger.info(f"Using CMC run from {run_time.strftime('%Y-%m-%d %H:%M UTC')}")
    
    # Variable mapping for CMC
    variable_map = {
        "u10": "UGRD_TGL_10m",
        "v10": "VGRD_TGL_10m",
        "t2m": "TMP_TGL_2m",
        "prate": "APCP_SFC_0"  # Accumulated precipitation
    }
    
    # Choose the correct base URL based on resolution
    if resolution == '15km':
        base_url = f"https://dd.weather.gc.ca/model_gem_regional/15km/grib2/{run_str}"
    else:
        base_url = f"https://dd.weather.gc.ca/model_gem_global/25km/grib2/{run_str}"
    
    # List the directory to see available files
    try:
        r = requests.get(base_url, timeout=10)
        if r.status_code != 200:
            logger.error(f"Failed to access CMC directory: {base_url}")
            return
            
        soup = BeautifulSoup(r.text, 'html.parser')
        all_links = [a.get('href') for a in soup.find_all('a') if a.get('href', '').endswith('.grib2')]
        
        # Download each forecast hour
        for fh in range(0, hours+1, 1):
            # CMC files might use 3-hourly steps or pad forecast hours differently
            # Try different formats like P000, P003, 000, etc.
            for var in variables:
                if var not in variable_map:
                    continue
                
                cmc_var = variable_map[var]
                # Try different forecast hour notations
                fh_patterns = [f"P{fh:03d}", f"{fh:03d}", f"P{fh:02d}", f"{fh:02d}"]
                
                # Find any matching file for this variable and forecast hour
                matching_files = []
                for pattern in fh_patterns:
                    matching_files.extend([link for link in all_links 
                                          if cmc_var in link and pattern in link])
                
                if not matching_files:
                    logger.warning(f"No matching CMC files found for {var} at hour {fh}")
                    continue
                
                # Get the first matching file
                file_url = f"{base_url}/{matching_files[0]}"
                out_path = os.path.join(out_dir, f"cmc_{resolution}_{run_date}_{run_str}_f{fh:03d}_{var}.grib2")
                
                logger.info(f"Downloading CMC {resolution} {var} forecast hour {fh}: {file_url}")
                try:
                    r = requests.get(file_url, timeout=30)
                    if r.status_code == 200:
                        with open(out_path, "wb") as f:
                            f.write(r.content)
                        logger.info(f"Saved {out_path}")
                    else:
                        logger.error(f"Failed to download CMC file: {file_url} (Status: {r.status_code})")
                except Exception as e:
                    logger.error(f"Error downloading CMC file: {str(e)}")
    
    except Exception as e:
        logger.error(f"Error accessing CMC directory: {str(e)}")

def download_hrrr_gribs(lat_min, lat_max, lon_min, lon_max, variables, hours=72, out_dir="hrrr_gribs", resolution='3km'):
    os.makedirs(out_dir, exist_ok=True)
    config = MODEL_RESOLUTIONS['hrrr'][resolution]
    base_url = config['url']

    # Get latest available run
    run_time = get_latest_hrrr_run()
    if run_time is None:
        logger.error("No available HRRR runs found in the last 2 days")
        return

    run_date = run_time.strftime("%Y%m%d")
    run_str = run_time.strftime("%H")
    logger.info(f"Using HRRR {resolution} run from {run_time.strftime('%Y-%m-%d %H:%M UTC')}")

    # --- NEW: Detect max available forecast hour ---
    # The HRRR directory for this run
    hrrr_dir_url = f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/hrrr/prod/hrrr.{run_date}/conus/"
    try:
        r = requests.get(hrrr_dir_url, timeout=10)
        if r.status_code == 200:
            # Find all forecast hour files for this run
            import re
            pattern = re.compile(r"hrrr\.t" + run_str + r"z\.wrfsfcf(\d{2})\.grib2")
            available_hours = [int(m.group(1)) for m in pattern.finditer(r.text)]
            if available_hours:
                max_hour = max(available_hours)
                logger.info(f"Detected max available HRRR forecast hour: {max_hour}")
            else:
                logger.warning(f"No HRRR forecast hours found in directory listing, defaulting to {hours}")
                max_hour = hours
        else:
            logger.warning(f"Could not access HRRR directory listing (status {r.status_code}), defaulting to {hours}")
            max_hour = hours
    except Exception as e:
        logger.warning(f"Error accessing HRRR directory listing: {e}, defaulting to {hours}")
        max_hour = hours
    # --- END NEW ---

    for fh in range(0, max_hour+1, 1):  # hourly steps
        params = {
            "file": config['file_pattern'].format(run_str=run_str, fh=fh),
            "lev_2_m_above_ground": "on",
            "lev_10_m_above_ground": "on",
            "var_UGRD": "on" if "u10" in variables else "off",
            "var_VGRD": "on" if "v10" in variables else "off",
            "var_TMP": "on" if "t2m" in variables else "off",
            "var_PRATE": "on" if "prate" in variables else "off",
            "subregion": "",
            "leftlon": lon_min,
            "rightlon": lon_max,
            "toplat": lat_max,
            "bottomlat": lat_min,
            "dir": f"/hrrr.{run_date}/conus"  # Fixed date format in directory path
        }
        url = base_url + "?" + "&".join(f"{k}={v}" for k, v in params.items())
        out_path = os.path.join(out_dir, f"hrrr_{resolution}_{run_date}_{run_str}_f{fh:02d}.grib2")
        logger.info(f"Downloading HRRR {resolution} forecast hour {fh}: {url} ...")
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                with open(out_path, "wb") as f:
                    f.write(r.content)
                logger.info(f"Saved {out_path}")
            else:
                logger.error(f"Failed to download HRRR {resolution} forecast hour {fh}: {url} (Status code: {r.status_code})")
        except Exception as e:
            logger.error(f"Error downloading HRRR {resolution} forecast hour {fh}: {str(e)}")

if __name__ == "__main__":
    # Halifax Harbour region
    lat_min, lat_max = 44.5, 44.8
    lon_min, lon_max = -63.6, -63.4
    variables = ["u10", "v10", "t2m", "prate"]
    
    # Resolution configurations
    resolutions = {
        'gfs': '0.25',    # 0.25°, 0.5°, or 1.0°
        'icon': '13km',   # 13km or 7km
        'cmc': '15km',    # 25km or 15km
        'hrrr': '3km'     # 3km or 13km
    }
    
    # Set forecast hours to exactly 72 (3 days)
    forecast_hours = 72
    
    logger.info("Starting forecast data download...")
    logger.info("Downloading GFS data...")
    download_gfs_gribs(lat_min, lat_max, lon_min, lon_max, variables, hours=forecast_hours, resolution=resolutions['gfs'])
    logger.info("\nDownloading ICON data...")
    download_icon_gribs(lat_min, lat_max, lon_min, lon_max, variables, hours=forecast_hours, resolution=resolutions['icon'])
    logger.info("\nDownloading CMC data...")
    download_cmc_gribs(lat_min, lat_max, lon_min, lon_max, variables, hours=forecast_hours, resolution=resolutions['cmc'])
    logger.info("\nDownloading HRRR data...")
    download_hrrr_gribs(lat_min, lat_max, lon_min, lon_max, variables, hours=forecast_hours, resolution=resolutions['hrrr'])
    logger.info("Download complete!") 