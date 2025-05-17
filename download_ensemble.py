import os
import requests
from datetime import datetime, timedelta
import bz2
import re
from bs4 import BeautifulSoup
import logging
import time
import concurrent.futures
from typing import List, Dict, Any, Optional, Tuple
from tqdm import tqdm
import dateutil.parser

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
    """Find the latest available ICON run by checking variable subdirectories in each hour directory, searching up to 7 days back."""
    base_url = "https://opendata.dwd.de/weather/nwp/icon/grib"
    hour_dirs = ["00", "06", "12", "18"]
    var_map = {
        "u10": "u_10m",
        "v10": "v_10m",
        "t2m": "t_2m",
        "prate": "tot_prec"
    }
    
    latest_run = None
    latest_timestamp = None
    now = datetime.utcnow()
    # Search up to 7 days back
    for days_back in range(7):
        date = now - timedelta(days=days_back)
        date_str = date.strftime("%Y%m%d")
        for hour in hour_dirs:
            hour_url = f"{base_url}/{hour}/"
            logging.debug(f"Checking hour directory: {hour_url}")
            try:
                response = requests.get(hour_url)
                response.raise_for_status()
                logging.debug(f"HTTP status code for {hour_url}: {response.status_code}")
                soup = BeautifulSoup(response.text, 'html.parser')
                links = soup.find_all('a')
                logging.debug(f"Found {len(links)} links in {hour} directory on {date_str}")
                for link in links:
                    href = link.get('href', '')
                    if href.endswith('/'):
                        href = href[:-1]
                    logging.debug(f"Found link: {href}")
                    if href in var_map.values():
                        var_url = f"{hour_url}{href}/"
                        logging.debug(f"Found variable directory: {href}")
                        logging.debug(f"Checking variable directory URL: {var_url}")
                        try:
                            var_response = requests.get(var_url)
                            var_response.raise_for_status()
                            logging.debug(f"Variable directory status code: {var_response.status_code}")
                            var_soup = BeautifulSoup(var_response.text, 'html.parser')
                            var_links = var_soup.find_all('a')
                            logging.debug(f"Found {len(var_links)} files in {href} for {date_str} {hour}")
                            for var_link in var_links:
                                var_href = var_link.get('href', '')
                                if var_href.startswith('icon_global_icosahedral_single-level_') and var_href.endswith('.grib2.bz2'):
                                    try:
                                        parts = var_href.split('_')
                                        if len(parts) >= 6:
                                            timestamp_str = parts[4]  # This is the YYYYMMDDHH part
                                            timestamp = datetime.strptime(timestamp_str, '%Y%m%d%H')
                                            logging.debug(f"Found file: {var_href} with timestamp {timestamp_str}")
                                            if latest_timestamp is None or timestamp > latest_timestamp:
                                                latest_timestamp = timestamp
                                                latest_run = {
                                                    'hour': hour,
                                                    'timestamp': timestamp,
                                                    'url': var_url
                                                }
                                                logging.info(f"Found new latest run: {hour} at {timestamp}")
                                    except (ValueError, IndexError) as e:
                                        logging.debug(f"Error parsing timestamp from {var_href}: {e}")
                                        continue
                        except requests.RequestException as e:
                            logging.debug(f"Error accessing variable directory {var_url}: {e}")
                            continue
            except requests.RequestException as e:
                logging.debug(f"Error accessing hour directory {hour_url}: {e}")
                continue
    if latest_run:
        logging.info(f"Found latest ICON run: {latest_run['hour']} at {latest_run['timestamp']}")
        return latest_run
    else:
        logging.warning("No available ICON runs found in the variable subdirectories.")
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

def get_latest_nam_run():
    """Find the latest available NAM run"""
    base_url = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/nam/prod"
    now = datetime.utcnow()
    
    # Try the last 3 days
    for days_back in range(3):
        date = now - timedelta(days=days_back)
        date_str = date.strftime("%Y%m%d")
        
        # Try each run hour (00, 06, 12, 18)
        for hour in [18, 12, 6, 0]:
            run_str = f"{hour:02d}"
            url = f"{base_url}/nam.{date_str}"
            try:
                r = requests.get(url, timeout=10)
                if r.status_code == 200:
                    # Check if the run files exist
                    test_file = f"nam.t{run_str}z.awphys000.tm00.grib2"
                    if test_file in r.text:
                        return date.replace(hour=hour, minute=0, second=0, microsecond=0)
            except Exception as e:
                print(f"Error checking NAM run: {str(e)}")
    return None

def get_latest_rap_run():
    """Find the latest available RAP run"""
    base_url = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/rap/prod"
    now = datetime.utcnow()
    
    # Try the last 2 days (RAP is more frequent)
    for days_back in range(2):
        date = now - timedelta(days=days_back)
        date_str = date.strftime("%Y%m%d")
        
        # Try each run hour (23 to 0)
        for hour in range(23, -1, -1):
            run_str = f"{hour:02d}"
            url = f"{base_url}/rap.{date_str}"
            try:
                r = requests.get(url, timeout=10)
                if r.status_code == 200:
                    # Check if the run files exist
                    test_file = f"rap.t{run_str}z.awp130pgrbf00.grib2"
                    if test_file in r.text:
                        return date.replace(hour=hour, minute=0, second=0, microsecond=0)
            except Exception as e:
                print(f"Error checking RAP run: {str(e)}")
    return None

def get_latest_nbm_run():
    """Find the latest available NBM run"""
    base_url = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/blend/prod"
    now = datetime.utcnow()
    
    # Try the last 2 days
    for days_back in range(2):
        date = now - timedelta(days=days_back)
        date_str = date.strftime("%Y%m%d")
        
        # Try each run hour (23 to 0)
        for hour in range(23, -1, -1):
            run_str = f"{hour:02d}"
            url = f"{base_url}/blend.{date_str}"
            try:
                r = requests.get(url, timeout=10)
                if r.status_code == 200:
                    # Check if the run files exist
                    test_file = f"blend.t{run_str}z.core.f000.co.grib2"
                    if test_file in r.text:
                        return date.replace(hour=hour, minute=0, second=0, microsecond=0)
            except Exception as e:
                print(f"Error checking NBM run: {str(e)}")
    return None

def get_latest_sref_run():
    """Find the latest available SREF run"""
    base_url = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/sref/prod"
    now = datetime.utcnow()
    
    # Try the last 3 days
    for days_back in range(3):
        date = now - timedelta(days=days_back)
        date_str = date.strftime("%Y%m%d")
        
        # Try each run hour (00, 06, 12, 18)
        for hour in [18, 12, 6, 0]:
            run_str = f"{hour:02d}"
            url = f"{base_url}/sref.{date_str}"
            try:
                r = requests.get(url, timeout=10)
                if r.status_code == 200:
                    # Check if the run files exist
                    test_file = f"sref.t{run_str}z.pgrb132.f000.grib2"
                    if test_file in r.text:
                        return date.replace(hour=hour, minute=0, second=0, microsecond=0)
            except Exception as e:
                print(f"Error checking SREF run: {str(e)}")
    return None

def download_with_retry(url: str, out_path: str, max_retries: int = 3, timeout: int = 30) -> bool:
    """
    Download a file with retry logic and exponential backoff.
    """
    for attempt in range(max_retries):
        try:
            r = requests.get(url, timeout=timeout)
            if r.status_code == 200:
                with open(out_path, "wb") as f:
                    f.write(r.content)
                return True
            elif r.status_code == 429:
                logger.error("Rate limit hit. Stopping retries.")
                return False
            else:
                logger.warning(f"Attempt {attempt + 1}/{max_retries}: Failed to download (Status code: {r.status_code})")
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1}/{max_retries}: Error downloading: {str(e)}")
        
        if attempt < max_retries - 1:
            time.sleep(2 ** attempt)  # Exponential backoff
    
    logger.error(f"Failed to download after {max_retries} attempts: {url}")
    return False

def download_bz2_with_retry(url: str, out_path: str, max_retries: int = 3, timeout: int = 30) -> bool:
    """
    Download and decompress a bz2 file with retry logic.
    """
    for attempt in range(max_retries):
        try:
            r = requests.get(url, timeout=timeout)
            if r.status_code == 200:
                decompressed = bz2.decompress(r.content)
                with open(out_path, "wb") as f:
                    f.write(decompressed)
                return True
            elif r.status_code == 429:
                logger.error("Rate limit hit. Stopping retries.")
                return False
            else:
                logger.warning(f"Attempt {attempt + 1}/{max_retries}: Failed to download (Status code: {r.status_code})")
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1}/{max_retries}: Error downloading/decompressing: {str(e)}")
        
        if attempt < max_retries - 1:
            time.sleep(2 ** attempt)  # Exponential backoff
    
    logger.error(f"Failed to download/decompress after {max_retries} attempts: {url}")
    return False

def parallel_download(download_tasks: List[Tuple[str, str]], max_workers: int = 5, desc: str = "Downloading") -> None:
    """
    Download multiple files in parallel with progress tracking.
    
    Args:
        download_tasks: List of (url, output_path) tuples
        max_workers: Maximum number of parallel downloads
        desc: Description for the progress bar
    """
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for url, out_path in download_tasks:
            if url.endswith('.bz2'):
                futures.append(executor.submit(download_bz2_with_retry, url, out_path))
            else:
                futures.append(executor.submit(download_with_retry, url, out_path))
        
        # Create progress bar
        with tqdm(total=len(download_tasks), desc=desc, unit="file") as pbar:
            for future in concurrent.futures.as_completed(futures):
                try:
                    success = future.result()
                    if success:
                        pbar.update(1)
                except Exception as e:
                    logger.error(f"Error in download task: {str(e)}")
                    pbar.update(1)

def prepare_gfs_download_tasks(run_time: datetime, variables: List[str], hours: int, 
                             lat_min: float, lat_max: float, lon_min: float, lon_max: float,
                             resolution: str = '0.25', out_dir: str = "gribs") -> List[Tuple[str, str]]:
    """
    Prepare GFS download tasks for parallel execution.
    """
    download_tasks = []
    config = MODEL_RESOLUTIONS['gfs'][resolution]
    base_url = config['url']
    
    run_date = run_time.strftime("%Y%m%d")
    run_str = run_time.strftime("%H")
    
    for fh in range(0, hours + 1, 1):
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
        out_path = os.path.join(out_dir, "gfs_gribs", f"gfs_{resolution}_{run_date}_{run_str}_f{fh:03d}.grib2")
        download_tasks.append((url, out_path))
    
    return download_tasks

def prepare_icon_download_tasks(run_time: datetime, variables: List[str], hours: int,
                              resolution: str = '13km', out_dir: str = "gribs") -> List[Tuple[str, str]]:
    """
    Prepare ICON download tasks for parallel execution.
    """
    download_tasks = []
    run_date = run_time.strftime("%Y%m%d")
    run_str = run_time.strftime("%H")
    
    # Map our variables to DWD subdirectories and filename parts (dir, file)
    var_map = {
        "u10": ("u_10m", "U_10M"),
        "v10": ("v_10m", "V_10M"),
        "t2m": ("t_2m", "T_2M"),
        "prate": ("tot_prec", "TOT_PREC")
    }
    
    # Base URL for ICON global model
    base_url = f"https://opendata.dwd.de/weather/nwp/icon/grib/{run_str}"
    
    for fh in range(0, hours + 1, 1):
        for var in variables:
            if var not in var_map:
                continue
            
            var_dir, var_file = var_map[var]
            # Use lowercase for directory, uppercase for filename
            url = f"{base_url}/{var_dir}/icon_global_icosahedral_single-level_{run_date}{run_str}_{fh:03d}_{var_file}.grib2.bz2"
            out_path = os.path.join(out_dir, "icon_gribs", f"icon_{resolution}_{run_date}_{run_str}_f{fh:03d}_{var}.grib2")
            download_tasks.append((url, out_path))
    
    return download_tasks

def prepare_cmc_download_tasks(run_time: datetime, variables: List[str], hours: int,
                             resolution: str = '15km', out_dir: str = "gribs") -> List[Tuple[str, str]]:
    """
    Prepare CMC download tasks for parallel execution.
    """
    download_tasks = []
    run_date = run_time.strftime("%Y%m%d")
    run_str = run_time.strftime("%H")
    
    variable_map = {
        "u10": "UGRD_TGL_10m",
        "v10": "VGRD_TGL_10m",
        "t2m": "TMP_TGL_2m",
        "prate": "APCP_SFC_0"
    }
    
    if resolution == '15km':
        base_url = f"https://dd.weather.gc.ca/model_gem_regional/15km/grib2/{run_str}"
    else:
        base_url = f"https://dd.weather.gc.ca/model_gem_global/25km/grib2/{run_str}"
    
    for fh in range(0, hours + 1, 1):
        for var in variables:
            if var not in variable_map:
                continue
            
            cmc_var = variable_map[var]
            fh_patterns = [f"P{fh:03d}", f"{fh:03d}", f"P{fh:02d}", f"{fh:02d}"]
            
            for pattern in fh_patterns:
                url = f"{base_url}/CMC_reg_{cmc_var}_latlon.15x.15_{run_date}{run_str}_{pattern}.grib2"
                out_path = os.path.join(out_dir, "cmc_gribs", f"cmc_{resolution}_{run_date}_{run_str}_f{fh:03d}_{var}.grib2")
                download_tasks.append((url, out_path))
    
    return download_tasks

def prepare_hrrr_download_tasks(run_time: datetime, variables: List[str], hours: int,
                              lat_min: float, lat_max: float, lon_min: float, lon_max: float,
                              resolution: str = '3km', out_dir: str = "gribs") -> List[Tuple[str, str]]:
    """
    Prepare HRRR download tasks for parallel execution.
    """
    download_tasks = []
    config = MODEL_RESOLUTIONS['hrrr'][resolution]
    base_url = config['url']
    
    run_date = run_time.strftime("%Y%m%d")
    run_str = run_time.strftime("%H")
    
    for fh in range(0, hours + 1, 1):
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
            "dir": f"/hrrr.{run_date}/conus"
        }
        url = base_url + "?" + "&".join(f"{k}={v}" for k, v in params.items())
        out_path = os.path.join(out_dir, "hrrr_gribs", f"hrrr_{resolution}_{run_date}_{run_str}_f{fh:02d}.grib2")
        download_tasks.append((url, out_path))
    
    return download_tasks

def prepare_nam_download_tasks(run_time: datetime, variables: List[str], hours: int,
                             lat_min: float, lat_max: float, lon_min: float, lon_max: float,
                             out_dir: str = "gribs") -> List[Tuple[str, str]]:
    """
    Prepare NAM download tasks for parallel execution.
    """
    download_tasks = []
    run_date = run_time.strftime("%Y%m%d")
    run_str = run_time.strftime("%H")
    base_url = "https://nomads.ncep.noaa.gov/cgi-bin/filter_nam.pl"
    
    for fh in range(0, hours + 1, 3):  # 3-hourly steps
        params = {
            "file": f"nam.t{run_str}z.awphys{fh:03d}.tm00.grib2",
            "lev_2_m_above_ground": "on",
            "lev_10_m_above_ground": "on",
            "lev_surface": "on",
            "var_UGRD": "on" if "u10" in variables else "off",
            "var_VGRD": "on" if "v10" in variables else "off",
            "var_TMP": "on" if "t2m" in variables else "off",
            "var_PRATE": "on" if "prate" in variables else "off",
            "subregion": "",
            "leftlon": lon_min,
            "rightlon": lon_max,
            "toplat": lat_max,
            "bottomlat": lat_min,
            "dir": f"/nam.{run_date}"
        }
        url = base_url + "?" + "&".join(f"{k}={v}" for k, v in params.items())
        out_path = os.path.join(out_dir, "nam_gribs", f"nam_{run_date}_{run_str}_f{fh:03d}.grib2")
        download_tasks.append((url, out_path))
    
    return download_tasks

def prepare_rap_download_tasks(run_time: datetime, variables: List[str], hours: int,
                             lat_min: float, lat_max: float, lon_min: float, lon_max: float,
                             out_dir: str = "gribs") -> List[Tuple[str, str]]:
    """
    Prepare RAP download tasks for parallel execution.
    """
    download_tasks = []
    run_date = run_time.strftime("%Y%m%d")
    run_str = run_time.strftime("%H")
    base_url = "https://nomads.ncep.noaa.gov/cgi-bin/filter_rap.pl"
    
    for fh in range(0, hours + 1, 1):  # hourly steps
        params = {
            "file": f"rap.t{run_str}z.awp130pgrbf{fh:02d}.grib2",
            "lev_2_m_above_ground": "on",
            "lev_10_m_above_ground": "on",
            "lev_surface": "on",
            "var_UGRD": "on" if "u10" in variables else "off",
            "var_VGRD": "on" if "v10" in variables else "off",
            "var_TMP": "on" if "t2m" in variables else "off",
            "var_PRATE": "on" if "prate" in variables else "off",
            "subregion": "",
            "leftlon": lon_min,
            "rightlon": lon_max,
            "toplat": lat_max,
            "bottomlat": lat_min,
            "dir": f"/rap.{run_date}"
        }
        url = base_url + "?" + "&".join(f"{k}={v}" for k, v in params.items())
        out_path = os.path.join(out_dir, "rap_gribs", f"rap_{run_date}_{run_str}_f{fh:02d}.grib2")
        download_tasks.append((url, out_path))
    
    return download_tasks

def prepare_nbm_download_tasks(run_time: datetime, variables: List[str], hours: int,
                             lat_min: float, lat_max: float, lon_min: float, lon_max: float,
                             out_dir: str = "gribs") -> List[Tuple[str, str]]:
    """
    Prepare NBM download tasks for parallel execution.
    """
    download_tasks = []
    run_date = run_time.strftime("%Y%m%d")
    run_str = run_time.strftime("%H")
    base_url = "https://nomads.ncep.noaa.gov/cgi-bin/filter_nbm.pl"
    
    for fh in range(0, hours + 1, 1):  # hourly steps
        params = {
            "file": f"blend.t{run_str}z.core.f{fh:03d}.co.grib2",
            "lev_2_m_above_ground": "on",
            "lev_10_m_above_ground": "on",
            "lev_surface": "on",
            "var_UGRD": "on" if "u10" in variables else "off",
            "var_VGRD": "on" if "v10" in variables else "off",
            "var_TMP": "on" if "t2m" in variables else "off",
            "var_PRATE": "on" if "prate" in variables else "off",
            "subregion": "",
            "leftlon": lon_min,
            "rightlon": lon_max,
            "toplat": lat_max,
            "bottomlat": lat_min,
            "dir": f"/nbm.{run_date}"
        }
        url = base_url + "?" + "&".join(f"{k}={v}" for k, v in params.items())
        out_path = os.path.join(out_dir, "nbm_gribs", f"nbm_{run_date}_{run_str}_f{fh:03d}.grib2")
        download_tasks.append((url, out_path))
    
    return download_tasks

def download_gfs_gribs(lat_min, lat_max, lon_min, lon_max, variables, hours=72, out_dir="gribs", resolution='0.25'):
    """
    Download GFS GRIB files for the specified region and variables.
    """
    try:
        # Create output directory if it doesn't exist
        output_dir = os.path.join(out_dir, "gfs_gribs")
        os.makedirs(output_dir, exist_ok=True)
        
        # Get latest available run
        run_time = get_latest_gfs_run()
        if run_time is None:
            logger.error("No available GFS runs found in the last 3 days")
            return
        
        logger.info(f"Using GFS {resolution}° run from {run_time.strftime('%Y-%m-%d %H:%M UTC')}")
        
        # Prepare download tasks
        download_tasks = prepare_gfs_download_tasks(
            run_time, variables, hours, lat_min, lat_max, lon_min, lon_max, resolution, out_dir
        )
        
        # Execute downloads in parallel
        parallel_download(download_tasks, desc="Downloading GFS data")
        
    except Exception as e:
        logger.error(f"Error downloading GFS data: {str(e)}")

def download_icon_gribs(lat_min, lat_max, lon_min, lon_max, variables, hours=72, out_dir="gribs", resolution='13km'):
    """
    Download ICON GRIB files for the specified region and variables.
    """
    try:
        # Create output directory if it doesn't exist
        output_dir = os.path.join(out_dir, "icon_gribs")
        os.makedirs(output_dir, exist_ok=True)
        
        # Get latest available run
        run_time = get_latest_icon_run()
        if run_time is None:
            logger.error("No available ICON runs found in the last 3 days")
            return
        
        logger.info(f"Using ICON {resolution} run from {run_time['timestamp'].strftime('%Y-%m-%d %H:%M UTC')}")
        
        # Prepare download tasks
        download_tasks = prepare_icon_download_tasks(
            run_time['timestamp'], variables, hours, resolution, out_dir
        )
        
        # Execute downloads in parallel
        parallel_download(download_tasks, desc="Downloading ICON data")
        
    except Exception as e:
        logger.error(f"Error downloading ICON data: {str(e)}")

def download_cmc_gribs(lat_min, lat_max, lon_min, lon_max, variables, hours=72, out_dir="gribs", resolution='15km'):
    """
    Download CMC GRIB files for the specified region and variables.
    """
    try:
        # Create output directory if it doesn't exist
        output_dir = os.path.join(out_dir, "cmc_gribs")
        os.makedirs(output_dir, exist_ok=True)
        
        # Get latest available run
        run_time = get_latest_cmc_run()
        if run_time is None:
            logger.error("No available CMC runs found in the last 3 days")
            return
        
        logger.info(f"Using CMC run from {run_time.strftime('%Y-%m-%d %H:%M UTC')}")
        
        # Prepare download tasks
        download_tasks = prepare_cmc_download_tasks(
            run_time, variables, hours, resolution, out_dir
        )
        
        # Execute downloads in parallel
        parallel_download(download_tasks, desc="Downloading CMC data")
        
    except Exception as e:
        logger.error(f"Error downloading CMC data: {str(e)}")

def download_hrrr_gribs(lat_min, lat_max, lon_min, lon_max, variables, hours=18, out_dir="gribs", resolution='3km'):
    """
    Download HRRR GRIB files for the specified region and variables.
    """
    try:
        # Create output directory if it doesn't exist
        output_dir = os.path.join(out_dir, "hrrr_gribs")
        os.makedirs(output_dir, exist_ok=True)
        
        # Get latest available run
        run_time = get_latest_hrrr_run()
        if run_time is None:
            logger.error("No available HRRR runs found in the last 2 days")
            return
        
        logger.info(f"Using HRRR {resolution} run from {run_time.strftime('%Y-%m-%d %H:%M UTC')}")
        
        # Prepare download tasks
        download_tasks = prepare_hrrr_download_tasks(
            run_time, variables, hours, lat_min, lat_max, lon_min, lon_max, resolution, out_dir
        )
        
        # Execute downloads in parallel
        parallel_download(download_tasks, desc="Downloading HRRR data")
        
    except Exception as e:
        logger.error(f"Error downloading HRRR data: {str(e)}")

def download_nam_gribs(lat_min, lat_max, lon_min, lon_max, variables, hours=84, out_dir="gribs"):
    """
    Download NAM GRIB files for the specified region and variables.
    """
    try:
        # Create output directory if it doesn't exist
        output_dir = os.path.join(out_dir, "nam_gribs")
        os.makedirs(output_dir, exist_ok=True)
        
        # Get latest available run
        run_time = get_latest_nam_run()
        if run_time is None:
            logger.error("No available NAM runs found in the last 3 days")
            return
        
        logger.info(f"Using NAM run from {run_time.strftime('%Y-%m-%d %H:%M UTC')}")
        
        # Prepare download tasks
        download_tasks = prepare_nam_download_tasks(
            run_time, variables, hours, lat_min, lat_max, lon_min, lon_max, out_dir
        )
        
        # Execute downloads in parallel
        parallel_download(download_tasks, desc="Downloading NAM data")
        
    except Exception as e:
        logger.error(f"Error downloading NAM data: {str(e)}")

def download_rap_gribs(lat_min, lat_max, lon_min, lon_max, variables, hours=18, out_dir="gribs"):
    """
    Download RAP GRIB files for the specified region and variables.
    """
    try:
        # Create output directory if it doesn't exist
        output_dir = os.path.join(out_dir, "rap_gribs")
        os.makedirs(output_dir, exist_ok=True)
        
        # Get latest available run
        run_time = get_latest_rap_run()
        if run_time is None:
            logger.error("No available RAP runs found in the last 2 days")
            return
        
        logger.info(f"Using RAP run from {run_time.strftime('%Y-%m-%d %H:%M UTC')}")
        
        # Prepare download tasks
        download_tasks = prepare_rap_download_tasks(
            run_time, variables, hours, lat_min, lat_max, lon_min, lon_max, out_dir
        )
        
        # Execute downloads in parallel
        parallel_download(download_tasks, desc="Downloading RAP data")
        
    except Exception as e:
        logger.error(f"Error downloading RAP data: {str(e)}")

def download_nbm_gribs(lat_min, lat_max, lon_min, lon_max, variables, hours=72, out_dir="gribs"):
    """
    Download NBM GRIB files for the specified region and variables.
    """
    try:
        # Create output directory if it doesn't exist
        output_dir = os.path.join(out_dir, "nbm_gribs")
        os.makedirs(output_dir, exist_ok=True)
        
        # Get latest available run
        run_time = get_latest_nbm_run()
        if run_time is None:
            logger.error("No available NBM runs found in the last 2 days")
            return
        
        logger.info(f"Using NBM run from {run_time.strftime('%Y-%m-%d %H:%M UTC')}")
        
        # Prepare download tasks
        download_tasks = prepare_nbm_download_tasks(
            run_time, variables, hours, lat_min, lat_max, lon_min, lon_max, out_dir
        )
        
        # Execute downloads in parallel
        parallel_download(download_tasks, desc="Downloading NBM data")
        
    except Exception as e:
        logger.error(f"Error downloading NBM data: {str(e)}")

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
    logger.info("\nDownloading NAM data...")
    download_nam_gribs(lat_min, lat_max, lon_min, lon_max, variables, hours=forecast_hours)
    logger.info("\nDownloading RAP data...")
    download_rap_gribs(lat_min, lat_max, lon_min, lon_max, variables, hours=forecast_hours)
    logger.info("\nDownloading NBM data...")
    download_nbm_gribs(lat_min, lat_max, lon_min, lon_max, variables, hours=forecast_hours)
    logger.info("Download complete!") 