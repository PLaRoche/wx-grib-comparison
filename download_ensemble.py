import os
import requests
from datetime import datetime, timedelta
import bz2
import re
from bs4 import BeautifulSoup

def get_latest_gfs_run():
    """Find the latest available GFS run"""
    base_url = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod"
    now = datetime.utcnow()
    
    # Try the last 3 days
    for days_back in range(3):
        date = now - timedelta(days=days_back)
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
                        return date.replace(hour=hour, minute=0, second=0, microsecond=0)
            except Exception as e:
                print(f"Error checking GFS run: {str(e)}")
    return None

def get_latest_icon_run():
    """Find the latest available ICON run"""
    base_url = "https://opendata.dwd.de/weather/nwp/icon/grib/global/icon_global_icosahedral_single-level"
    now = datetime.utcnow()
    
    # Try the last 3 days
    for days_back in range(3):
        date = now - timedelta(days=days_back)
        date_str = date.strftime("%Y%m%d")
        
        # Try each run hour (00, 06, 12, 18)
        for hour in [18, 12, 6, 0]:
            run_str = f"{hour:02d}"
            url = f"{base_url}/{date_str}/{run_str}"
            try:
                r = requests.get(url, timeout=10)
                if r.status_code == 200:
                    # Check if the run files exist
                    test_file = f"icon_global_icosahedral_single-level_{date_str}{run_str}_000_10u.grib2.bz2"
                    if test_file in r.text:
                        return date.replace(hour=hour, minute=0, second=0, microsecond=0)
            except Exception as e:
                print(f"Error checking ICON run: {str(e)}")
    return None

def get_latest_cmc_run():
    """Find the latest available CMC run"""
    base_url = "https://dd.weather.gc.ca/model_gem_global/25km/grib2/lat_lon"
    now = datetime.utcnow()
    
    # Try the last 3 days
    for days_back in range(3):
        date = now - timedelta(days=days_back)
        date_str = date.strftime("%Y%m%d")
        
        # Try each run hour (00, 06, 12, 18)
        for hour in [18, 12, 6, 0]:
            run_str = f"{hour:02d}"
            url = f"{base_url}/{date_str}/{run_str}"
            try:
                r = requests.get(url, timeout=10)
                if r.status_code == 200:
                    # Check if the run files exist
                    test_file = f"CMC_glb_UGRD_TGL_10_latlon.24x.24_{date_str}{run_str}_P000.grib2"
                    if test_file in r.text:
                        return date.replace(hour=hour, minute=0, second=0, microsecond=0)
            except Exception as e:
                print(f"Error checking CMC run: {str(e)}")
    return None

def get_latest_hrrr_run():
    """Find the latest available HRRR run"""
    base_url = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/hrrr/prod"
    now = datetime.utcnow()
    
    # Try the last 2 days (HRRR is more frequent)
    for days_back in range(2):
        date = now - timedelta(days=days_back)
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
                        return date.replace(hour=hour, minute=0, second=0, microsecond=0)
            except Exception as e:
                print(f"Error checking HRRR run: {str(e)}")
    return None

def download_gfs_gribs(lat_min, lat_max, lon_min, lon_max, variables, hours=72, out_dir="gfs_gribs"):
    os.makedirs(out_dir, exist_ok=True)
    base_url = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"
    
    # Get latest available run
    run_time = get_latest_gfs_run()
    if run_time is None:
        print("No available GFS runs found in the last 3 days")
        return
        
    run_date = run_time.strftime("%Y%m%d")
    run_str = run_time.strftime("%H")
    print(f"Using GFS run from {run_time.strftime('%Y-%m-%d %H:%M UTC')}")
    
    for fh in range(0, hours+1, 3):  # 3-hourly steps
        params = {
            "file": f"gfs.t{run_str}z.pgrb2.0p25.f{fh:03d}",
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
            "dir": f"/gfs.{run_date}/{run_str}/atmos"
        }
        url = base_url + "?" + "&".join(f"{k}={v}" for k, v in params.items())
        out_path = os.path.join(out_dir, f"gfs_{run_date}_{run_str}_f{fh:03d}.grib2")
        print(f"Downloading GFS forecast hour {fh}: {url} ...")
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                with open(out_path, "wb") as f:
                    f.write(r.content)
                print(f"Saved {out_path}")
            else:
                print(f"Failed to download GFS forecast hour {fh}: {url} (Status code: {r.status_code})")
        except Exception as e:
            print(f"Error downloading GFS forecast hour {fh}: {str(e)}")

def download_icon_gribs(lat_min, lat_max, lon_min, lon_max, variables, hours=72, out_dir="icon_gribs"):
    os.makedirs(out_dir, exist_ok=True)
    base_url = "https://opendata.dwd.de/weather/nwp/icon/grib/global/icon_global_icosahedral_single-level"
    
    # Get latest available run
    run_time = get_latest_icon_run()
    if run_time is None:
        print("No available ICON runs found in the last 3 days")
        return
        
    run_date = run_time.strftime("%Y%m%d")
    run_str = run_time.strftime("%H")
    print(f"Using ICON run from {run_time.strftime('%Y-%m-%d %H:%M UTC')}")
    
    for fh in range(0, hours+1, 3):  # 3-hourly steps
        # Download each variable separately
        for var in variables:
            if var == "u10":
                var_name = "10u"
            elif var == "v10":
                var_name = "10v"
            elif var == "t2m":
                var_name = "2t"
            elif var == "prate":
                var_name = "tp"
            else:
                continue
                
            url = f"{base_url}/{run_date}/{run_str}/icon_global_icosahedral_single-level_{run_date}{run_str}_{fh:03d}_{var_name}.grib2.bz2"
            out_path = os.path.join(out_dir, f"icon_{run_date}_{run_str}_f{fh:03d}_{var}.grib2")
            print(f"Downloading ICON {var} forecast hour {fh}: {url} ...")
            try:
                r = requests.get(url, timeout=30)
                if r.status_code == 200:
                    # Decompress bz2 data
                    decompressed = bz2.decompress(r.content)
                    with open(out_path, "wb") as f:
                        f.write(decompressed)
                    print(f"Saved {out_path}")
                else:
                    print(f"Failed to download ICON {var} forecast hour {fh}: {url} (Status code: {r.status_code})")
            except Exception as e:
                print(f"Error downloading ICON {var} forecast hour {fh}: {str(e)}")

def download_cmc_gribs(lat_min, lat_max, lon_min, lon_max, variables, hours=72, out_dir="cmc_gribs"):
    os.makedirs(out_dir, exist_ok=True)
    base_url = "https://dd.weather.gc.ca/model_gem_global/25km/grib2/lat_lon"
    
    # Get latest available run
    run_time = get_latest_cmc_run()
    if run_time is None:
        print("No available CMC runs found in the last 3 days")
        return
        
    run_date = run_time.strftime("%Y%m%d")
    run_str = run_time.strftime("%H")
    print(f"Using CMC run from {run_time.strftime('%Y-%m-%d %H:%M UTC')}")
    
    for fh in range(0, hours+1, 3):  # 3-hourly steps
        # Download each variable separately
        for var in variables:
            if var == "u10":
                var_name = "UGRD_TGL_10"
            elif var == "v10":
                var_name = "VGRD_TGL_10"
            elif var == "t2m":
                var_name = "TMP_TGL_2"
            elif var == "prate":
                var_name = "PRATE_SFC_0"
            else:
                continue
                
            url = f"{base_url}/{run_date}/{run_str}/CMC_glb_{var_name}_latlon.24x.24_{run_date}{run_str}_P{fh:03d}.grib2"
            out_path = os.path.join(out_dir, f"cmc_{run_date}_{run_str}_f{fh:03d}_{var}.grib2")
            print(f"Downloading CMC {var} forecast hour {fh}: {url} ...")
            try:
                r = requests.get(url, timeout=30)
                if r.status_code == 200:
                    with open(out_path, "wb") as f:
                        f.write(r.content)
                    print(f"Saved {out_path}")
                else:
                    print(f"Failed to download CMC {var} forecast hour {fh}: {url} (Status code: {r.status_code})")
            except Exception as e:
                print(f"Error downloading CMC {var} forecast hour {fh}: {str(e)}")

def download_hrrr_gribs(lat_min, lat_max, lon_min, lon_max, variables, hours=48, out_dir="hrrr_gribs"):
    os.makedirs(out_dir, exist_ok=True)
    base_url = "https://nomads.ncep.noaa.gov/cgi-bin/filter_hrrr_2d.pl"
    
    # Get latest available run
    run_time = get_latest_hrrr_run()
    if run_time is None:
        print("No available HRRR runs found in the last 2 days")
        return
        
    run_date = run_time.strftime("%Y%m%d")
    run_str = run_time.strftime("%H")
    print(f"Using HRRR run from {run_time.strftime('%Y-%m-%d %H:%M UTC')}")
    
    for fh in range(0, hours+1, 1):  # hourly steps
        params = {
            "file": f"hrrr.t{run_str}z.wrfsfcf{fh:02d}.grib2",
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
            "dir": f"/hrrr.{run_date}/conus"
        }
        url = base_url + "?" + "&".join(f"{k}={v}" for k, v in params.items())
        out_path = os.path.join(out_dir, f"hrrr_{run_date}_{run_str}_f{fh:02d}.grib2")
        print(f"Downloading HRRR forecast hour {fh}: {url} ...")
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                with open(out_path, "wb") as f:
                    f.write(r.content)
                print(f"Saved {out_path}")
            else:
                print(f"Failed to download HRRR forecast hour {fh}: {url} (Status code: {r.status_code})")
        except Exception as e:
            print(f"Error downloading HRRR forecast hour {fh}: {str(e)}")

if __name__ == "__main__":
    # Halifax Harbour region
    lat_min, lat_max = 44.5, 44.8
    lon_min, lon_max = -63.6, -63.4
    variables = ["u10", "v10", "t2m", "prate"]
    
    print("Downloading GFS data...")
    download_gfs_gribs(lat_min, lat_max, lon_min, lon_max, variables)
    print("\nDownloading ICON data...")
    download_icon_gribs(lat_min, lat_max, lon_min, lon_max, variables)
    print("\nDownloading CMC data...")
    download_cmc_gribs(lat_min, lat_max, lon_min, lon_max, variables)
    print("\nDownloading HRRR data...")
    download_hrrr_gribs(lat_min, lat_max, lon_min, lon_max, variables) 