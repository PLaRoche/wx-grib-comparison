import os
import requests
from datetime import datetime, timedelta

def download_gfs_gribs(lat_min, lat_max, lon_min, lon_max, variables, hours=72, out_dir="gfs_gribs"):
    os.makedirs(out_dir, exist_ok=True)
    base_url = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"
    now = datetime.utcnow()
    run_hour = now.hour // 6 * 6  # nearest 6-hour run
    run_date = now.strftime("%Y%m%d")
    run_str = f"{run_hour:02d}"
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
        print(f"Downloading GFS: {url} ...")
        r = requests.get(url)
        if r.status_code == 200:
            with open(out_path, "wb") as f:
                f.write(r.content)
            print(f"Saved {out_path}")
        else:
            print(f"Failed to download GFS: {url}")

def download_icon_gribs(lat_min, lat_max, lon_min, lon_max, variables, hours=72, out_dir="icon_gribs"):
    os.makedirs(out_dir, exist_ok=True)
    base_url = "https://opendata.dwd.de/weather/nwp/icon/grib"
    now = datetime.utcnow()
    run_hour = now.hour // 6 * 6  # nearest 6-hour run
    run_date = now.strftime("%Y%m%d")
    run_str = f"{run_hour:02d}"
    for fh in range(0, hours+1, 3):  # 3-hourly steps
        url = f"{base_url}/{run_date}/{run_str}/icon_global_icosahedral_single-level_{run_date}{run_str}_{fh:03d}_10u.grib2.bz2"
        out_path = os.path.join(out_dir, f"icon_{run_date}_{run_str}_f{fh:03d}.grib2.bz2")
        print(f"Downloading ICON: {url} ...")
        r = requests.get(url)
        if r.status_code == 200:
            with open(out_path, "wb") as f:
                f.write(r.content)
            print(f"Saved {out_path}")
        else:
            print(f"Failed to download ICON: {url}")

def download_cmc_gribs(lat_min, lat_max, lon_min, lon_max, variables, hours=72, out_dir="cmc_gribs"):
    os.makedirs(out_dir, exist_ok=True)
    base_url = "https://dd.weather.gc.ca/model_gem_global/25km/grib2/lat_lon"
    now = datetime.utcnow()
    run_hour = now.hour // 6 * 6  # nearest 6-hour run
    run_date = now.strftime("%Y%m%d")
    run_str = f"{run_hour:02d}"
    for fh in range(0, hours+1, 3):  # 3-hourly steps
        url = f"{base_url}/{run_date}/{run_str}/CMC_glb_UGRD_TGL_10_latlon.24x.24_{run_date}{run_str}_P{fh:03d}.grib2"
        out_path = os.path.join(out_dir, f"cmc_{run_date}_{run_str}_f{fh:03d}.grib2")
        print(f"Downloading CMC: {url} ...")
        r = requests.get(url)
        if r.status_code == 200:
            with open(out_path, "wb") as f:
                f.write(r.content)
            print(f"Saved {out_path}")
        else:
            print(f"Failed to download CMC: {url}")

def download_hrrr_gribs(lat_min, lat_max, lon_min, lon_max, variables, hours=48, out_dir="hrrr_gribs"):
    os.makedirs(out_dir, exist_ok=True)
    base_url = "https://nomads.ncep.noaa.gov/cgi-bin/filter_hrrr_2d.pl"
    now = datetime.utcnow()
    run_hour = now.hour // 1 * 1  # nearest 1-hour run
    run_date = now.strftime("%Y%m%d")
    run_str = f"{run_hour:02d}"
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
        print(f"Downloading HRRR: {url} ...")
        r = requests.get(url)
        if r.status_code == 200:
            with open(out_path, "wb") as f:
                f.write(r.content)
            print(f"Saved {out_path}")
        else:
            print(f"Failed to download HRRR: {url}")

if __name__ == "__main__":
    # Halifax Harbour region
    lat_min, lat_max = 44.5, 44.8
    lon_min, lon_max = -63.6, -63.4
    variables = ["u10", "v10", "t2m", "prate"]
    download_gfs_gribs(lat_min, lat_max, lon_min, lon_max, variables)
    download_icon_gribs(lat_min, lat_max, lon_min, lon_max, variables)
    download_cmc_gribs(lat_min, lat_max, lon_min, lon_max, variables)
    download_hrrr_gribs(lat_min, lat_max, lon_min, lon_max, variables) 