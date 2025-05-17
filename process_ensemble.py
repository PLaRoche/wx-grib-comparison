import pandas as pd
import numpy as np
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def process_hrrr_data(hrrr_files):
    """
    Process HRRR GRIB files and convert to DataFrame.
    
    Args:
        hrrr_files (list): List of HRRR GRIB file paths
        
    Returns:
        pd.DataFrame: Processed HRRR data
    """
    try:
        import xarray as xr
        processed_data = []
        for file_path in hrrr_files:
            try:
                # Try to open wind at 10m
                ds_wind = xr.open_dataset(file_path, engine='cfgrib', filter_by_keys={'typeOfLevel': 'heightAboveGround', 'level': 10})
            except Exception as e:
                logger.warning(f"Could not open wind at 10m for {file_path}: {e}")
                ds_wind = None
            try:
                # Try to open temperature at 2m
                ds_temp = xr.open_dataset(file_path, engine='cfgrib', filter_by_keys={'typeOfLevel': 'heightAboveGround', 'level': 2})
            except Exception as e:
                logger.warning(f"Could not open temperature at 2m for {file_path}: {e}")
                ds_temp = None
            try:
                # Try to open precipitation at surface
                ds_prate = xr.open_dataset(file_path, engine='cfgrib', filter_by_keys={'typeOfLevel': 'surface'})
            except Exception as e:
                logger.warning(f"Could not open precipitation at surface for {file_path}: {e}")
                ds_prate = None

            # Extract valid time
            valid_time = None
            if ds_temp is not None and 'time' in ds_temp and 'step' in ds_temp:
                base_time = pd.to_datetime(ds_temp['time'].values)
                steps = ds_temp['step'].values
                if not hasattr(steps, '__len__') or isinstance(steps, str):
                    steps = [steps]
                valid_time = base_time + pd.to_timedelta(steps, unit='s')
            elif ds_wind is not None and 'time' in ds_wind and 'step' in ds_wind:
                base_time = pd.to_datetime(ds_wind['time'].values)
                steps = ds_wind['step'].values
                if not hasattr(steps, '__len__') or isinstance(steps, str):
                    steps = [steps]
                valid_time = base_time + pd.to_timedelta(steps, unit='s')
            elif ds_temp is not None and 'time' in ds_temp:
                valid_time = pd.to_datetime(ds_temp['time'].values)
            elif ds_wind is not None and 'time' in ds_wind:
                valid_time = pd.to_datetime(ds_wind['time'].values)
            else:
                logger.warning(f"No valid time found in {file_path}, skipping file.")
                continue
            if not hasattr(valid_time, '__len__') or isinstance(valid_time, str):
                valid_time = [valid_time]
            valid_time = np.array([pd.to_datetime(vt) for vt in valid_time])

            # Helper to extract variable arrays
            def get_var(ds, keys, convert=None):
                if ds is None:
                    return np.full(len(valid_time), np.nan)
                for key in keys:
                    if key in ds:
                        arr = ds[key].values
                        if not hasattr(arr, '__len__') or isinstance(arr, str):
                            arr = [arr]
                        arr = np.array(arr)
                        if convert:
                            arr = convert(arr)
                        if arr.ndim == 0:
                            arr = arr[None]
                        arr = arr.flatten()
                        if len(arr) < len(valid_time):
                            arr = np.pad(arr, (0, len(valid_time)-len(arr)), constant_values=np.nan)
                        elif len(arr) > len(valid_time):
                            arr = arr[:len(valid_time)]
                        return arr
                return np.full(len(valid_time), np.nan)

            # Extract variables
            u_arr = get_var(ds_wind, ["u10", "UGRD", "UGRD_10m"], None)
            v_arr = get_var(ds_wind, ["v10", "VGRD", "VGRD_10m"], None)
            temp_arr = get_var(ds_temp, ["t2m", "TMP", "2t", "TMP_2m"], lambda x: x - 273.15 if (x > 200).any() else x)
            prate_arr = get_var(ds_prate, ["prate", "PRATE"], lambda x: x * 3600 if (x < 1).all() else x)

            speed_arr = np.sqrt(u_arr**2 + v_arr**2)
            direction_arr = (np.arctan2(u_arr, v_arr) * 180 / np.pi) % 360

            for i, vt in enumerate(valid_time):
                processed_data.append({
                    'timestamp': vt,
                    'forecast_hour': (vt - valid_time[0]).total_seconds() // 3600,
                    'temperature': float(temp_arr[i]) if i < len(temp_arr) else np.nan,
                    'wind_speed': float(speed_arr[i]) if i < len(speed_arr) else np.nan,
                    'wind_direction': float(direction_arr[i]) if i < len(direction_arr) else np.nan,
                    'precipitation': float(prate_arr[i]) if i < len(prate_arr) else np.nan,
                    'model': 'HRRR'
                })
        if processed_data:
            return pd.DataFrame(processed_data)
        else:
            logger.warning("No HRRR data to process")
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error processing HRRR data: {str(e)}")
        raise

def process_gfs_data(gfs_files):
    """
    Process GFS GRIB files and convert to DataFrame.
    
    Args:
        gfs_files (list): List of GFS GRIB file paths
        
    Returns:
        pd.DataFrame: Processed GFS data
    """
    try:
        import xarray as xr
        processed_data = []
        for file_path in gfs_files:
            try:
                # Try to open wind at 10m (instantaneous)
                ds_wind = xr.open_dataset(file_path, engine='cfgrib', filter_by_keys={'typeOfLevel': 'heightAboveGround', 'level': 10, 'stepType': 'instant'})
            except Exception as e:
                logger.warning(f"Could not open wind at 10m for {file_path}: {e}")
                ds_wind = None
            try:
                # Try to open temperature at 2m (instantaneous)
                ds_temp = xr.open_dataset(file_path, engine='cfgrib', filter_by_keys={'typeOfLevel': 'heightAboveGround', 'level': 2, 'stepType': 'instant'})
            except Exception as e:
                logger.warning(f"Could not open temperature at 2m for {file_path}: {e}")
                ds_temp = None
            try:
                # Try to open precipitation at surface (averaged)
                ds_prate = xr.open_dataset(file_path, engine='cfgrib', filter_by_keys={'typeOfLevel': 'surface', 'stepType': 'avg'})
            except Exception as e:
                logger.warning(f"Could not open precipitation at surface for {file_path}: {e}")
                ds_prate = None

            # Extract valid time
            valid_time = None
            if ds_temp is not None and 'time' in ds_temp and 'step' in ds_temp:
                base_time = pd.to_datetime(ds_temp['time'].values)
                steps = ds_temp['step'].values
                if not hasattr(steps, '__len__') or isinstance(steps, str):
                    steps = [steps]
                valid_time = base_time + pd.to_timedelta(steps, unit='s')
            elif ds_wind is not None and 'time' in ds_wind and 'step' in ds_wind:
                base_time = pd.to_datetime(ds_wind['time'].values)
                steps = ds_wind['step'].values
                if not hasattr(steps, '__len__') or isinstance(steps, str):
                    steps = [steps]
                valid_time = base_time + pd.to_timedelta(steps, unit='s')
            elif ds_temp is not None and 'time' in ds_temp:
                valid_time = pd.to_datetime(ds_temp['time'].values)
            elif ds_wind is not None and 'time' in ds_wind:
                valid_time = pd.to_datetime(ds_wind['time'].values)
            else:
                logger.warning(f"No valid time found in {file_path}, skipping file.")
                continue
            if not hasattr(valid_time, '__len__') or isinstance(valid_time, str):
                valid_time = [valid_time]
            valid_time = np.array([pd.to_datetime(vt) for vt in valid_time])

            # Helper to extract variable arrays
            def get_var(ds, keys, convert=None):
                if ds is None:
                    return np.full(len(valid_time), np.nan)
                for key in keys:
                    if key in ds:
                        arr = ds[key].values
                        if not hasattr(arr, '__len__') or isinstance(arr, str):
                            arr = [arr]
                        arr = np.array(arr)
                        if convert:
                            arr = convert(arr)
                        if arr.ndim == 0:
                            arr = arr[None]
                        arr = arr.flatten()
                        if len(arr) < len(valid_time):
                            arr = np.pad(arr, (0, len(valid_time)-len(arr)), constant_values=np.nan)
                        elif len(arr) > len(valid_time):
                            arr = arr[:len(valid_time)]
                        return arr
                return np.full(len(valid_time), np.nan)

            # Extract variables
            u_arr = get_var(ds_wind, ["u10", "UGRD", "UGRD_10m"], None)
            v_arr = get_var(ds_wind, ["v10", "VGRD", "VGRD_10m"], None)
            temp_arr = get_var(ds_temp, ["t2m", "TMP", "2t", "TMP_2m"], lambda x: x - 273.15 if (x > 200).any() else x)
            prate_arr = get_var(ds_prate, ["prate", "PRATE"], lambda x: x * 3600 if (x < 1).all() else x)

            speed_arr = np.sqrt(u_arr**2 + v_arr**2)
            direction_arr = (np.arctan2(u_arr, v_arr) * 180 / np.pi) % 360

            for i, vt in enumerate(valid_time):
                processed_data.append({
                    'timestamp': vt,
                    'forecast_hour': (vt - valid_time[0]).total_seconds() // 3600,
                    'temperature': float(temp_arr[i]) if i < len(temp_arr) else np.nan,
                    'wind_speed': float(speed_arr[i]) if i < len(speed_arr) else np.nan,
                    'wind_direction': float(direction_arr[i]) if i < len(direction_arr) else np.nan,
                    'precipitation': float(prate_arr[i]) if i < len(prate_arr) else np.nan,
                    'model': 'GFS'
                })
        if processed_data:
            return pd.DataFrame(processed_data)
        else:
            logger.warning("No GFS data to process")
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error processing GFS data: {str(e)}")
        raise 