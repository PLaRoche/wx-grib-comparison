import pandas as pd
import numpy as np
import logging
import xarray as xr
from datetime import datetime
import re

logger = logging.getLogger(__name__)

def _extract_var(ds, varnames, valid_time):
    """Helper to extract a variable from a dataset, handling scalars and arrays."""
    for var in varnames:
        if var in ds:
            arr = ds[var].values
            # If scalar, make it an array
            if np.isscalar(arr):
                arr = np.array([arr])
            arr = np.asarray(arr).flatten()
            # Pad or trim to match valid_time
            if len(arr) < len(valid_time):
                arr = np.pad(arr, (0, len(valid_time)-len(arr)), constant_values=np.nan)
            elif len(arr) > len(valid_time):
                arr = arr[:len(valid_time)]
            return arr
    return np.full(len(valid_time), np.nan)

def _print_ds_debug(ds, label):
    logger.debug(f"{label} dataset structure: {ds}")
    logger.debug(f"Variables: {list(ds.variables.keys())}")
    for var in ds.variables:
        logger.debug(f"{label} variable '{var}': shape {ds[var].shape}, dtype {ds[var].dtype}")

def process_hrrr_data(hrrr_files):
    """
    Process HRRR GRIB files and convert to pandas DataFrame.
    """
    try:
        data = []
        for i, file in enumerate(hrrr_files):
            try:
                ds_wind = xr.open_dataset(file, engine='cfgrib', filter_by_keys={'typeOfLevel': 'heightAboveGround', 'level': 10})
                ds_temp = xr.open_dataset(file, engine='cfgrib', filter_by_keys={'typeOfLevel': 'heightAboveGround', 'level': 2})
                ds_prate = xr.open_dataset(file, engine='cfgrib', filter_by_keys={'typeOfLevel': 'surface'})
                if i == 0:
                    _print_ds_debug(ds_wind, 'HRRR wind')
                    _print_ds_debug(ds_temp, 'HRRR temp')
                    _print_ds_debug(ds_prate, 'HRRR prate')
            except Exception as e:
                logger.warning(f"Could not open HRRR datasets for {file}: {e}")
                continue
            # Extract valid time
            valid_time = ds_wind['time'].values
            if np.isscalar(valid_time):
                valid_time = np.array([valid_time])
            valid_time = pd.to_datetime(valid_time)
            
            # Extract forecast hour from filename
            match = re.search(r'f(\d{2})\.grib2$', file)
            if match:
                forecast_hour = int(match.group(1))
            else:
                logger.warning(f"Could not extract forecast hour from filename: {file}")
                continue
            
            # Extract variables
            u10 = _extract_var(ds_wind, ['u10', 'UGRD', 'UGRD_10m'], valid_time)
            v10 = _extract_var(ds_wind, ['v10', 'VGRD', 'VGRD_10m'], valid_time)
            t2m = _extract_var(ds_temp, ['t2m', 'TMP', '2t', 'TMP_2m'], valid_time)
            prate = _extract_var(ds_prate, ['prate', 'PRATE'], valid_time)
            
            # Calculate wind speed and direction
            wind_speed = np.sqrt(u10**2 + v10**2)
            wind_direction = (np.arctan2(u10, v10) * 180 / np.pi) % 360
            
            # Convert temperature from Kelvin to Celsius
            temperature = t2m - 273.15
            
            # Convert precipitation to mm/hour
            precipitation = prate * 3600
            
            for j, vt in enumerate(valid_time):
                data.append({
                    'timestamp': vt,
                    'forecast_hour': forecast_hour,
                    'temperature': float(temperature[j]) if j < len(temperature) else np.nan,
                    'wind_speed': float(wind_speed[j]) if j < len(wind_speed) else np.nan,
                    'wind_direction': float(wind_direction[j]) if j < len(wind_direction) else np.nan,
                    'precipitation': float(precipitation[j]) if j < len(precipitation) else np.nan,
                    'model': 'HRRR'
                })
        return pd.DataFrame(data)
    except Exception as e:
        logger.error(f"Error processing HRRR data: {str(e)}")
        return pd.DataFrame()

def process_gfs_data(gfs_files):
    """
    Process GFS GRIB files and convert to pandas DataFrame.
    """
    try:
        data = []
        for i, file in enumerate(gfs_files):
            try:
                ds_wind = xr.open_dataset(file, engine='cfgrib', filter_by_keys={'typeOfLevel': 'heightAboveGround', 'level': 10, 'stepType': 'instant'})
                ds_temp = xr.open_dataset(file, engine='cfgrib', filter_by_keys={'typeOfLevel': 'heightAboveGround', 'level': 2, 'stepType': 'instant'})
                ds_prate = xr.open_dataset(file, engine='cfgrib', filter_by_keys={'typeOfLevel': 'surface', 'stepType': 'instant'})
                if i == 0:
                    _print_ds_debug(ds_wind, 'GFS wind')
                    _print_ds_debug(ds_temp, 'GFS temp')
                    _print_ds_debug(ds_prate, 'GFS prate')
            except Exception as e:
                logger.warning(f"Could not open GFS datasets for {file}: {e}")
                continue
            valid_time = ds_wind['time'].values
            if np.isscalar(valid_time):
                valid_time = np.array([valid_time])
            valid_time = pd.to_datetime(valid_time)
            
            # Extract forecast hour from filename
            match = re.search(r'f(\d{3})\.grib2$', file)
            if match:
                forecast_hour = int(match.group(1))
            else:
                logger.warning(f"Could not extract forecast hour from filename: {file}")
                continue
            
            u10 = _extract_var(ds_wind, ['u10', 'UGRD', 'UGRD_10m'], valid_time)
            v10 = _extract_var(ds_wind, ['v10', 'VGRD', 'VGRD_10m'], valid_time)
            t2m = _extract_var(ds_temp, ['t2m', 'TMP', '2t', 'TMP_2m'], valid_time)
            prate = _extract_var(ds_prate, ['prate', 'PRATE'], valid_time)
            wind_speed = np.sqrt(u10**2 + v10**2)
            wind_direction = (np.arctan2(u10, v10) * 180 / np.pi) % 360
            temperature = t2m - 273.15
            precipitation = prate * 3600
            for j, vt in enumerate(valid_time):
                data.append({
                    'timestamp': vt,
                    'forecast_hour': forecast_hour,
                    'temperature': float(temperature[j]) if j < len(temperature) else np.nan,
                    'wind_speed': float(wind_speed[j]) if j < len(wind_speed) else np.nan,
                    'wind_direction': float(wind_direction[j]) if j < len(wind_direction) else np.nan,
                    'precipitation': float(precipitation[j]) if j < len(precipitation) else np.nan,
                    'model': 'GFS'
                })
        return pd.DataFrame(data)
    except Exception as e:
        logger.error(f"Error processing GFS data: {str(e)}")
        return pd.DataFrame()

def process_icon_data(icon_files):
    """
    Process ICON GRIB files and convert to pandas DataFrame.
    """
    try:
        data = []
        # Group files by forecast hour
        files_by_hour = {}
        for file in icon_files:
            match = re.search(r'_f(\d{3})_', file)
            if match:
                forecast_hour = int(match.group(1))
                if forecast_hour not in files_by_hour:
                    files_by_hour[forecast_hour] = []
                files_by_hour[forecast_hour].append(file)
        
        # Process each forecast hour
        for forecast_hour, hour_files in files_by_hour.items():
            try:
                # Find files for each variable
                u10_file = next((f for f in hour_files if '_u10.grib2' in f), None)
                v10_file = next((f for f in hour_files if '_v10.grib2' in f), None)
                t2m_file = next((f for f in hour_files if '_t2m.grib2' in f), None)
                prate_file = next((f for f in hour_files if '_prate.grib2' in f), None)
                
                if not all([u10_file, v10_file, t2m_file, prate_file]):
                    logger.warning(f"Missing files for forecast hour {forecast_hour}")
                    continue
                
                # Open datasets
                ds_wind_u = xr.open_dataset(u10_file, engine='cfgrib')
                ds_wind_v = xr.open_dataset(v10_file, engine='cfgrib')
                ds_temp = xr.open_dataset(t2m_file, engine='cfgrib')
                ds_prate = xr.open_dataset(prate_file, engine='cfgrib')
                
                # Extract valid time from any dataset (they should all have the same time)
                valid_time = ds_wind_u['time'].values
                if np.isscalar(valid_time):
                    valid_time = np.array([valid_time])
                valid_time = pd.to_datetime(valid_time)
                
                # Extract variables
                u10 = _extract_var(ds_wind_u, ['u10', 'U_10M'], valid_time)
                v10 = _extract_var(ds_wind_v, ['v10', 'V_10M'], valid_time)
                t2m = _extract_var(ds_temp, ['t2m', 'T_2M'], valid_time)
                prate = _extract_var(ds_prate, ['prate', 'PRATE'], valid_time)
                
                # Calculate wind speed and direction
                wind_speed = np.sqrt(u10**2 + v10**2)
                wind_direction = (np.arctan2(u10, v10) * 180 / np.pi) % 360
                
                # Convert temperature from Kelvin to Celsius
                temperature = t2m - 273.15
                
                # Convert precipitation to mm/hour
                precipitation = prate * 3600
                
                for j, vt in enumerate(valid_time):
                    data.append({
                        'timestamp': vt,
                        'forecast_hour': forecast_hour,
                        'temperature': float(temperature[j]) if j < len(temperature) else np.nan,
                        'wind_speed': float(wind_speed[j]) if j < len(wind_speed) else np.nan,
                        'wind_direction': float(wind_direction[j]) if j < len(wind_direction) else np.nan,
                        'precipitation': float(precipitation[j]) if j < len(precipitation) else np.nan,
                        'model': 'ICON'
                    })
            except Exception as e:
                logger.warning(f"Error processing ICON data for forecast hour {forecast_hour}: {str(e)}")
                continue
        
        return pd.DataFrame(data)
    except Exception as e:
        logger.error(f"Error processing ICON data: {str(e)}")
        return pd.DataFrame()

def process_cmc_data(cmc_files):
    """
    Process CMC GRIB files and convert to pandas DataFrame.
    """
    try:
        data = []
        for i, file in enumerate(cmc_files):
            try:
                ds_wind = xr.open_dataset(file, engine='cfgrib', filter_by_keys={'typeOfLevel': 'heightAboveGround', 'level': 10})
                ds_temp = xr.open_dataset(file, engine='cfgrib', filter_by_keys={'typeOfLevel': 'heightAboveGround', 'level': 2})
                ds_prate = xr.open_dataset(file, engine='cfgrib', filter_by_keys={'typeOfLevel': 'surface'})
                if i == 0:
                    _print_ds_debug(ds_wind, 'CMC wind')
                    _print_ds_debug(ds_temp, 'CMC temp')
                    _print_ds_debug(ds_prate, 'CMC prate')
            except Exception as e:
                logger.warning(f"Could not open CMC datasets for {file}: {e}")
                continue
            valid_time = ds_wind['time'].values
            if np.isscalar(valid_time):
                valid_time = np.array([valid_time])
            valid_time = pd.to_datetime(valid_time)
            
            # Extract forecast hour from filename
            match = re.search(r'f(\d{3})_', file)
            if match:
                forecast_hour = int(match.group(1))
            else:
                logger.warning(f"Could not extract forecast hour from filename: {file}")
                continue
            
            u10 = _extract_var(ds_wind, ['u10', 'UGRD_TGL_10m'], valid_time)
            v10 = _extract_var(ds_wind, ['v10', 'VGRD_TGL_10m'], valid_time)
            t2m = _extract_var(ds_temp, ['t2m', 'TMP_TGL_2m'], valid_time)
            prate = _extract_var(ds_prate, ['prate', 'APCP_SFC_0'], valid_time)
            wind_speed = np.sqrt(u10**2 + v10**2)
            wind_direction = (np.arctan2(u10, v10) * 180 / np.pi) % 360
            temperature = t2m - 273.15
            precipitation = prate * 3600
            for j, vt in enumerate(valid_time):
                data.append({
                    'timestamp': vt,
                    'forecast_hour': forecast_hour,
                    'temperature': float(temperature[j]) if j < len(temperature) else np.nan,
                    'wind_speed': float(wind_speed[j]) if j < len(wind_speed) else np.nan,
                    'wind_direction': float(wind_direction[j]) if j < len(wind_direction) else np.nan,
                    'precipitation': float(precipitation[j]) if j < len(precipitation) else np.nan,
                    'model': 'CMC'
                })
        return pd.DataFrame(data)
    except Exception as e:
        logger.error(f"Error processing CMC data: {str(e)}")
        return pd.DataFrame()

def process_nam_data(nam_files):
    """
    Process NAM GRIB files and convert to pandas DataFrame.
    """
    try:
        data = []
        for i, file in enumerate(nam_files):
            try:
                ds_wind = xr.open_dataset(file, engine='cfgrib', filter_by_keys={'typeOfLevel': 'heightAboveGround', 'level': 10})
                ds_temp = xr.open_dataset(file, engine='cfgrib', filter_by_keys={'typeOfLevel': 'heightAboveGround', 'level': 2})
                ds_prate = xr.open_dataset(file, engine='cfgrib', filter_by_keys={'typeOfLevel': 'surface'})
                if i == 0:
                    _print_ds_debug(ds_wind, 'NAM wind')
                    _print_ds_debug(ds_temp, 'NAM temp')
                    _print_ds_debug(ds_prate, 'NAM prate')
            except Exception as e:
                logger.warning(f"Could not open NAM datasets for {file}: {e}")
                continue
            valid_time = ds_wind['time'].values
            if np.isscalar(valid_time):
                valid_time = np.array([valid_time])
            valid_time = pd.to_datetime(valid_time)
            
            # Extract forecast hour from filename
            match = re.search(r'f(\d{3})\.grib2$', file)
            if match:
                forecast_hour = int(match.group(1))
            else:
                logger.warning(f"Could not extract forecast hour from filename: {file}")
                continue
            
            u10 = _extract_var(ds_wind, ['u10', 'UGRD', 'UGRD_10m'], valid_time)
            v10 = _extract_var(ds_wind, ['v10', 'VGRD', 'VGRD_10m'], valid_time)
            t2m = _extract_var(ds_temp, ['t2m', 'TMP', '2t', 'TMP_2m'], valid_time)
            prate = _extract_var(ds_prate, ['prate', 'PRATE'], valid_time)
            wind_speed = np.sqrt(u10**2 + v10**2)
            wind_direction = (np.arctan2(u10, v10) * 180 / np.pi) % 360
            temperature = t2m - 273.15
            precipitation = prate * 3600
            for j, vt in enumerate(valid_time):
                data.append({
                    'timestamp': vt,
                    'forecast_hour': forecast_hour,
                    'temperature': float(temperature[j]) if j < len(temperature) else np.nan,
                    'wind_speed': float(wind_speed[j]) if j < len(wind_speed) else np.nan,
                    'wind_direction': float(wind_direction[j]) if j < len(wind_direction) else np.nan,
                    'precipitation': float(precipitation[j]) if j < len(precipitation) else np.nan,
                    'model': 'NAM'
                })
        return pd.DataFrame(data)
    except Exception as e:
        logger.error(f"Error processing NAM data: {str(e)}")
        return pd.DataFrame() 