# Weather GRIB Comparison Tool

A Python application for downloading and comparing weather forecasts from multiple models (GFS, HRRR, ICON, CMC, NAM, RAP, NBM) for a specific region.

## Features

- Download GRIB2 forecast data from multiple weather models:
  - GFS (Global Forecast System)
  - HRRR (High-Resolution Rapid Refresh)
  - ICON (Icosahedral Nonhydrostatic)
  - CMC (Canadian Meteorological Centre)
  - NAM (North American Mesoscale)
  - RAP (Rapid Refresh)
  - NBM (National Blend of Models)
- Process and analyze data from all models
- Generate comparative plots
- Parallel downloads with progress tracking
- Automatic retry logic for failed downloads
- Support for different model resolutions

## Requirements

- Python 3.6+
- Required Python packages (install via `pip install -r requirements.txt`):
  - xarray
  - cfgrib
  - pandas
  - matplotlib
  - numpy
  - requests
  - beautifulsoup4
  - tqdm

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd ensemble-analysis
```

2. Install required packages:
```bash
python3 -m pip install -r requirements.txt
```

## Usage

Run the main script with your desired parameters:

```bash
python3 run_ensemble.py --lat-min 44.5 --lat-max 44.8 --lon-min -63.6 --lon-max -63.4 --hours 72
```

### Command-line Arguments

- `--lat-min`: Minimum latitude (default: 44.5)
- `--lat-max`: Maximum latitude (default: 44.8)
- `--lon-min`: Minimum longitude (default: -63.6)
- `--lon-max`: Maximum longitude (default: -63.4)
- `--hours`: Number of hours of weather data to download (default: 72)
- `--skip-download`: Skip downloading and use existing GRIB files
- `--skip-process`: Skip processing and use existing processed data
- `--skip-analyze`: Skip analysis and use existing analysis results
- `--skip-plot`: Skip plotting and use existing plots

### Model Resolutions

The tool supports different resolutions for each model:

- GFS: 0.25°, 0.5°, or 1.0°
- HRRR: 3km or 13km
- ICON: 13km or 7km
- CMC: 25km or 15km

### Download Features

- **Parallel Downloads**: Files are downloaded concurrently (default: 5 parallel downloads)
- **Progress Tracking**: Visual progress bars for each model's downloads
- **Retry Logic**: Automatic retries with exponential backoff for failed downloads
- **Compression Support**: Handles both regular and bz2 compressed files

## Project Structure

```
ensemble-analysis/
├── gribs/                    # Downloaded GRIB files
│   ├── gfs_gribs/           # GFS model files
│   ├── hrrr_gribs/          # HRRR model files
│   ├── icon_gribs/          # ICON model files
│   ├── cmc_gribs/           # CMC model files
│   ├── nam_gribs/           # NAM model files
│   ├── rap_gribs/           # RAP model files
│   └── nbm_gribs/           # NBM model files
├── ensemble_output/         # Analysis and visualization output
├── run_ensemble.py          # Main script
├── download_ensemble.py     # Download functions
├── process_ensemble.py      # Data processing
├── analyze_ensemble.py      # Analysis functions
├── visualize_ensemble.py    # Visualization code
└── requirements.txt         # Python dependencies
```

## Notes

- All models are downloaded by default
- The `--hours` argument controls how many hours of forecast data to download
- Files are downloaded in parallel with progress tracking
- Failed downloads are automatically retried with exponential backoff
- The tool supports both regular and compressed (bz2) GRIB files

## License

This project is licensed under the MIT License - see the LICENSE file for details. 