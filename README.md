# Weather GRIB Comparison Tool

A Python tool for downloading and comparing weather forecasts from multiple models (currently GFS and HRRR) for a specific region.

## Features

- Downloads GRIB2 forecast data from multiple weather models:
  - GFS (Global Forecast System)
  - HRRR (High-Resolution Rapid Refresh)
  - (Framework in place for ICON and CMC, but not currently active)
- Processes and analyzes temperature, precipitation, and wind data
- Generates comparative plots showing forecasts from all models
- Focuses on Halifax Harbour region (customizable)
- Robust variable extraction from GRIB files (handles different variable names and levels)
- Improved error handling for missing or ambiguous GRIB variables

## Requirements

- Python 3.9+
- Required packages:
  - xarray
  - cfgrib
  - pandas
  - matplotlib
  - seaborn
  - requests
  - numpy

## Installation

1. Clone the repository:
```bash
git clone git@github.com:PLaRoche/wx-grib-comparison.git
cd wx-grib-comparison
```

2. Create and activate a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Project Structure

- `run_ensemble.py`: Main script that combines download, processing, analysis, and visualization
- `download_ensemble.py`: Functions for downloading GRIB2 data from different models
- `process_ensemble.py`: Functions for processing GRIB2 files into DataFrames
- `analyze_ensemble.py`: Functions for analyzing the processed data
- `visualize_ensemble.py`: Functions for generating plots and visualizations

Directory Structure:
```
.
├── gribs/                    # Main directory for all GRIB files
│   ├── gfs_gribs/           # GFS model files
│   ├── hrrr_gribs/          # HRRR model files
│   ├── icon_gribs/          # ICON model files
│   ├── cmc_gribs/           # CMC model files
│   ├── nam_gribs/           # NAM model files
│   ├── nbm_gribs/           # NBM model files
│   └── rap_gribs/           # RAP model files
├── ensemble_output/         # Output directory for analysis and plots
└── ...                     # Other project files
```

## Usage

1. Configure your analysis parameters in `run_ensemble.py`:
   - Set your location coordinates
   - Define forecast variables
   - Specify time ranges
   - Configure visualization options

2. Run the analysis:
```bash
python3 run_ensemble.py [--skip-download] [--hours HOURS]
```

Arguments:
- `--skip-download`: Optional. Skip downloading new forecast data and use existing files in the data directory.
- `--hours`: Optional. Number of hours of weather data to download (default: 72).

This will:
1. Download forecast data from all configured models (unless --skip-download is specified)
2. Process the GRIB2 files
3. Analyze and generate plots showing the forecasts

## Notes

- The code is currently set up for the Halifax Harbour region, but you can change the latitude/longitude bounds in `run_ensemble.py`.
- All models (GFS, HRRR, ICON, CMC, NAM, NBM, RAP) are supported and will be downloaded and processed.
- The code robustly handles variable extraction and missing data in GRIB files, making it more reliable for operational use.
- The `--hours` argument allows you to customize the forecast window (default is 72 hours).

## License

MIT License 