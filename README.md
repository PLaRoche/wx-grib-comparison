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

## Usage

1. Configure your analysis parameters in `run_ensemble.py`:
   - Set your location coordinates
   - Define forecast variables
   - Specify time ranges
   - Configure visualization options

2. Run the analysis:
```bash
python3 run_ensemble.py [--skip-download]
```

Arguments:
- `--skip-download`: Optional. Skip downloading new forecast data and use existing files in the data directory.

This will:
1. Download forecast data from GFS and HRRR (unless --skip-download is specified)
2. Process the GRIB2 files
3. Analyze and generate plots showing the forecasts

## Project Structure

- `run_ensemble.py`: Main script that combines download, processing, analysis, and visualization
- `download_ensemble.py`: Functions for downloading GRIB2 data from different models
- `process_ensemble.py`: Functions for processing GRIB2 files into DataFrames
- `analyze_ensemble.py`: Functions for analyzing the processed data
- `visualize_ensemble.py`: Functions for generating plots and visualizations

## Notes

- The code is currently set up for the Halifax Harbour region, but you can change the latitude/longitude bounds in `run_ensemble.py`.
- Only GFS and HRRR are currently downloaded and processed. ICON and CMC support can be re-enabled by updating the workflow and download functions.
- The code robustly handles variable extraction and missing data in GRIB files, making it more reliable for operational use.

## License

MIT License 