# Weather GRIB Comparison Tool

A Python tool for downloading and comparing weather forecasts from multiple models (GFS, ICON, CMC, and HRRR) for a specific region.

## Features

- Downloads GRIB2 forecast data from multiple weather models:
  - GFS (Global Forecast System)
  - ICON (ICOsahedral Nonhydrostatic)
  - CMC GDPS (Canadian Meteorological Centre)
  - HRRR (High-Resolution Rapid Refresh)
- Processes and analyzes temperature, precipitation, and wind data
- Generates comparative plots showing forecasts from all models
- Focuses on Halifax Harbour region (customizable)

## Requirements

- Python 3.9+
- Required packages:
  - xarray
  - cfgrib
  - pandas
  - matplotlib
  - requests

## Installation

1. Clone the repository:
```bash
git clone git@github.com:PLaRoche/wx-grib-comparison.git
cd wx-grib-comparison
```

2. Install required packages:
```bash
pip3 install xarray cfgrib pandas matplotlib requests
```

## Usage

Run the complete analysis with a single command:
```bash
python3 run_ensemble.py
```

This will:
1. Download forecast data from all models
2. Process the GRIB2 files
3. Generate plots showing the forecasts

## Project Structure

- `run_ensemble.py`: Main script that combines download and analysis
- `download_ensemble.py`: Functions for downloading GRIB2 data from different models
- `ensemble_analysis.py`: Functions for processing and visualizing the data

## License

MIT License 