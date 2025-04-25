# ETL Pipeline Setup Guide

## Prerequisites
- Python 3.10+
- MongoDB Atlas account
- GitHub account for CI/CD

## Setup Instructions
1. Clone the repository:
https://github.com/hinasaeed25/ETL_Pipeline_HinaSaeed_DS-055-2025.git


2. Install dependencies:
   pip install -r requirements.txt

3. Configure MongoDB:
- Update `config/db_config.json` with your MongoDB Atlas URI.
  
4. Place data files in `data/`:
- `sample_data.csv`: Provided weather CSV.
- `sample_weather.json`: Simulated API data.
- `google_sheet_sample.csv`: Exported Google Sheets data.
  
5. Run the pipeline manually:
  python etl_pipeline.py
  python load_to_db.py
6. Automate daily runs:
   python scheduler.py

   Or use cron: `0 1 * * * /usr/bin/python3 /path/to/scheduler.py`

## CI/CD
- Push to GitHub to trigger the workflow defined in `.github/workflows/ci_cd.yml`.
- Tests run automatically on each commit/pull request.

## Output
- Processed data is saved to `output/final_cleaned_data.csv`.
- Data is loaded into MongoDB (`weather_db.weather_data`).
