import pandas as pd
from datetime import datetime

# Extract
def extract_csv(file_path='data/weather_data.csv'):
    return pd.read_csv(file_path)

# Transform
def clean_data(df):
    df = df.dropna(subset=['Formatted Date'])
    df = df.drop_duplicates(subset=['Formatted Date'])
    if 'Wind Speed (km/h)' in df.columns:
        df = df[df['Wind Speed (km/h)'] >= 0]
    if 'Pressure (millibars)' in df.columns:
        df = df[df['Pressure (millibars)'] >= 0]
    numeric_cols = df.select_dtypes(include=['float64']).columns
    df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].median())
    return df

def standardize_timestamps(df):
    df['timestamp_utc'] = pd.to_datetime(df['Formatted Date'], utc=True).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    return df

def feature_engineering(df):
    if all(col in df.columns for col in ['Temperature (C)', 'Humidity', 'Wind Speed (km/h)']):
        df['weather_impact_score'] = (
            df['Temperature (C)'] * 0.4 + 
            df['Humidity'] * 0.3 + 
            df['Wind Speed (km/h)'] * 0.3
        )
    return df

def run_etl():
    # Extract
    csv_data = extract_csv()
    
    # Transform
    dataframes = [csv_data]
    cleaned_dfs = [clean_data(df) for df in dataframes]
    timestamp_dfs = [standardize_timestamps(df) for df in cleaned_dfs]
    enriched_dfs = [feature_engineering(df) for df in timestamp_dfs]
    final_df = pd.concat(enriched_dfs, ignore_index=True)
    
    return final_df

if __name__ == "__main__":
    final_df = run_etl()
    print(final_df.head())
