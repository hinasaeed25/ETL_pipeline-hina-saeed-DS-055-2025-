import pandas as pd
import json
from pymongo import MongoClient

# Load configuration
with open('config/db_config.json', 'r') as f:
    config = json.load(f)

def load_to_mongodb(df):
    client = MongoClient(config['mongo_uri'])
    db = client['weather_db']
    collection = db['weather_data']
    records = df.to_dict('records')
    collection.delete_many({})  # Clear existing data
    collection.insert_many(records)
    print(f"Inserted {len(records)} records into MongoDB.")

if __name__ == "__main__":
    from etl_pipeline import run_etl
    final_df = run_etl()
    load_to_mongodb(final_df)
