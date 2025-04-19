import pandas as pd
import json
from pymongo import MongoClient
from sqlalchemy import create_engine

# Load configuration
with open('config/db_config.json', 'r') as f:
    config = json.load(f)

# MongoDB loading
def load_to_mongodb(df):
    client = MongoClient(config['mongo_uri'])
    db = client['ride_weather_db']
    collection = db['weather_data']
    records = df.to_dict('records')
    collection.delete_many({})
    collection.insert_many(records)
    print(f"Inserted {len(records)} records into MongoDB.")

# PostgreSQL loading
def load_to_postgres(df):
    engine = create_engine(
        f"postgresql://{config['postgres']['user']}:{config['postgres']['password']}@"
        f"{config['postgres']['host']}:{config['postgres']['port']}/{config['postgres']['database']}"
    )
    df.to_sql('weather_data', engine, if_exists='replace', index=False)
    print("Data loaded to PostgreSQL successfully!")

if __name__ == "__main__":
    from etl_pipeline import run_etl
    final_df = run_etl()
    load_to_mongodb(final_df)
    load_to_postgres(final_df)
