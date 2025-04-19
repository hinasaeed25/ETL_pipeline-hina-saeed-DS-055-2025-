import schedule
import time
from etl_pipeline import run_etl
from load_to_db import load_to_mongodb, load_to_postgres

def job():
    print("Running ETL pipeline...")
    final_df = run_etl()
    load_to_mongodb(final_df)
    load_to_postgres(final_df)
    print("ETL pipeline completed.")

# Schedule daily at 1:00 AM
schedule.every().day.at("01:00").do(job)

while True:
    schedule.run_pending()
    time.sleep(60)  # Check every minute
