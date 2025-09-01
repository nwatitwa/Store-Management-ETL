import schedule
from schedule import every, repeat, cancel_job
import time as tm
from datetime import datetime, time, timedelta
from etl import run_etl

# Schedule the ETL to run every day at 7 PM
schedule.every().day.at("19:00").do(run_etl)

print("⏰ Scheduler started. Waiting for 7 PM daily...")

# Keep the scheduler running
while True:
    schedule.run_pending()
    time.sleep(60)  # check every 60 seconds


