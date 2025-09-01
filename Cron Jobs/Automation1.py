import schedule
from schedule import every, repeat, cancel_job
import time as tm
from datetime import datetime, time, timedelta

def job():
    print("First ever tutorial")

j = schedule.every(3).seconds.do(job)

counter = 0

while True:
    schedule.run_pending()
    tm.sleep(1)
    counter += 1
    if counter == 10:
        schedule.cancel_job(j)
        print("Job cancelled")


