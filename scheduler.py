import schedule
import time
from datetime import datetime
from script import run_stock_ticker_job

def basic_job():
    print("Job started at:", datetime.now())

schedule.every().minute.do(basic_job)
schedule.every().minute.do(run_stock_ticker_job)


while True:
    schedule.run_pending()
    time.sleep(1)

# When I run this script, it will run the basic_job and the run_stock_ticker_job every minute. But it will only run when my laptop is on and won't run in the background.
# So for that we need to use a cron job to run the script in the background.

# put crontab -e in terminal to edit the cron 
# add the following line to the cronjob:
# * * * * * /usr/bin/python3 path/to/script.py  
# for example:
# * * * * * /usr/bin/python3 /home/user/Desktop/Scheduler.py
# !wq to write and exit and then crontab command to run it

# this will run the script every minute.
