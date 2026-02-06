import time
import schedule
from config import INTERVAL_MINUTES
from pipeline import run_pipeline

def job():
    try:
        run_pipeline()
    except Exception as e:
        print('Job failed:', e)

if __name__ == '__main__':
    print(f'Starting scheduler: running every {INTERVAL_MINUTES} minutes')
    # run once at startup
    job()
    schedule.every(INTERVAL_MINUTES).minutes.do(job)
    while True:
        schedule.run_pending()
        time.sleep(1)
