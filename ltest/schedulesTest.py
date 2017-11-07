from apscheduler.schedulers.blocking import BlockingScheduler

sched = BlockingScheduler()


@sched.scheduled_job('interval', seconds=3)
def timed_job():
    print('This job is run every three minutes.')


@sched.scheduled_job('cron', day_of_week='mon-fri', hour='0-23', minute='10-59', second='*/3')
def scheduled_job():
    print('This job is run every weekday at 5pm.')


print('before the start funciton')
sched.start()
print("let us figure out the situation")