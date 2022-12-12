kill $( ps aux | grep molsim_job_scheduler.py | grep -v grep | awk '{print $2}')
rm -f nohup.out
cp -f jobs.dat jobs_backup.dat
cp -f stat.dat stat_backup.dat
nohup python3 molsim_job_scheduler.py &
