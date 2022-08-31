kill $( ps aux | grep molsim_job_scheduler.py | grep -v grep | awk '{print $2}')
rm -f nohup.out
nohup python3 molsim_job_scheduler.py &
