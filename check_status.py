from pathlib import Path
import subprocess
from subprocess import PIPE, TimeoutExpired
import time

############################################
timeout = 30              # 20 second
time_interval = 10 * 60   # 10 min
test_qsub_path = './test_qsub/test.qsub'
restart_path = './run.sh'
logger = './restart.log'
###########################################

test_qsub_path = Path(test_qsub_path).resolve()
restart_path = Path(restart_path).resolve()
logger = Path(logger).resolve()


def check_status():
    popen = subprocess.Popen(['qas', str(test_qsub_path)], stdout=PIPE, stderr=PIPE)
   
    try:
        out, err = popen.communicate(timeout=timeout) 
        assert not err
    except (TimeoutExpired, AssertionError, NameError):
        return False
    else:
        return True


datetime = time.strftime('%Y.%m.%d - %X')
with logger.open('w') as f:
    f.write(f'{datetime} | start\n')


while True:
    if check_status():
        ans = 'success'
    else:
        ans = 'failed\t|'
        popen = subprocess.Popen(['sh', str(restart_path)], #, '</dev/null', '>/dev/null', '2>&1'],
                                  stdout=PIPE, stderr=PIPE)
        try:
            out, err = popen.communicate(timeout=timeout)
        except TimeoutExpired:
            out, err = None, None

        if err:
            ans += err.decode('utf-8')
        else:
            ans += 'restart successfuly'

    datetime = time.strftime('%Y.%m.%d - %X')
    with logger.open('a') as f:
        f.write(f'{datetime} | {ans}\n')

    time.sleep(time_interval)
