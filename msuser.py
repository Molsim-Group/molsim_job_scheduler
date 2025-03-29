import sys
import pathlib
import argparse
import getpass

import zmq
import molsim_job_scheduler as mjs

user = getpass.getuser()
context = zmq.Context()

socket = context.socket(zmq.REQ)
socket.connect("tcp://localhost:{}".format(mjs.JobManipulator.PORT))

message = "|".join(['msuser']+sys.argv[1:])
socket.send(message.encode())
print(socket.recv().decode('utf-8'))
