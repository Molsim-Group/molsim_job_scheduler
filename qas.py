import sys
import pathlib

import zmq
import molsim_job_scheduler as mjs

#print (sys.argv)
assert len(sys.argv) > 1

job_ls = []
for job in sys.argv[1:]:
    qsub_file = pathlib.Path(job)

    # Check node exists
    assert qsub_file.exists(), f'{qsub_file} does not exists'

    # Check node format
    nodes = mjs.extract_nodes_from_qsub(qsub_file)
    assert nodes is not None, f'{qsub_file} have a wrong node information.'
    mjs.parse_nodes(nodes)
    
    # Append resolve path
    job_ls.append(str(qsub_file.resolve()))

context = zmq.Context()

#  Socket to talk to server
socket = context.socket(zmq.REQ)
socket.connect("tcp://localhost:{}".format(mjs.JobManipulator.PORT))

# message = "qas|qsubfile"
message = "qas|" + "|".join(job_ls)
socket.send(message.encode())

for qsub_file in sys.argv[1:]:
    print(qsub_file)
print(socket.recv().decode("utf-8"))
