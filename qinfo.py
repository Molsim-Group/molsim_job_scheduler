import pathlib
import argparse
import molsim_job_scheduler as mjs

parser = argparse.ArgumentParser(description="show q-info")
parser.add_argument(
    "-u", "--user", type=str, default='all', dest='user', action='store',
)
#parser.add_argment("inputs", action='store', nargs='*', type=str)
args = parser.parse_args()

def check_pbc_name(_dir, _file):
    path = pathlib.Path("{}/{}".format(_dir, _file))
    try:
        with path.open("r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                tokens = line.split()
                if tokens[0] == "#PBS" and tokens[1] == "-N":
                    return " ".join(tokens[2:])
            return None
    except:
        return None    
    
def main():
    global args
    user = args.user

    jobs = mjs.read_jobs()
    print("="*(15+20+25+15))
    print("{:15s} {:20s} {:25s} {:15s}".format("Id", "Nodes", "File", "User"))
    print("="*(15+20+25+15))
    for job in jobs:
        name = check_pbc_name(job.dir, job.file)
        if name is None:
           name = job.file

        if user=='all' or user==job.user:
            print("{:<15d} {:20s} {:<25s} {:<15s}"
                .format(job.id, job.nodes, name[:25], job.user)
            )

if __name__ == "__main__":
    main()

