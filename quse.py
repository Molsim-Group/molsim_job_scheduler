import collections
import pathlib

import argparse
import subprocess
from xml.etree import ElementTree

import molsim_job_scheduler as mjs

parser = argparse.ArgumentParser(description="show q-status")
parser.add_argument(
    "-t", "--type", action='store', dest="type", type=str,
    choices=["by_node", "by_property"], default="by_property",
)
parser.add_argument("inputs", action='store', nargs='*', type=str)
args = parser.parse_args()
QSTAT_PATH = "/usr/local/pbs/bin/qstat"


class DataBase():
    def __init__(self):
        pass

    @property
    def config(self):
        config = {}
        with open(str(pathlib.Path(__file__).parent / "config.txt"), "r") as f:
            for line in f:
                if line.startswith("#"):
                    continue
                name, max_cores, limits = line.split()
                max_cores = int(max_cores)
                config[name] = max_cores
        return config

    @property
    def config_node(self):
        config_node = {}
        with open(str(pathlib.Path(__file__).parent / "config_node.txt"), "r") as f:
            for line in f:
                if line.startswith("#"):
                    continue
                name, property, max_cores = line.split()
                config_node[name] = (property, int(max_cores))
        return config_node

    @property
    def usage(self):
        stat_data = mjs.read_stat()
        usage = mjs.calculate_running_cores(stat_data)
        return usage

    @property
    def usage_node(self):
        defaultdict = collections.defaultdict
        usage_node = defaultdict(lambda: defaultdict(int))
        result = subprocess.run(
            [QSTAT_PATH, "-xf"], stdout=subprocess.PIPE
        )
    
        root = ElementTree.fromstring(result.stdout.decode("utf-8"))
        for e in root.findall("Job"):
            c = e.find("job_state")
            if c.text != "R":
                continue
            c = e.find("Job_Owner")
            user = c.text.split("@")[0]
            c = e.find("exec_host")
            info = [x.split("/")[0]for x in c.text.split("+")]
            for x in info:
                usage_node[user][x] += 1
        return usage_node


class Show():
    def __init__(self, args, config, usage):
        if len(args.inputs) == 1:
            self.target_user_pattern = args.inputs[0]
        else:
            self.target_user_pattern = ""
        self.config = config
        self.usage = usage

    def display_each(self):
        for user, data in self.usage.items():
            if not (self.target_user_pattern in user):
                continue
            print("="*45)
            print("User:", user)
            self.func_each(data)
            print("")
        self.display_all()

    def display_all(self):
        all_users = collections.defaultdict(int)
        for user, data in self.usage.items():
            for k, v in self.config.items():
                all_users[k] += data[k]
        print("="*45)
        print("All users")
        self.func_all(all_users)

    def func_each(self, data):
        raise NotImplementedError

    def func_all(self):
        raise NotImplementedError


class ShowByPro(Show):
    def __init__(self, args, config, usage):
        super().__init__(args, config, usage)

    def func_each(self, data):
        for k, v in self.config.items():
            print("{:15s} {:>6d} / {:<6d} ({:>6.1f} %)"
                  .format(k, data[k], v, data[k]/v*100))

    def func_all(self, all_users):
        for k, v in self.config.items():
            print("{:15s} {:>6d} / {:<6d} ({:>6.1f} %)"
                  .format(k, all_users[k], v, all_users[k]/v*100))


class ShowByNode(ShowByPro):
    def __init__(self, args, config, usage):
        super().__init__(args, config, usage)

    def func_each(self, data):
        for k, v in self.config.items():
            print("[{:8s}] {:>10s} : {:>10d} / {:<6d} ({:>6.1f} %)"
                  .format(k, v[0], data[k], v[1], data[k]/v[1]*100))

    def func_all(self, all_users):
        for k, v in self.config.items():
            print("[{:8s}] {:>10s} : {:>10d} / {:<6d} ({:>6.1f} %)"
                  .format(k, v[0], all_users[k], v[1], all_users[k]/v[1]*100))


def main():
    global args
    db = DataBase()
    dis_pro = ShowByPro(args, db.config, db.usage)
    dis_node = ShowByNode(args, db.config_node, db.usage_node)
    if args.type == "by_node":
        if not args.inputs:
            dis_node.target_user_pattern = ""
        dis_node.display_each()

    else:
        if not args.inputs:
            dis_pro.target_user_pattern = ""
        dis_pro.display_each()


if __name__ == "__main__":
    main()
