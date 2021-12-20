import os
import sys
import json
import time

NODES = sc._jsc.sc().getExecutorMemoryStatus().size()
print(f"NODES: {NODES}")

w_path = os.path.abspath(__file__)
print(w_path)

sc.setLocalProperty("spark.databricks.devbricks.path", w_path)
sc.setLocalProperty("spark.databricks.devbricks.project_dir", os.path.dirname(w_path))


def command(cmd, key):
    import os
    import subprocess

    key_file = "/tmp/exec-" + key
    try:
        os.mkdir(key_file)  # so that a command is only run on a node once
    except Exception as e:
        print(e)
        return None

    if isinstance(cmd, str):
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        out, err = p.communicate()
        result = out.decode().strip(), err.decode().strip()
    else:
        result = cmd()

    hostname = subprocess.check_output("hostname")

    return hostname.decode().strip(), result


def get_sys_path():
    import sys
    return sys.path


def add_to_sys_path(args):
    import sys
    for arg in args:
        sys.path.insert(0, arg)
    return sys.path


def get_env_var(var):
    import os
    return os.getenv(var)


def run_cmd(cmd):
    key = str(time.monotonic())
    results = sc.parallelize(range(0, NODES)).map(lambda x: command(cmd, key)).collect()
    return list(filter(lambda x: x is not None, results))


data = dict()

data["pwd_driver"] = os.getcwd()
data["PYTHONPATH_driver"] = os.getenv("PYTHONPATH")
data["sys.path_driver"] = ",".join(sys.path)

dir_w_path = os.path.dirname(w_path)
print(f"change current directory to {dir_w_path}")

output = run_cmd(f"cd {dir_w_path}")
print("change dir")
print(output)

output = run_cmd(lambda: add_to_sys_path([dir_w_path]))
print("add to sys paths")
data["sys.path_workers_modified"] = output

pwd_workers = run_cmd("pwd")
data["pwd_workers"] = pwd_workers

sys_path_workers = run_cmd(get_sys_path)
data["sys.path_workers"] = sys_path_workers

data["sys.PYTHONPATH_workers"] = run_cmd("echo $PYTHONPATH")

ls_nfs = run_cmd("ls /local_disk0/.ephemeral_nfs/user-custom-module")
data["ls_nfs_workers"] = ls_nfs

with open("debug_main.json", "w") as fp:
    json.dump(data, fp, indent=2, sort_keys=True)

from mymodule import fns, spark_fns

print(fns.identity(5))
print(fns.square(5))

# test spark cmd with user custom modules
spark.range(1, 20).createOrReplaceTempView("test")
df = spark.table('test')
print(df.select('id').collect())
print(df.select("id", spark_fns.squared_udf("id").alias("id_squared")).collect())
