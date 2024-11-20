import os
import datetime
from parsl import Config
from parsl.executors import ThreadPoolExecutor
from parsl.utils import get_all_checkpoints


this_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.abspath(os.path.join(this_dir, "../../../"))


def dev_resource_config():
    return Config(
        # put the log files in in the top level folder, "run_logs".
        run_dir=os.path.join(project_dir, "run_logs", datetime.date.today().isoformat()),
        executors=[ThreadPoolExecutor(label="local_dev_testing", max_threads=3)],
    )
