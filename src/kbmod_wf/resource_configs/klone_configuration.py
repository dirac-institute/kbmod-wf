import os
import datetime
from parsl import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider, SlurmProvider

walltimes = {
    "compute-bigmem": "01:00:00",  # change this to be appropriate
}


def klone_resource_config():
    return Config(
        run_dir=os.path.join("/gscratch/dirac/kbmod/workflow/run_logs", datetime.date.today().isoformat()),
        executors=[
            HighThroughputExecutor(
                label="small_cpu",
                provider=SlurmProvider(
                    partition="compute-bigmem",
                    account="astro",
                    min_blocks=0,
                    max_blocks=4,
                    init_blocks=1,
                    parallelism=1,
                    nodes_per_block=1,
                    cores_per_node=1,  # perhaps should be 8???
                    mem_per_node=64,  # In GB
                    exclusive=False,
                    walltime=walltimes["compute-bigmem"],
                    # Command to run before starting worker - i.e. conda activate <special_env>
                    worker_init="",
                ),
            ),
            HighThroughputExecutor(
                label="local_thread",
                provider=LocalProvider(
                    init_blocks=1,
                    max_blocks=1,
                ),
            ),
        ],
    )
