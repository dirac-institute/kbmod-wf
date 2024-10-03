import os
import datetime
from parsl import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider, SlurmProvider
from parsl.utils import get_all_checkpoints

walltimes = {
    "compute_bigmem": "01:00:00",
    "large_mem": "04:00:00",
    "sharded_reproject": "04:00:00",
    "gpu_max": "08:00:00",
}


def klone_resource_config():
    return Config(
        app_cache=True,
        checkpoint_mode="task_exit",
        checkpoint_files=get_all_checkpoints(
            os.path.join(os.path.abspath(os.curdir), datetime.date.today().isoformat())
        ),
        run_dir=os.path.join(os.path.abspath(os.curdir), datetime.date.today().isoformat()),
        retries=1,
        executors=[
            HighThroughputExecutor(
                label="ckpt_96gb_8cpus",
                max_workers=1,  # Do we mean max_workers_per_node here?
                provider=SlurmProvider(
                    partition="gpu-a40", # ckpt-all
                    account="escience", # astro
                    min_blocks=0,
                    max_blocks=5,
                    init_blocks=0,
                    parallelism=1,
                    nodes_per_block=1,
                    mem_per_node=12, # 96 GB
                    cores_per_node=8,
                    exclusive=False,
                    walltime=walltimes["sharded_reproject"],
                    # Command to run before starting worker - i.e. conda activate <special_env>
                    worker_init="",
                ),
            ),
            HighThroughputExecutor(
                label="astro_2gb_2cpus",
                max_workers=1,  # Do we mean max_workers_per_node here?
                provider=SlurmProvider(
                    partition="gpu-a40", # ckpt-all
                    account="escience", # astro
                    min_blocks=0,
                    max_blocks=5,
                    init_blocks=0,
                    parallelism=1,
                    nodes_per_block=1,
                    mem_per_node=4,
                    cores_per_node=2,
                    exclusive=False,
                    walltime=walltimes["sharded_reproject"],
                    # Command to run before starting worker - i.e. conda activate <special_env>
                    worker_init="",
                ),
            ),
            HighThroughputExecutor(
                label="esci_2gb_2cpus",
                max_workers=1,  # Do we mean max_workers_per_node here?
                provider=SlurmProvider(
                    partition="gpu-a40", # ckpt-all
                    account="escience", # astro
                    min_blocks=0,
                    max_blocks=5,
                    init_blocks=0,
                    parallelism=1,
                    nodes_per_block=1,
                    mem_per_node=4,
                    cores_per_node=2,
                    exclusive=False,
                    walltime=walltimes["sharded_reproject"],
                    # Command to run before starting worker - i.e. conda activate <special_env>
                    worker_init="",
                ),
            ),
            HighThroughputExecutor(
                label="ckpt_2gb_2cpus",
                max_workers=1,  # Do we mean max_workers_per_node here?
                provider=SlurmProvider(
                    partition="gpu-a40", # ckpt-all
                    account="escience", # astro
                    min_blocks=0,
                    max_blocks=5,
                    init_blocks=0,
                    parallelism=1,
                    nodes_per_block=1,
                    mem_per_node=4,
                    cores_per_node=2,
                    exclusive=False,
                    walltime=walltimes["sharded_reproject"],
                    # Command to run before starting worker - i.e. conda activate <special_env>
                    worker_init="",
                ),
            ),
            HighThroughputExecutor(
                label="gpu",
                max_workers=1,
                provider=SlurmProvider(
                    partition="gpu-a40",
                    account="escience",
                    min_blocks=0,
                    max_blocks=4,
                    init_blocks=0,
                    parallelism=1,
                    nodes_per_block=1,
                    cores_per_node=2,  # perhaps should be 8???
                    mem_per_node=12,  # 64 In GB
                    exclusive=False,
                    walltime=walltimes["gpu_max"],
                    # Command to run before starting worker - i.e. conda activate <special_env>
                    worker_init="",
                    scheduler_options="#SBATCH --gpus=1",
                ),
            ),
        ],
    )
