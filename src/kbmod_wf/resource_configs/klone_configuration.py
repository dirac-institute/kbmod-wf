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
            os.path.join(os.path.abspath(os.curdir), "parsl_rundir")
        ),
        run_dir=os.path.join(os.path.abspath(os.curdir), "parsl_rundir"),
        retries=4,
        executors=[
            ####################
            #          Resample resources
            ####################
            HighThroughputExecutor(
                label="astro_96gb_8cpus",
                max_workers=1,
                provider=SlurmProvider(
                    partition="compute-bigmem",
                    account="astro", 
                    min_blocks=0,
                    max_blocks=4,   # Low block count for shared resource
                    init_blocks=0,
                    parallelism=1,
                    nodes_per_block=1,
                    mem_per_node=96, # 96 GB for >100, 48 for < 100
                    cores_per_node=8,
                    exclusive=False,
                    walltime=walltimes["sharded_reproject"],
                    worker_init="",
                ),
            ),
            HighThroughputExecutor(
                label="astro_48gb_8cpus",
                max_workers=1,
                provider=SlurmProvider(
                    partition="compute-bigmem",
                    account="astro",
                    min_blocks=0,
                    max_blocks=4,   # Low block count for shared resource
                    init_blocks=0,
                    parallelism=1,
                    nodes_per_block=1,
                    mem_per_node=48, # 96 GB for >100, 48 for < 100
                    cores_per_node=8,
                    exclusive=False,
                    walltime=walltimes["sharded_reproject"],
                    worker_init="",
                ),
            ),
            HighThroughputExecutor(
                label="esci_96gb_8cpus",
                max_workers=1,
                provider=SlurmProvider(
                    partition="gpu-a40",
                    account="escience",
                    min_blocks=0,
                    max_blocks=4,  # low block count for shared resources
                    init_blocks=0,
                    parallelism=1,
                    nodes_per_block=1,
                    mem_per_node=96, # 96 GB for >100, 48 for < 100
                    cores_per_node=8,
                    exclusive=False,
                    walltime=walltimes["sharded_reproject"],
                    worker_init="",
                ),
            ),
            HighThroughputExecutor(
                label="esci_48gb_8cpus",
                max_workers=1,
                provider=SlurmProvider(
                    partition="gpu-a40",
                    account="escience",
                    min_blocks=0,
                    max_blocks=4,  # low block count for shared resources
                    init_blocks=0,
                    parallelism=1,
                    nodes_per_block=1,
                    mem_per_node=48, # 96 GB for >100, 48 for < 100
                    cores_per_node=8,
                    exclusive=False,
                    walltime=walltimes["sharded_reproject"],
                    worker_init="",
                ),
            ),
            HighThroughputExecutor(
                label="ckpt_96gb_8cpus",
                max_workers=1,
                provider=SlurmProvider(
                    partition="ckpt-all",
                    account="astro",
                    min_blocks=0,
                    max_blocks=50,  # scale to the size of the GPU blocks, big number for low memory
                    init_blocks=0,
                    parallelism=1,
                    nodes_per_block=1,
                    mem_per_node=96,  # 96 GB for >100, 48 for < 100
                    cores_per_node=8,
                    exclusive=False,
                    walltime=walltimes["sharded_reproject"],
                    worker_init="",
                ),
            ),
            HighThroughputExecutor(
                label="ckpt_48gb_8cpus",
                max_workers=1,
                provider=SlurmProvider(
                    partition="ckpt-all",
                    account="astro",
                    min_blocks=0,
                    max_blocks=50,  # scale to the size of the GPU blocks, big number for low memory
                    init_blocks=0,
                    parallelism=1,
                    nodes_per_block=1,
                    mem_per_node=48, # 96 GB for >100, 48 for < 100
                    cores_per_node=8,
                    exclusive=False,
                    walltime=walltimes["sharded_reproject"],
                    worker_init="",
                ),
            ),
            ####################
            #          Search resources
            ####################
            HighThroughputExecutor(
                label="esci_96gb_2cpu_1gpu",
                max_workers=1,
                provider=SlurmProvider(
                    partition="gpu-a40",
                    account="escience",
                    min_blocks=0,
                    max_blocks=4,  # low block count for shared resource
                    init_blocks=0,
                    parallelism=1,
                    nodes_per_block=1,
                    cores_per_node=2,  # perhaps should be 8???
                    mem_per_node=96,  # 96 GB for >100, 48 for < 100
                    exclusive=False,
                    walltime=walltimes["gpu_max"],
                    worker_init="",
                    scheduler_options="#SBATCH --gpus=1",
                ),
            ),
            HighThroughputExecutor(
                label="esci_48gb_2cpu_1gpu",
                max_workers=1,
                provider=SlurmProvider(
                    partition="gpu-a40",
                    account="escience",
                    min_blocks=0,
                    max_blocks=4,  # low block count for shared resource
                    init_blocks=0,
                    parallelism=1,
                    nodes_per_block=1,
                    cores_per_node=2,  # perhaps should be 8???
                    mem_per_node=48,  # 96 GB for >100, 48 for < 100
                    exclusive=False,
                    walltime=walltimes["gpu_max"],
                    worker_init="",
                    scheduler_options="#SBATCH --gpus=1",
                ),
            ),
            HighThroughputExecutor(
                label="esci_32gb_2cpu_1gpu",
                max_workers=1,
                provider=SlurmProvider(
                    partition="gpu-a40",
                    account="escience",
                    min_blocks=0,
                    max_blocks=6,  # low block count for shared resource
                    init_blocks=0,
                    parallelism=1,
                    nodes_per_block=1,
                    cores_per_node=2,  # perhaps should be 8???
                    mem_per_node=32,  # 96 GB for >100, 48 for < 100
                    exclusive=False,
                    walltime=walltimes["gpu_max"],
                    worker_init="",
                    scheduler_options="#SBATCH --gpus=1",
                ),
            ),
            HighThroughputExecutor(
                label="ckpt_96gb_2cpu_1gpu",
                max_workers=1,
                provider=SlurmProvider(
                    partition="ckpt-g2",
                    account="escience",
                    min_blocks=0,
                    max_blocks=50, # 20 for 96, 50 for 48
                    init_blocks=0,
                    parallelism=1,
                    nodes_per_block=1,
                    cores_per_node=2,  # perhaps should be 8???
                    mem_per_node=96, # 96 GB for >100, 48 for < 100
                    exclusive=False,
                    walltime=walltimes["gpu_max"],
                    # Command to run before starting worker - i.e. conda activate <special_env>
                    worker_init="",
                    scheduler_options="#SBATCH --gpus=1",
                ),
            ),
            HighThroughputExecutor(
                label="ckpt_48gb_2cpu_1gpu",
                max_workers=1,
                provider=SlurmProvider(
                    partition="ckpt-g2",
                    account="escience",
                    min_blocks=0,
                    max_blocks=50, # 20 for 96, 50 for 48
                    init_blocks=0,
                    parallelism=1,
                    nodes_per_block=1,
                    cores_per_node=2,  # perhaps should be 8???
                    mem_per_node=48, # 96 GB for >100, 48 for < 100
                    exclusive=False,
                    walltime=walltimes["gpu_max"],
                    # Command to run before starting worker - i.e. conda activate <special_env>
                    worker_init="",
                    scheduler_options="#SBATCH --gpus=1",
                ),
            ),
            HighThroughputExecutor(
                label="ckpt_32gb_2cpu_1gpu",
                max_workers=1,
                provider=SlurmProvider(
                    partition="ckpt-g2",
                    account="escience",
                    min_blocks=0,
                    max_blocks=50, # 20 for 96, 50 for 48
                    init_blocks=0,
                    parallelism=1,
                    nodes_per_block=1,
                    cores_per_node=2,  # perhaps should be 8???
                    mem_per_node=32, # 96 GB for >100, 48 for < 100
                    exclusive=False,
                    walltime=walltimes["gpu_max"],
                    # Command to run before starting worker - i.e. conda activate <special_env>
                    worker_init="",
                    scheduler_options="#SBATCH --gpus=1",
                ),
            ),

            ####################
            #          Analysis resource
            ####################
            HighThroughputExecutor(
                label="astro_4gb_2cpus",
                max_workers=1,  # Do we mean max_workers_per_node here?
                provider=SlurmProvider(
                    partition="compute-bigmem", # ckpt-all
                    account="astro", # astro
                    min_blocks=0,
                    max_blocks=12, # low block count for shared resource
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
                label="esci_4gb_2cpus",
                max_workers=1,  # Do we mean max_workers_per_node here?
                provider=SlurmProvider(
                    partition="gpu-a40", # ckpt-all
                    account="escience", # astro
                    min_blocks=0,
                    max_blocks=12, # low block count for shared resource
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
                label="ckpt_4gb_2cpus",
                max_workers=1,  # Do we mean max_workers_per_node here?
                provider=SlurmProvider(
                    partition="ckpt-all", # ckpt-all
                    account="astro", # astro
                    min_blocks=0,
                    max_blocks=100, # can leave large at all times
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
        ],
    )
