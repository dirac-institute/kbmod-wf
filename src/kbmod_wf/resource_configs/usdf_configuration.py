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

base_path = f"{os.environ['HOME']}/rubin-user/parsl/workflow_output"

account_name = "rubin:commissioning"


def usdf_resource_config():
    return Config(
        app_cache=True,
        checkpoint_mode="task_exit",
        checkpoint_files=get_all_checkpoints(
            os.path.join(base_path, "kbmod/workflow/run_logs", datetime.date.today().isoformat())
        ),
        run_dir=os.path.join(base_path, "kbmod/workflow/run_logs", datetime.date.today().isoformat()),
        retries=1,
        executors=[
            HighThroughputExecutor(
                label="small_cpu",
                max_workers=1,
                provider=SlurmProvider(
                    partition="milano",
                    account=account_name,
                    min_blocks=0,
                    max_blocks=4,
                    init_blocks=0,
                    parallelism=1,
                    nodes_per_block=1,
                    cores_per_node=1,  # perhaps should be 8???
                    mem_per_node=256,  # In GB; milano has a 256 Gb cap
                    exclusive=False,
                    walltime=walltimes["compute_bigmem"],
                    scheduler_options="#SBATCH --export=ALL",  # Add other options as needed
                    # Command to run before starting worker - i.e. conda activate <special_env>
                    worker_init="",
                ),
            ),
            HighThroughputExecutor(
                label="large_mem",
                max_workers=1,
                provider=SlurmProvider(
                    partition="ampere", # or ada?; note: milano has a 256 Gb cap
                    account=account_name,
                    min_blocks=0,
                    max_blocks=2,
                    init_blocks=0,
                    parallelism=1,
                    nodes_per_block=1,
                    cores_per_node=32,
                    mem_per_node=512,
                    exclusive=False,
                    walltime=walltimes["large_mem"],
                    scheduler_options="#SBATCH --export=ALL",  # Add other options as needed
                    # Command to run before starting worker - i.e. conda activate <special_env>
                    worker_init="",
                ),
            ),
            HighThroughputExecutor(
                label="sharded_reproject",
                max_workers=1,
                provider=SlurmProvider(
                    partition="ampere", # or ada?; note: milano has a 256 Gb cap
                    account=account_name,
                    min_blocks=0,
                    max_blocks=2,
                    init_blocks=0,
                    parallelism=1,
                    nodes_per_block=1,
                    cores_per_node=32,
                    mem_per_node=512,  # ~2-4 GB per core
                    exclusive=False,
                    walltime=walltimes["sharded_reproject"],
                    scheduler_options="#SBATCH --export=ALL",  # Add other options as needed
                    # Command to run before starting worker - i.e. conda activate <special_env>
                    worker_init="",
                ),
            ),
            HighThroughputExecutor(
                label="gpu",
                max_workers=1,
                provider=SlurmProvider(
                    partition="ampere", # or ada
                    account=account_name,
                    min_blocks=0,
                    max_blocks=2,
                    init_blocks=0,
                    parallelism=1,
                    nodes_per_block=1,
                    cores_per_node=2,  # perhaps should be 8???
                    mem_per_node=512,  # In GB
                    exclusive=False,
                    walltime=walltimes["gpu_max"],
                    # Command to run before starting worker - i.e. conda activate <special_env>
                    worker_init="",
                    scheduler_options="#SBATCH --gpus=1\n#SBATCH --export=ALL",
                ),
            ),
            HighThroughputExecutor(
                label="large_gpu",
                max_workers=1,
                provider=SlurmProvider(
                    partition="ampere",  # or ada; was turing, but we do not have access
                    account=account_name,
                    min_blocks=0,
                    max_blocks=2,
                    init_blocks=0,
                    parallelism=1,
                    nodes_per_block=1,
                    cores_per_node=2,  # perhaps should be 8???
                    mem_per_node=512,  # In GB
                    exclusive=False,
                    walltime=walltimes["gpu_max"],
                    # Command to run before starting worker - i.e. conda activate <special_env>
                    worker_init="",
                    scheduler_options="#SBATCH --gpus=1\n#SBATCH --export=ALL",
                ),
            ),
            HighThroughputExecutor(
                label="local_thread",
                provider=LocalProvider(
                    init_blocks=0,
                    max_blocks=1,
                ),
            ),
        ],
    )
