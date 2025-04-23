# Resource notes
# Milano - 480 Gb max RAM - Rubin main nodes for processing
# Roma - 480 Gb max RAM
# Ampere - A100 - 40 Gb GPU - ~896 Gb max RAM
# Ada - L40S - 46 Gb GPU - ~512 Gb max RAM

import os
import datetime
from parsl import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider, SlurmProvider
from parsl.utils import get_all_checkpoints
import platform

nodename_map = {"sdfada":"ada", "sdfampere":"ampere", "sdfroma":"roma", "sdfmilano":"milano"}
max_ram_dict = {"ada":350, "ampere":896, "roma":480, "milano":480}

gpu_partition = "ampere"
cpu_partition = "milano"

if "GPUNODE" in os.environ:
	gpu_partition = os.environ["GPUNODE"].lower()
	print(f"Set gpu_partition to {gpu_partition} via environment variable GPUNODE.")


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
            os.path.join(base_path, "kbmod/workflow/run_logs")
        ),
        run_dir=os.path.join(base_path, "kbmod/workflow/run_logs"),
        retries=1,
        executors=[
            HighThroughputExecutor(
                label="small_cpu",
                max_workers=1,
                provider=SlurmProvider(
                    partition=cpu_partition,
                    account=account_name,
                    min_blocks=0,
                    max_blocks=64,
                    init_blocks=0,
                    parallelism=1,
                    nodes_per_block=1,
                    cores_per_node=1,  # perhaps should be 8???
                    mem_per_node=256,  # In GB; milano, roma have a ~480 Gb cap 4/16/2025 COC
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
                    partition="roma", # or ada?; note: milano, roma have a ~480 Gb cap 4/16/2025 COC
                    account=account_name,
                    min_blocks=0,
                    max_blocks=20, # 12 to 20 4/16/2025 COC
                    init_blocks=0,
                    parallelism=1,
                    nodes_per_block=1,
                    cores_per_node=32,
                    mem_per_node=max_ram_dict["roma"],
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
                    partition=cpu_partition, # or ada?; see resource notes at top
                    account=account_name,
                    min_blocks=0,
                    max_blocks=20, # 12 to 20 4/16/2025 COC
                    init_blocks=0,
                    parallelism=1,
                    nodes_per_block=1,
                    cores_per_node=32,
                    mem_per_node=max_ram_dict[cpu_partition],  # ~2-4 GB per core
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
                    partition=gpu_partition, # or ada
                    account=account_name,
                    min_blocks=0,
                    max_blocks=8, # 8 to 24 4/16/2025 COC to 12 4/20/2025 COC to 8 4/22/2025 COC
                    init_blocks=0,
                    parallelism=1,
                    nodes_per_block=1,
                    cores_per_node=2,  # perhaps should be 8???
                    mem_per_node=max_ram_dict[gpu_partition],  # In GB; 512 OOMs with 20X20s 4/16/2025 COC
                    exclusive=False,
                    walltime=walltimes["gpu_max"],
                    # Command to run before starting worker - i.e. conda activate <special_env>
                    worker_init="hostname;hostnamectl;nvidia-smi",
                    scheduler_options="#SBATCH --gpus=1\n#SBATCH --export=ALL",
                ),
            ),
            HighThroughputExecutor(
                label="large_gpu",
                max_workers=1,
                provider=SlurmProvider(
                    partition=gpu_partition,  # or ada; was turing, but we do not have access
                    account=account_name,
                    min_blocks=0,
                    max_blocks=8, # 8 to 24 4/16/2025 COC to 12 4/20/2025 to 8 4/25/2025 COC
                    init_blocks=0,
                    parallelism=1,
                    nodes_per_block=1,
                    cores_per_node=2,  # perhaps should be 8???
                    mem_per_node=max_ram_dict[gpu_partition],  # In GB; 512G OOM with 20X20 4/16/2025 COC
                    exclusive=False,
                    walltime=walltimes["gpu_max"],
                    # Command to run before starting worker - i.e. conda activate <special_env>
                    worker_init="hostname;hostnamectl;nvidia-smi",
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
