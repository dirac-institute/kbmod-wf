# Resource notes
# Milano - 480 Gb max RAM - Rubin main nodes for processing
# Roma - 480 Gb max RAM
# Ampere - A100 - 40 Gb GPU - ~896 Gb max RAM
# Ada - L40S - 46 Gb GPU - ~512 Gb max RAM

#
#
# trying 10X10s so lowering memory by 50% to 240 on reproejct phase

import os
import datetime
from parsl import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider, SlurmProvider
from parsl.utils import get_all_checkpoints
from parsl.monitoring.monitoring import MonitoringHub # COC
from parsl.addresses import address_by_hostname # COC
import logging # COC
import numpy as np
import platform

nodename_map = {"sdfada":"ada", "sdfampere":"ampere", "sdfroma":"roma", "sdfmilano":"milano"}

slurm_cmd_timeout = 60 # default is 10 and that is timing out for sacct -X 5/1/2025 COC

max_ram_dict = {"ada":350, # 351 Gb total, with 5 GPUs total on the one node, leaves 70 Gb per task
		"ampere":952, # 896 per each of the two nodes we can access, each with 4 GPUs
		"roma":140, # 240 to 140; to 240 5/20/2025 COC; RAM fixes, back to 140 5/23/2025 COC
		"milano":140 # 240 to 140
}
max_block_dict = {"ada":1, "ampere":2}
gpus_per_node_dict = {"ada":5, "ampere":4}
max_nodes_dict = {"ada":1, "ampere":2}
cpus_per_node_dict = {"ada":30, "ampere":112} # {"ada":6, "ampere":28} # ada cap is 36, ampere ≥100
cores_per_worker_dict = {"ada":6, "ampere":28}
monitor_port_dict = {"ada":55056, "ampere":55066}


gpu_partition = "ampere"
cpus_for_gpus_dict = {"ampere":"roma", "ada":"milano"}
cpu_partition = cpus_for_gpus_dict[gpu_partition]


if "GPUNODE" in os.environ:
    gpu_partition = os.environ["GPUNODE"].lower()
    print(f"Set gpu_partition to {gpu_partition} via environment variable GPUNODE.")


walltimes = {
    "sharded_reproject": "04:00:00",
    "gpu": "12:00:00",
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
                label="sharded_reproject",
                # max_workers=1,
                max_workers_per_node=1,
                provider=SlurmProvider(
                    partition=cpu_partition, # or ada?; see resource notes at top
                    account=account_name,
                    min_blocks=0,
                    max_blocks=10, # 12 to 20 4/16/2025 COC; 20 to 10 while debugging RAM issues 5/20/2025 COC
                    init_blocks=0,
                    parallelism=1,
                    nodes_per_block=1,
                    cores_per_node=32,
                    mem_per_node=max_ram_dict[cpu_partition],  # ~2-4 GB per core
                    exclusive=False,
                    walltime=walltimes["sharded_reproject"],
                    scheduler_options="#SBATCH --export=ALL",  # Add other options as needed
                    # Command to run before starting worker - i.e. conda activate <special_env>
                    worker_init="env | grep SLURM",
                    cmd_timeout=slurm_cmd_timeout,
                ),
            ),
            HighThroughputExecutor(
                label="gpu",
                max_workers_per_node=gpus_per_node_dict[gpu_partition], # was 1
                cores_per_worker=cores_per_worker_dict[gpu_partition],
                mem_per_worker=int(np.floor(max_ram_dict[gpu_partition]/gpus_per_node_dict[gpu_partition])),
                provider=SlurmProvider(
                    partition=gpu_partition, # or ada
                    account=account_name,
                    min_blocks=max_nodes_dict[gpu_partition], # was 0
                    init_blocks=max_nodes_dict[gpu_partition], # added 4/29/2025 COC
                    max_blocks=max_block_dict[gpu_partition], # 8 to 24 4/16/2025 COC to 12 4/20/2025 COC to 8 4/22/2025 COC
                    parallelism=1,
                    nodes_per_block=1,
                    cores_per_node=cpus_per_node_dict[gpu_partition],
                    mem_per_node=max_ram_dict[gpu_partition],  # In GB; 512 OOMs with 20X20s 4/16/2025 COC
 #                   exclusive=False, # disabled
                    walltime=walltimes["gpu"],
                    cmd_timeout=slurm_cmd_timeout,
                    worker_init="""
export CUDA_VISIBLE_DEVICES=$((${PARSL_WORKER_RANK:-0} % 4))
echo Assigned GPU $CUDA_VISIBLE_DEVICES to worker $PARSL_WORKER_RANK
hostname
hostnamectl
nvidia-smi
env
""",
                    scheduler_options=f"""
#SBATCH --gres=gpu:{gpus_per_node_dict[gpu_partition]}
#SBATCH --exclusive
""",
#"#SBATCH --gpus=1\n#SBATCH --export=ALL",
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
	monitoring=MonitoringHub(
       		hub_address=address_by_hostname(),
       		hub_port=monitor_port_dict[gpu_partition],
       		monitoring_debug=True,
       		resource_monitoring_interval=10,
   	),
    )
    
