import os
import datetime
from parsl import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider, SlurmProvider
from parsl.utils import get_all_checkpoints

walltimes = {
    "compute_bigmem": "01:00:00",
    "large_mem": "04:00:00",
    "gpu_max": "08:00:00",
}


def klone_resource_config():
    return Config(
        app_cache=True,
        checkpoint_mode="task_exit",
        checkpoint_files=get_all_checkpoints(
            os.path.join("/gscratch/dirac/kbmod/workflow/run_logs", datetime.date.today().isoformat())
        ),
        run_dir=os.path.join("/gscratch/dirac/kbmod/workflow/run_logs", datetime.date.today().isoformat()),
        retries=1,
        executors=[
            HighThroughputExecutor(
                label="small_cpu",
                provider=SlurmProvider(
                    partition="compute-bigmem",
                    account="astro",
                    min_blocks=0,
                    max_blocks=4,
                    init_blocks=0,
                    parallelism=1,
                    nodes_per_block=1,
                    cores_per_node=1,  # perhaps should be 8???
                    mem_per_node=256,  # In GB
                    exclusive=False,
                    walltime=walltimes["compute_bigmem"],
                    # Command to run before starting worker - i.e. conda activate <special_env>
                    worker_init="",
                ),
            ),
            HighThroughputExecutor(
                label="large_mem",
                provider=SlurmProvider(
                    partition="ckpt-g2",
                    account="astro",
                    min_blocks=0,
                    max_blocks=2,
                    init_blocks=0,
                    parallelism=1,
                    nodes_per_block=1,
                    cores_per_node=8,
                    mem_per_node=512,
                    exclusive=False,
                    walltime=walltimes["large_mem"],
                    # Command to run before starting worker - i.e. conda activate <special_env>
                    worker_init="",
                ),
            ),
            HighThroughputExecutor(
                label="gpu",
                provider=SlurmProvider(
                    partition="ckpt-g2",
                    account="escience",
                    min_blocks=0,
                    max_blocks=2,
                    init_blocks=0,
                    parallelism=1,
                    nodes_per_block=1,
                    cores_per_node=4,  # perhaps should be 8???
                    mem_per_node=512,  # In GB
                    exclusive=False,
                    walltime=walltimes["gpu_max"],
                    # Command to run before starting worker - i.e. conda activate <special_env>
                    worker_init="",
                    scheduler_options="#SBATCH --gpus=1",
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
