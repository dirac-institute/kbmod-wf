from parsl import Config
from parsl.executors import ThreadPoolExecutor


def dev_config():
    return Config(
        # run_dir='runinfo', # do some introspection here so that we can place the runinfo directory somewhere above src.
        initialize_logging=False,
        executors=[
            ThreadPoolExecutor(
                label="local_dev_testing",
            )
        ],
    )
