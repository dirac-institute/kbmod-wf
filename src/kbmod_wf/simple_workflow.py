import argparse
import os
import toml

import parsl
from parsl import File, python_app
import parsl.executors

from kbmod_wf.utilities.configuration_utilities import apply_runtime_updates, get_resource_config

from parsl import python_app


@python_app(executors=["local_dev_testing"])
def step_1(inputs=[], outputs=[]):
    import time

    time.sleep(int(inputs[1]))
    print(f"Step 1: key: {inputs[0]}, slept: {inputs[1]}")
    return outputs[0]


@python_app(executors=["local_dev_testing"])
def step_2(inputs=[], outputs=[]):
    import time

    time.sleep(1)
    print(f"Step 2: key: {inputs[0].filepath}")
    return outputs[0]


def workflow_runner(env=None, runtime_config={}):
    """This function will load and configure Parsl, and run the workflow.

    Parameters
    ----------
    env : str, optional
        Environment string used to define which resource configuration to use,
        by default None
    runtime_config : dict, optional
        Dictionary of assorted runtime configuration parameters, by default {}
    """
    resource_config = get_resource_config(env=env)
    resource_config = apply_runtime_updates(resource_config, runtime_config)

    dfk = parsl.load(resource_config)
    if dfk:
        print("Starting workflow")

        things = {
            "cats": 15,
            "dogs": 14,
            "ants": 4,
            "bees": 3,
            "elephants": 2,
            "frogs": 1,
        }

        step_1_futures = []
        for k, v in things.items():
            step_1_futures.append(
                step_1(
                    inputs=[k, v],
                    outputs=[File(k + ".txt")],
                )
            )

        step_2_futures = []
        for f in step_1_futures:
            step_2_futures.append(
                step_2(
                    inputs=[f],
                    outputs=[File("empty.txt")],
                )
            )

        [f.result() for f in step_2_futures]

        print("Workflow complete")

    parsl.clear()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--env",
        type=str,
        choices=["dev", "klone"],
        help="The environment to run the workflow in.",
    )

    parser.add_argument(
        "--runtime-config",
        type=str,
        help="The complete runtime configuration filepath to use for the workflow.",
    )

    args = parser.parse_args()

    # if a runtime_config file was provided and exists, load the toml as a dict.
    runtime_config = {}
    if args.runtime_config is not None and os.path.exists(args.runtime_config):
        with open(args.runtime_config, "r") as toml_runtime_config:
            runtime_config = toml.load(toml_runtime_config)

    workflow_runner(env=args.env, runtime_config=runtime_config)
