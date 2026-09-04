#!/usr/bin/env python
"""Run only the multi-night WorkUnit reprojection stage.

This intentionally does not import or submit the KBMOD GPU search app. Searches
for these WorkUnits are performed on Tillicum after verified synchronization.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import parsl
import toml
from parsl import File, python_app

from kbmod_wf.utilities import (
    apply_runtime_updates,
    get_configured_logger,
    get_executors,
    get_resource_config,
)
from kbmod_wf.workflow_tasks.create_manifest import create_manifest


@python_app(
    cache=True,
    executors=get_executors(["local_dev_testing", "sharded_reproject"]),
    ignore_for_cache=["logging_file"],
)
def reproject_wu(inputs=(), outputs=(), runtime_config=None, logging_file=None):
    from kbmod_wf.task_impls.reproject_multi_chip_multi_night_wu import reproject_wu
    from kbmod_wf.utilities.logger_utilities import ErrorLogger, get_configured_logger

    runtime_config = runtime_config or {}
    logger = get_configured_logger("task.reproject_wu", logging_file.filepath)
    guess_dist = inputs[1]
    logger.info(f"Starting reprojection-only task for guess distance {guess_dist}")
    with ErrorLogger(logger):
        reproject_wu(
            guess_dist,
            ic_filepath=inputs[0],
            reprojected_wu_filepath=outputs[0].filepath,
            runtime_config=runtime_config,
            logger=logger,
        )
    logger.info("Completed reprojection-only task")
    return outputs[0]


def workflow_runner(*, env: str, runtime_config: dict) -> list[Path]:
    resource_config = get_resource_config(env=env)
    resource_config = apply_runtime_updates(resource_config, runtime_config)

    # Guarantee that this workflow cannot provision a USDF GPU block. Parsl's
    # Config exposes executors read-only, so filter its validated backing tuple
    # before loading the DFK.
    resource_config._executors = tuple(
        executor for executor in resource_config.executors if executor.label != "gpu"
    )
    resource_config._validate_executors()
    executor_labels = {executor.label for executor in resource_config.executors}
    if "gpu" in executor_labels:
        raise RuntimeError("GPU executor was not removed from reprojection-only workflow")
    if "sharded_reproject" not in executor_labels:
        raise RuntimeError("Missing sharded_reproject executor")

    app_configs = runtime_config.get("apps", {})
    create_manifest_config = app_configs.get("create_manifest", {})
    reproject_config = app_configs.get("reproject_wu", {})
    distances = reproject_config.get("helio_guess_dists")
    if not distances:
        raise ValueError("No helio_guess_dists were provided for reprojection")

    output_directory = Path(
        create_manifest_config.get("output_directory", os.getcwd())
    ).resolve()
    # The stock USDF configuration uses one shared run-log base and chooses its
    # next numeric child with a non-atomic directory scan. Concurrent wave
    # parents can therefore select the same child. Give every wave its own base
    # so array concurrency cannot race during parsl.load().
    resource_config.run_dir = str(output_directory / "parsl_run_logs")

    outputs: list[Path] = []
    dfk = parsl.load(resource_config)
    try:
        logging_file = File(os.path.join(dfk.run_dir, "kbmod.log"))
        logger = get_configured_logger("workflow.reproject_only", logging_file.filepath)
        logger.info("Starting reprojection-only workflow; GPU executor is disabled")
        logger.info(f"Enabled executors: {sorted(executor_labels)}")
        logger.info(f"Runtime configuration:\n{toml.dumps(runtime_config)}")

        manifest_file = File(str(output_directory / "manifest.txt"))
        manifest_future = create_manifest(
            inputs=[],
            outputs=[manifest_file],
            runtime_config=create_manifest_config,
            logging_file=logging_file,
        )

        futures = []
        with Path(manifest_future.result().filepath).open() as handle:
            for line in handle:
                collection_path = line.strip()
                if not collection_path:
                    continue
                for distance in distances:
                    output_path = Path(f"{collection_path}.wu.{distance}.repro")
                    outputs.append(output_path)
                    futures.append(
                        reproject_wu(
                            inputs=[File(collection_path), distance],
                            outputs=[File(str(output_path))],
                            runtime_config=reproject_config,
                            logging_file=logging_file,
                        )
                    )

        failures = []
        for future in futures:
            try:
                future.result()
            except Exception as exc:
                failures.append(exc)
                logger.exception("Reprojection future failed", exc_info=exc)
        if failures:
            raise RuntimeError(f"{len(failures)} reprojection future(s) failed")

        missing = [
            path
            for path in outputs
            if not path.is_file() or not path.with_name(f"0_{path.name}").is_file()
        ]
        if missing:
            raise RuntimeError(f"{len(missing)} reprojected WorkUnit head(s) are incomplete")
        logger.info(f"Reprojection-only workflow complete: {len(outputs)} WorkUnits")
        return outputs
    finally:
        parsl.clear()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", choices=["dev", "klone", "usdf"], required=True)
    parser.add_argument("--runtime-config", type=Path, required=True)
    args = parser.parse_args()

    if not args.runtime_config.is_file():
        parser.error(f"Missing runtime configuration: {args.runtime_config}")
    with args.runtime_config.open() as handle:
        runtime_config = toml.load(handle)
    outputs = workflow_runner(env=args.env, runtime_config=runtime_config)
    print(f"Materialized {len(outputs)} reprojected WorkUnits without GPU search.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
