"""Portable, bounded DEEP track validation, separate from multi-night production.

Run preparation where original FITS files are available, then search the staged
cutouts on a GPU host. One manifest and science configuration serve both stages.
A site module may supply build_config(settings, stage); executor labels are 'deep_cpu' and 'deep_gpu'.
"""

import argparse
import importlib.util
import json
from pathlib import Path


def prepare_app(spec, settings, inputs=(), outputs=()):
    from kbmod_wf.task_impls.deep_validation import prepare

    return prepare(spec, settings, outputs[0].filepath)


def reproject_app(distance, settings, inputs=(), outputs=()):
    from kbmod_wf.task_impls.deep_validation import reproject

    return reproject(inputs[0].filepath, distance, settings, outputs[0].filepath)


def search_app(spec, distance, settings, inputs=(), outputs=()):
    from kbmod_wf.task_impls.deep_validation import search

    return search(spec, distance, settings, inputs[0].filepath, outputs[0].filepath)


def build_config(settings, stage):
    from parsl import Config
    from parsl.executors import HighThroughputExecutor, ThreadPoolExecutor
    from parsl.providers import LocalProvider

    if stage == "prepare":
        executors = [ThreadPoolExecutor(label="deep_cpu", max_threads=settings.get("cpu_workers", 4))]
    else:
        executors = [
            HighThroughputExecutor(
                label="deep_cpu",
                max_workers_per_node=settings.get("cpu_workers", 4),
                cores_per_worker=settings.get("reproject_workers", 2),
                provider=LocalProvider(init_blocks=1, max_blocks=1),
            )
        ]
        executors.append(
            HighThroughputExecutor(
                label="deep_gpu",
                max_workers_per_node=1,
                cores_per_worker=1,
                available_accelerators=settings.get("gpu_ids", ["0"]),
                provider=LocalProvider(init_blocks=1, max_blocks=1),
            )
        )
    return Config(executors=executors, run_dir=settings["run_dir"], retries=0)


def run(manifest, settings, stage, site_config=None):
    import parsl
    from parsl import File, python_app

    specs = json.loads(Path(manifest).read_text())
    ids = [s["id"] for s in specs]
    if len(ids) != len(set(ids)) or any(Path(i).name != i for i in ids):
        raise ValueError("Manifest IDs must be unique basenames")
    distances = settings.get("distances_au", [None, 42.0])
    if any(d is not None and (not isinstance(d, (int, float)) or not 1.02 < d < 10000) for d in distances):
        raise ValueError("Distances must be null (original sky) or finite AU >1.02")
    if len(distances) != len(set(distances)):
        raise ValueError("Distances must be unique")
    root = Path(settings["output_root"]).resolve()
    for p in [root / "cutouts", root / "results", root / "workunits"]:
        p.mkdir(parents=True, exist_ok=True)
    config_factory = build_config
    if site_config:
        module_spec = importlib.util.spec_from_file_location("deep_site", site_config)
        module = importlib.util.module_from_spec(module_spec)
        module_spec.loader.exec_module(module)
        config_factory = module.build_config
    config = config_factory(settings, stage)
    prep = python_app(prepare_app, executors=["deep_cpu"], cache=False)
    project = python_app(reproject_app, executors=["deep_cpu"], cache=False)
    find = python_app(search_app, executors=["deep_gpu"], cache=False)
    with parsl.load(config):
        futures = []
        for spec in specs:
            cut = File(str(root / "cutouts" / (spec["id"] + ".npz")))
            if stage in ("prepare", "all"):
                prepared = prep(spec, settings, outputs=[cut])
                source = prepared.outputs[0]
                futures.append(prepared)
            else:
                source = cut
            if stage in ("search", "all"):
                for distance in distances:
                    tag = "original" if distance is None else f"ebd{distance:g}"
                    result = File(str(root / "results" / f'{spec["id"]}_{tag}.json'))
                    workunit = File(str(root / "workunits" / f'{spec["id"]}_{tag}' / "stack.fits"))
                    projected = project(distance, settings, inputs=[source], outputs=[workunit])
                    futures.append(
                        find(
                            spec,
                            distance,
                            settings,
                            inputs=[projected.outputs[0]],
                            outputs=[result],
                        )
                    )
        # Submit the complete graph first; failures propagate and prevent success claims.
        for future in futures:
            future.result()
    parsl.clear()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--manifest", required=True)
    p.add_argument("--settings", required=True)
    p.add_argument("--stage", choices=["prepare", "search", "all"], required=True)
    p.add_argument("--site-config")
    a = p.parse_args()
    run(a.manifest, json.loads(Path(a.settings).read_text()), a.stage, a.site_config)


if __name__ == "__main__":
    main()
