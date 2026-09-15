# Parallel injection

Requires the matching KBMOD `feat/parallel-injection` branch. Enable per-injection
parallelism separately from worker counts used by other workflow stages:

```toml
[apps.ic_to_wu.injection]
injection_workers = 2
max_images_per_shard = 8
```

The default `injection_workers = 1` keeps the existing serial workflow and does
not import the new KBMOD API. Existing background and variance options continue
to apply. Both generated and precomputed catalogs use the same parallel path;
generation happens once for the entire input collection.

With more than one worker, `ICtoWUConverter` requires `save=True` and a new output
path. Workers create independent read-only Butlers, inject complete MJD groups,
and write image files. The parent combines metadata and saves the rendered catalog.
It does not build a full in-memory output ImageCollection or WorkUnit.

Outputs are in stable MJD order. WorkUnit metadata `injection_input_row` records
original row indices. Equal-time detector images remain distinct. WorkUnit image
files use lossless compression; the parallel path refuses to overwrite existing
primary/numbered files. Use fresh output destinations for retries.

`max_images_per_shard` limits the number of exposures retained by each injection
call, not bytes. Whole MJD groups larger than the limit fail before injection.
Start with two workers and small shards. Account for exposure dimensions, temporary
copies, WorkUnit conversion, runtime caches and concurrent workflow tasks. There is
no automatic RAM budget in this first version. Set native numerical-library thread
limits before starting the workflow.

The existing `ic_to_injected_ic()` helper remains a serial in-memory API; pass the
parallel setting through `ICtoWUConverter`/`ic_to_wu(..., save=True)` instead.

Before a workflow-scale run, use the KBMOD branch's
`benchmarks/smoke_parallel_injection.py` and `docs/parallel_injection.md` instructions.
The smoke script compares the existing serial output with one-worker and two-worker
sharding and reports memory/timing information. Then run a single saved workflow
conversion with the two settings above. Confirm the returned primary WorkUnit and
both provenance/rendered catalog files exist and can be read.

Local tests cover option validation, generated/precomputed routing and unchanged
default calls. Real Rubin/Butler execution and memory sizing require USDF testing.
