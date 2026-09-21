# DEEP validation through the Rubin reprojection path

This bounded workflow tests existing DEEP difference images. It does not insert
new sources. Native FITS pixels, persisted PSFs, PhotoCalib, per-image WCS, CTIO
location, and FITS UTC midpoints enter a normal KBMOD WorkUnit.

The production multi-night task and this validation task both call
`reproject_workunit_in_frame` in
`kbmod_wf.task_impls.reproject_multi_chip_multi_night_wu`. That shared helper calls
KBMOD `transform_wcses_to_ebd` (100 random fit points plus corners) and
`reproject_work_unit` with parallel reprojection. The corrected WorkUnit uses the
normal sharded FITS writer and reader before the GPU search. Distances are
barycentric AU, consistent with the actual KBMOD API; some older workflow comments
say heliocentric. The production task continues using its supplied ImageCollection
patch WCS. A small cutout without a patch WCS uses its middle exposure's WCS in the
selected frame. None selects original-sky reprojection; 42 selects EBD correction.

The DEEP adapter is intentionally separate from Rubin Butler ingestion. It avoids
DEEP collection timestamp and mask-plane metadata problems. A successful run tests
the shared reflex/reprojection/serialization path and actual GPU trajectory
scoring, but not Rubin Butler ingestion, wide-patch mosaicking, or multi-night
linearization. The targeted search is also separate from the general Rubin search
grid and production post-processing. Those remain additional validation steps.

## Science controls

- Retain FAKE, INJECTED, and INJECTED_TEMPLATE pixels. Keep ordinary bad-pixel
  rejection and the same 10-pixel mask growth as the previous DEEP experiment.
  Any ordinary bad flag on an injected pixel still rejects it.
- Match original-sky and 42 AU runs on the same cutout checksum, native PSFs,
  exposure times, search radius, 25 velocity offsets, and sigma-G settings.
- Transform truth into the same EBD frame, measure the WCS fit residual against
  geometric coordinate correction, and fail if it exceeds 0.05 arcsec.
- Join synthetic truth through EXPNUM and use actual image epochs. Do not infer
  a time scale from the catalogue's `mjd_mid` label.
- Preserve candidate pools before post-filtering, best contribution curves,
  normal sharded WorkUnits, and complete per-result provenance.
- Match known and synthetic names separately at 1, 5, and 10 arcsec with a one-
  second epoch tolerance. Report minimum matched observations, retained-observation
  purity, and catalogue observation completeness separately.
- The sample is targeted; its recovery fraction is not survey completeness.
  Truth catalogue membership is not itself evidence of successful pixel injection.

## Running

```
python -m kbmod_wf.deep_workflow --manifest manifest.json --settings site.json --stage prepare
python -m kbmod_wf.deep_workflow --manifest manifest.json --settings site.json --stage search
```

Preparation must run where native FITS paths are readable. Stage `cutouts/*.npz`
and companion JSON files unchanged to the search host's `output_root/cutouts`.
The search stage submits a DAG of shared CPU reprojection tasks and GPU search
consumers using Parsl DataFutures. `--stage all` runs the complete graph on a host
with both input access and a GPU. All tasks are submitted before waiting.

The default search configuration uses separate local HighThroughputExecutors for
CPU and GPU work; only the prepare-only adapter uses a thread pool. GPU affinity
comes from Parsl `available_accelerators`, never a reinterpretation of worker rank.
Set `gpu_ids` to accelerators available to the allocation. Use one worker per GPU;
size CPU and reprojection-worker counts to the allocation and memory budget.

Use `--site-config path.py` to provide `build_config(settings, stage)` returning a
Parsl Config with `deep_cpu` and, when searching, `deep_gpu` executor labels. A
SlurmProvider can replace LocalProvider without changing science functions or the
manifest. Paths, environments, queues, and accounts are site settings. Such a
migration still needs a real smoke test on that site. No site is claimed validated
merely because a configuration file can be instantiated.

Reruns intentionally recompute rather than trusting a same-named stale result.
Use a distinct output_root and run_dir for each experiment. Parsl task caching is
disabled. Preparation writes cutouts and JSON atomically and records checksums;
search verifies the cutout through reprojection and the WorkUnit primary header
before loading shards. Transfer checksums for all shards when moving WorkUnits.
