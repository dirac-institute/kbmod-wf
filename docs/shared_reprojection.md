# Shared WorkUnit reprojection

`reproject_workunit_in_frame` centralizes the existing Rubin WorkUnit EBD
transformation and parallel image reprojection. `guess_dist=None` selects the
original sky; a finite distance greater than 1.02 AU selects the barycentric frame.
The production caller continues to supply its ImageCollection patch WCS and
configured observatory. A caller without a patch WCS uses the middle exposure.

The helper requires homogeneous input WCS shapes for the existing EBD fitter.
`npoints` and `seed` make the fit reproducible when requested. The output keeps
the KBMOD sharded WorkUnit format. Preserving a non-default observing site through
reprojection and shard reload requires the companion KBMOD observatory fix.

Package imports are lazy so a native-FITS CPU preparation workflow does not
initialize Rubin resource discovery or require the GPU/Butler stack at import.
The included test checks the production caller's patch, site, and helper arguments;
it does not establish a full Rubin/Butler integration run.
