"""DEEP native-FITS preparation and matched original/EBD targeted GPU tests."""

import hashlib
import json
import os
from pathlib import Path
import socket
import time

import numpy as np

BAD_FLAGS = [
    "BAD",
    "SAT",
    "CR",
    "EDGE",
    "NO_DATA",
    "SUSPECT",
    "CLIPPED",
    "CROSSTALK",
    "SENSOR_EDGE",
    "UNMASKEDNAN",
]
INJECTION_FLAGS = ["FAKE", "INJECTED", "INJECTED_TEMPLATE"]


def digest(path):
    with open(path, "rb") as f:
        h = hashlib.sha256()
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
        return h.hexdigest()


def atomic_json(path, value):
    path = Path(path)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(value, indent=2, allow_nan=False))
    os.replace(tmp, path)


def cut(hdu, x, y, size, fill, dtype):
    nx, ny = hdu.header["NAXIS1"], hdu.header["NAXIS2"]
    out = np.full((size, size), fill, dtype=dtype)
    left, bottom, right, top = (
        max(x, 0),
        max(y, 0),
        min(nx, x + size),
        min(ny, y + size),
    )
    if left < right and bottom < top:
        out[bottom - y : top - y, left - x : right - x] = hdu.section[bottom:top, left:right]
    return out


def prepare(spec, settings, output):
    from astropy.io import fits
    from astropy.time import Time
    from astropy.wcs import WCS, Sip
    from scipy.ndimage import maximum_filter

    flags = settings.get("mask_flags", BAD_FLAGS)
    if not settings.get("mask_injections", False) and set(flags) & set(INJECTION_FLAGS):
        raise ValueError("Injection flags conflict with mask_injections=false")
    if settings.get("mask_injections", False):
        flags = list(set(flags) | set(INJECTION_FLAGS))
    size = spec["size"]
    arrays = {k: [] for k in ["science", "variance", "mask", "psfs", "times"]}
    frames = []
    for old in spec["frames"]:
        path = old["path"]
        with fits.open(path, memmap=True) as hd:
            h = hd[0].header
            if int(h["EXPNUM"]) != old["visit"] or int(h["CCDNUM"]) != spec["detector"]:
                raise ValueError("Source image identity mismatch")
            t = float(Time(h["DATE-AVG"], format="isot", scale=h["TIMESYS"].lower()).utc.mjd)
            if abs(t - old["mjd_utc"]) * 86400 > 0.001:
                raise ValueError("Image time differs from manifest")
            w = WCS(hd["IMAGE"].header)
            if "center_radec" in spec:
                xy = w.all_world2pix([spec["center_radec"]], 0)[0]
                x, y = np.floor(xy - size / 2).astype(int)
            else:
                x, y = old["x0"], old["y0"]
            sc = cut(hd["IMAGE"], x, y, size, np.nan, np.float32)
            va = cut(hd["VARIANCE"], x, y, size, np.nan, np.float32)
            plane = {k[3:]: int(hd["MASK"].header[k]) for k in hd["MASK"].header if k.startswith("MP_")}
            raw = cut(hd["MASK"], x, y, size, 1 << plane["NO_DATA"], np.int32)
            bits = sum(1 << plane[k] for k in flags if k in plane)
            injection_bits = sum(1 << plane[k] for k in INJECTION_FLAGS if k in plane)
            growth = int(settings.get("mask_growth", 10))
            if growth < 1:
                raise ValueError("mask_growth must be >=1")
            mask = maximum_filter((raw & bits) != 0, size=growth, mode="constant", cval=1)
            mask |= ~np.isfinite(sc) | ~np.isfinite(va) | (va <= 0)
            calibration = float(hd["PhotoCalib"].data["calibrationMean"][0])
            sc *= calibration
            va *= calibration**2
            kp = hd["FixedKernel"].data[0]
            kernel = np.array(kp["image"], dtype=np.float32).reshape(
                int(kp["dimensions_y"]), int(kp["dimensions_x"])
            )
            kernel /= kernel.sum()
            cw = w.deepcopy()
            cw.wcs.crpix -= [x, y]
            if w.sip is not None:
                cw.sip = Sip(w.sip.a, w.sip.b, w.sip.ap, w.sip.bp, w.sip.crpix - [x, y])
            cw.array_shape = (size, size)
            for key, value in dict(science=sc, variance=va, mask=mask, psfs=kernel, times=t).items():
                arrays[key].append(value)
            frames.append(
                dict(
                    old,
                    x0=int(x),
                    y0=int(y),
                    wcs=dict(cw.to_header(relax=True)),
                    masked_fraction=float(mask.mean()),
                    injection_flagged_pixels=int(np.count_nonzero(raw & injection_bits)),
                    injection_flagged_pixels_retained=int(
                        np.count_nonzero(((raw & injection_bits) != 0) & ~mask)
                    ),
                    source_bytes=Path(path).stat().st_size,
                    source_mtime_ns=Path(path).stat().st_mtime_ns,
                    mask_bits=plane,
                )
            )
    if np.any(np.diff(arrays["times"]) <= 0):
        raise ValueError("Manifest must contain strictly increasing exposure times")
    temp = str(output) + ".tmp"
    with open(temp, "wb") as f:
        np.savez_compressed(f, **{k: np.asarray(v) for k, v in arrays.items()})
    os.replace(temp, output)
    meta = dict(
        spec,
        frames=frames,
        mask_flags=flags,
        mask_growth=growth,
        cutout_sha256=digest(output),
        prepared_host=socket.gethostname(),
    )
    atomic_json(Path(output).with_suffix(".json"), meta)
    return str(output)


def transform_catalog(catalog, distance, observer):
    from kbmod.reprojection_utils import correct_parallax_geometrically_vectorized

    out = catalog.copy()
    if distance is not None and len(out):
        sky, _ = correct_parallax_geometrically_vectorized(
            out["RA"], out["DEC"], out["mjd_mid"], distance, observer
        )
        out["RA"] = sky.ra.deg
        out["DEC"] = sky.dec.deg
    return out


def reproject(input_path, distance, settings, output):
    """Use the production Rubin helper, then persist its normal sharded WorkUnit."""
    from astropy.coordinates import EarthLocation
    from astropy.wcs import WCS
    from astropy.utils import iers
    from kbmod.core.image_stack_py import ImageStackPy
    from kbmod.configuration import SearchConfiguration
    from kbmod.work_unit import WorkUnit
    from kbmod_wf.task_impls.reproject_multi_chip_multi_night_wu import (
        reproject_workunit_in_frame,
    )

    iers.conf.auto_download = False
    started = time.monotonic()
    meta = json.loads(Path(input_path).with_suffix(".json").read_text())
    if digest(input_path) != meta["cutout_sha256"]:
        raise ValueError("Cutout checksum failed")
    with np.load(input_path) as a:
        masks = a["mask"].copy()
        stack = ImageStackPy(
            times=a["times"],
            sci=a["science"],
            var=a["variance"],
            mask=a["mask"],
            psfs=a["psfs"],
        )
    wcses = [WCS(f["wcs"]) for f in meta["frames"]]
    for w in wcses:
        w.array_shape = (meta["size"], meta["size"])
    target_unmasked = []
    for i, w in enumerate(wcses):
        xy = np.rint(w.all_world2pix([meta["track"][i][1:]], 0)[0]).astype(int)
        x, y = xy
        inside = 0 <= x < meta["size"] and 0 <= y < meta["size"]
        target_unmasked.append(bool(inside and not masks[i, y, x]))
    f = meta["frames"][0]
    observer = EarthLocation.from_geodetic(f["obs_lon"], f["obs_lat"], f["obs_elev"])
    wu = WorkUnit(stack, SearchConfiguration(), per_image_wcs=wcses, observatory=observer)
    wr = reproject_workunit_in_frame(
        wu,
        guess_dist=distance,
        n_workers=settings.get("reproject_workers", 2),
        npoints=settings.get("ebd_fit_points", 100),
        seed=settings.get("ebd_fit_seed", 20260919),
    )
    dest = Path(output)
    dest.parent.mkdir(parents=True, exist_ok=True)
    wr.to_sharded_fits(dest.name, str(dest.parent), overwrite=True)
    meta.update(
        reprojection_distance_au=distance,
        reprojection_seconds=time.monotonic() - started,
        reprojection_host=socket.gethostname(),
        reprojected_header_sha256=digest(dest),
        target_center_unmasked_obs=int(sum(target_unmasked)),
        target_center_unmasked_by_exposure=target_unmasked,
        shared_reprojection_function="kbmod_wf.task_impls.reproject_multi_chip_multi_night_wu.reproject_workunit_in_frame",
    )
    atomic_json(dest.with_suffix(".json"), meta)
    return output


def search(spec, distance, settings, input_path, output):
    # Worker accelerator assignment is inherited from Parsl; never replace it with worker rank.
    import kbmod
    from kbmod import search as k
    from kbmod.work_unit import WorkUnit
    from kbmod.filters.sigma_g_filter import SigmaGClipping
    from kbmod.filters.known_object_filters import KnownObjsMatcher
    from kbmod.results import Results
    from astropy.coordinates import EarthLocation
    from astropy.table import Table
    from astropy.wcs import WCS
    from astropy.utils import iers

    iers.conf.auto_download = False
    start = time.monotonic()
    if not k.kb_has_gpu():
        raise RuntimeError("A CUDA-enabled KBMOD build is required")
    meta = json.loads(Path(input_path).with_suffix(".json").read_text())
    if digest(input_path) != meta["reprojected_header_sha256"]:
        raise ValueError("WorkUnit header checksum failed")
    if meta["id"] != spec["id"] or set(meta["mask_flags"]) & set(INJECTION_FLAGS):
        raise ValueError("Unexpected input identity or injection masking")
    path = Path(input_path)
    wr = WorkUnit.from_sharded_fits(path.name, str(path.parent), lazy=False)
    times = np.asarray(wr.get_all_obstimes())
    n, size = len(times), meta["size"]
    frames = meta["frames"]
    wcses = [WCS(f["wcs"]) for f in frames]
    f = frames[0]
    observer = EarthLocation.from_geodetic(f["obs_lon"], f["obs_lat"], f["obs_elev"])
    common = wr.wcs
    frame = wr.reprojection_frame
    if wr.barycentric_distance != distance or frame != ("original" if distance is None else "ebd"):
        raise ValueError("Serialized WorkUnit frame/distance mismatch")
    ebd = wr.get_constituent_meta("ebd_wcs") if distance is not None else None
    if not np.allclose(
        [v.value for v in wr.observatory.to_geocentric()],
        [v.value for v in observer.to_geocentric()],
        atol=1e-6,
        rtol=0,
    ):
        raise ValueError("Observatory lost in reprojection")
    # Exposure IDs, not an assumed truth time scale, join injected rows to image epochs.
    visit_times = {f["visit"]: f["mjd_utc"] for f in frames}
    records = [dict(r, mjd_mid=visit_times[r["visit"]]) for r in spec["catalog"] if r["visit"] in visit_times]
    ponder_hash = None
    if settings.get("ponder_catalog"):
        import csv

        ponder_hash = digest(settings["ponder_catalog"])
        with open(settings["ponder_catalog"]) as f:
            for row in csv.DictReader(f):
                visit = int(row["observationId"])
                if visit in visit_times:
                    if abs(float(row["mjd_utc"]) - visit_times[visit]) * 86400 > 1.0:
                        raise ValueError("Ponder epoch differs from image epoch")
                    records.append(
                        dict(
                            Name=row["MPC"],
                            kind="ponder",
                            visit=visit,
                            RA=float(row["RA_deg"]),
                            DEC=float(row["Dec_deg"]),
                            mjd_mid=visit_times[visit],
                        )
                    )
    catalog = (
        Table(rows=records)
        if records
        else Table(
            names=["Name", "kind", "visit", "RA", "DEC", "mjd_mid"],
            dtype=["U64", "U16", int, float, float, float],
        )
    )
    target = Table(
        rows=[
            dict(visit=v, RA=ra, DEC=dec, mjd_mid=visit_times[v])
            for v, ra, dec in spec["track"]
            if v in visit_times
        ]
    )
    if len(target) != n or list(target["visit"]) != [f["visit"] for f in frames]:
        raise ValueError("Target truth must cover every input exposure in order")
    target = transform_catalog(target, distance, observer)
    xy = common.all_world2pix(np.c_[target["RA"], target["DEC"]], 0)
    dt = times - times[0]
    fit = np.polynomial.polynomial.polyfit(dt, xy, 1)
    origin, velocity = fit
    linear_error = float(np.linalg.norm(xy - origin - dt[:, None] * velocity, axis=1).max())
    # Verify fitted image EBD transforms against the exact geometric truth transform.
    transform_error = []
    if distance is not None:
        for i, (original, mapped) in enumerate(zip(wcses, ebd)):
            pos = original.all_world2pix([[spec["track"][i][1], spec["track"][i][2]]], 0)
            rd = mapped.all_pix2world(pos, 0)[0]
            delta = (rd - np.array([target["RA"][i], target["DEC"][i]]) + 180) % 360 - 180
            delta[0] *= np.cos(np.deg2rad(target["DEC"][i]))
            transform_error.append(float(np.linalg.norm(delta) * 3600))
        if max(transform_error) > 0.05:
            raise ValueError(f"EBD WCS fit error exceeds 0.05 arcsec: {max(transform_error)}")
    s = wr.im_stack
    engine = k.StackSearch(s.sci, s.var, s.psfs, s.zeroed_times, -1)
    minimum = max(10, int(np.ceil(0.4 * n)))
    engine.set_min_obs(minimum)
    engine.set_min_lh(0.0)
    engine.set_results_per_pixel(1)
    engine.disable_gpu_sigmag_filter()
    radius = int(spec.get("search_radius_pix", 8))
    velocity_offsets = settings.get("velocity_offsets_pix_day", [-4.0, -2.0, 0.0, 2.0, 4.0])
    clipper = SigmaGClipping(low_bnd=25, high_bnd=75, n_sigma=2, clip_negative=False)
    fields = ["x", "y", "vx", "vy", "lh", "flux", "obs_count"]

    def pack(t):
        return {key: float(getattr(t, key)) for key in fields}

    pools, clipped, curves = {}, {}, {}
    for direction, org, vel in [
        ("forward", origin, velocity),
        ("reverse", origin + dt[-1] * velocity, -velocity),
    ]:
        engine.set_start_bounds_x(
            max(0, int(np.floor(org[0] - radius))),
            min(size, int(np.ceil(org[0] + radius)) + 1),
        )
        engine.set_start_bounds_y(
            max(0, int(np.floor(org[1] - radius))),
            min(size, int(np.ceil(org[1] + radius)) + 1),
        )
        velocities = []
        for dx in velocity_offsets:
            for dy in velocity_offsets:
                trj = k.Trajectory()
                trj.vx, trj.vy = float(vel[0] + dx), float(vel[1] + dy)
                velocities.append(trj)
        engine.search_all(velocities, True)
        raw = [t for t in engine.get_all_results() if t.obs_count >= minimum and np.isfinite(t.lh)]
        pools[direction] = np.asarray([[getattr(t, key) for key in fields] for t in raw], dtype=np.float32)
        # Keep all pre-filter candidates. Apply the same target-centered positional gate in both frames.
        selected = []
        for trj in raw:
            delta = (
                np.array([trj.x, trj.y])
                + dt[-1] / 2 * np.array([trj.vx, trj.vy])
                - (origin + dt[-1] / 2 * velocity)
            )
            if np.linalg.norm(delta) <= radius:
                selected.append(trj)
        if not selected:
            clipped[direction], curves[direction] = [], None
            continue
        all_curves = np.asarray(engine.get_all_psi_phi_curves(selected))
        psi, phi = all_curves[:, :n], all_curves[:, n:]
        good = np.isfinite(psi) & np.isfinite(phi) & (phi > 0)
        lh = np.full_like(psi, np.nan)
        lh[good] = psi[good] / np.sqrt(phi[good])
        valid = good & clipper.compute_clipped_sigma_g_matrix(lh)
        ps, ph, count = (
            np.where(valid, psi, 0).sum(axis=1),
            np.where(valid, phi, 0).sum(axis=1),
            valid.sum(axis=1),
        )
        passing = [i for i in range(len(selected)) if count[i] >= minimum and ph[i] > 0 and ps[i] > 0]
        passing.sort(key=lambda i: float(ps[i] / np.sqrt(ph[i])), reverse=True)
        clipped[direction] = []
        for i in passing:
            d = pack(selected[i])
            d.update(
                lh=float(ps[i] / np.sqrt(ph[i])),
                flux=float(ps[i] / ph[i]),
                obs_count=int(count[i]),
            )
            clipped[direction].append(d)
        curves[direction] = (psi[passing[0]], phi[passing[0]], valid[passing[0]]) if passing else None
    prefix = Path(output).with_suffix("")
    np.savez_compressed(str(prefix) + "_pools.npz", fields=np.array(fields), **pools)
    matches = []
    best = clipped["forward"][0] if clipped["forward"] else None
    if best:
        psi, phi, valid = curves["forward"]
        np.savez_compressed(
            str(prefix) + "_curves.npz",
            times=times,
            psi=psi,
            phi=phi,
            valid=valid,
            predicted_xy=xy,
        )
        data = {
            key: [int(best[key]) if key in ["x", "y", "obs_count"] else best[key]]
            for key in ["x", "y", "vx", "vy", "flux", "obs_count"]
        }
        data.update(likelihood=[best["lh"]], obs_valid=[valid])
        results = Results(data)
        for kind in ["known", "synthetic", "ponder"]:
            cat = catalog[catalog["kind"] == kind]
            cat = transform_catalog(cat, distance, observer)
            for sep in settings.get("match_radii_arcsec", [1.0, 5.0, 10.0]):
                matcher = KnownObjsMatcher(cat, times, "truth", sep_thresh=sep, time_thresh_s=1.0)
                matcher.match(results, common)
                matcher.match_on_min_obs(results, 3)
                matcher.match_on_obs_ratio(results, 0.8)
                for name, mask in results["truth"][0].items():
                    cnt = int(np.count_nonzero(mask))
                    denominator = int(np.count_nonzero(cat["Name"] == name))
                    matches.append(
                        dict(
                            kind=kind,
                            name=str(name),
                            radius_arcsec=sep,
                            matched_obs=cnt,
                            retained_obs=int(valid.sum()),
                            catalog_obs=denominator,
                            retained_ratio=cnt / int(valid.sum()),
                            catalog_ratio=cnt / denominator,
                            min3=cnt >= 3,
                            catalog_ratio_ge_08=name in results[matcher.match_obs_ratio_col(0.8)][0],
                        )
                    )
        best["first_half_lh"] = (
            float(psi[valid & (dt < dt[-1] / 2)].sum() / np.sqrt(phi[valid & (dt < dt[-1] / 2)].sum()))
            if np.any(valid & (dt < dt[-1] / 2))
            else None
        )
        best["second_half_lh"] = (
            float(psi[valid & (dt >= dt[-1] / 2)].sum() / np.sqrt(phi[valid & (dt >= dt[-1] / 2)].sum()))
            if np.any(valid & (dt >= dt[-1] / 2))
            else None
        )
    result = dict(
        id=spec["id"],
        kind=spec["kind"],
        target_name=spec["target_name"],
        collection=spec["collection"],
        distance_au=distance,
        frame=frame,
        images=n,
        cutout_sha256=meta["cutout_sha256"],
        mask_flags=meta["mask_flags"],
        mask_growth=meta["mask_growth"],
        target_center_unmasked_obs=meta["target_center_unmasked_obs"],
        injection_pixels_retained=sum(f["injection_flagged_pixels_retained"] for f in frames),
        max_linear_error_pix=linear_error,
        max_ebd_fit_error_arcsec=max(transform_error, default=0),
        common_wcs=dict(common.to_header(relax=True)),
        ctio_geocentric_m=[float(v.value) for v in observer.to_geocentric()],
        best=best,
        forward_top10=clipped["forward"][:10],
        reverse_top10=clipped["reverse"][:10],
        matches=matches,
        kbmod_version=kbmod.__version__,
        kbmod_source=kbmod.__file__,
        workflow_source_sha256=digest(__file__),
        ponder_catalog_sha256=ponder_hash,
        kbmod_python_sha256={
            name: digest(Path(kbmod.__file__).parent / name)
            for name in ["reprojection.py", "work_unit.py", "filters/known_object_filters.py"]
        },
        host=socket.gethostname(),
        cuda_visible_devices=os.environ.get("CUDA_VISIBLE_DEVICES"),
        seconds=time.monotonic() - start,
        reprojection_seconds=meta["reprojection_seconds"],
        shared_reprojection_function=meta["shared_reprojection_function"],
        science_settings=settings.get("science_revision", {}),
        search_radius_pix=radius,
        velocity_offsets_pix_day=velocity_offsets,
        limitations=[
            "targeted search, not blind discovery",
            "single-night distance comparison",
            "catalogue truth presence is not verified injection success",
            "no native PSF corroboration in this rerun",
        ],
    )
    atomic_json(output, result)
    return str(output)
