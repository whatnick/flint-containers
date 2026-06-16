"""Emit a sample FLINT astronomy-workflow notebook (nbformat v4) using only json.

The notebook runs a continuum-imaging mini-pipeline on FLINT's bundled sample
ASKAP RACS Measurement Set, distributed onto a Dask Gateway worker (the same
flint-worker image), and renders the image + source catalogue.
"""

import json
import sys

WORKFLOW_SRC = r'''
def run_imaging_workflow(flux_jy=1.0, noise_jy=0.1, size=512, scale="30asec", niter=2000, seed=7):
    """Runs on a Dask worker. A genuine *simulate -> image -> find* loop.

    FLINT's bundled sample MS provides a real ASKAP RACS template (field
    0635-31, 887 MHz) but it is a fully-flagged code-path fixture with zeroed
    DATA *and* zeroed UVW -- so it has no uv coverage to image. We therefore use
    it as a template: fabricate simple synthetic uv coverage, inject a `flux_jy`
    Stokes-I point source at the phase centre plus thermal noise, then image with
    WSClean and detect with BANE+Aegean (FLINT's source-finding tools). The MS
    ships inside the flint-worker image, so no shared filesystem is required."""
    import shutil, subprocess, re, glob, os
    from pathlib import Path
    import numpy as np
    from astropy.io import fits
    from astropy.wcs import WCS
    from casacore.tables import table
    from flint.utils import get_packaged_resource_path

    rng = np.random.default_rng(seed)
    work = Path("/tmp/flint_sample")
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)

    zip_path = get_packaged_resource_path(
        package="flint.data.tests",
        filename="SB39400.RACS_0635-31.beam0.small.ms.zip",
    )
    shutil.unpack_archive(str(zip_path), str(work))
    ms = work / "SB39400.RACS_0635-31.beam0.small.ms"

    # --- 0. simulate onto the real RACS template ---
    with table(str(ms), readonly=False, ack=False) as t:
        n = t.nrows()
        uvw = rng.uniform(-1500, 1500, size=(n, 3)); uvw[:, 2] *= 0.05
        t.putcol("UVW", uvw)
        d = t.getcol("DATA")
        noise = (rng.normal(0, noise_jy, d.shape) + 1j * rng.normal(0, noise_jy, d.shape)).astype(d.dtype)
        model = noise.copy()
        model[:, :, 0] += flux_jy            # XX
        model[:, :, 3] += flux_jy            # YY  (Stokes-I point source)
        t.putcol("DATA", model)
        t.putcol("FLAG", np.zeros_like(t.getcol("FLAG")))
        t.putcol("WEIGHT", np.ones_like(t.getcol("WEIGHT")))

    # --- 1. image with WSClean ---
    name = str(work / "sample_image")
    ws = subprocess.run(
        ["wsclean", "-size", str(size), str(size), "-scale", scale,
         "-niter", str(niter), "-auto-threshold", "3", "-mgain", "0.8",
         "-weight", "briggs", "0", "-name", name, "-data-column", "DATA", str(ms)],
        capture_output=True, text=True, timeout=900,
    )
    restored = name + "-image.fits"
    if not Path(restored).exists():
        raise RuntimeError("wsclean failed:\n" + ws.stdout[-1500:] + ws.stderr[-1500:])

    # --- 2. write a clean 2D FITS (Aegean 2.3.0 mishandles degenerate axes) ---
    with fits.open(restored) as hd:
        img = np.squeeze(hd[0].data).astype("float32")
        wcs2d = WCS(hd[0].header).celestial
        hdr = wcs2d.to_header()
        for k in ("BMAJ", "BMIN", "BPA", "BUNIT"):
            if k in hd[0].header:
                hdr[k] = hd[0].header[k]
        freq = hd[0].header.get("CRVAL3")
    image2d = str(work / "sample_2d.fits")
    fits.writeto(image2d, img, hdr, overwrite=True)

    # --- 3. BANE (background/rms) + Aegean (source finding) ---
    # Clear any stale POSIX shared-memory segments from a crashed prior BANE
    # (AegeanTools 2.3.0 reuses fixed names like /ibkg, /irms).
    for shm in glob.glob("/dev/shm/i*bkg") + glob.glob("/dev/shm/i*rms") + glob.glob("/dev/shm/*bkg*") + glob.glob("/dev/shm/*rms*"):
        try:
            os.remove(shm)
        except OSError:
            pass
    subprocess.run(["BANE", image2d, "--cores", "1", "--stripes", "1"],
                   capture_output=True, text=True, timeout=600)
    table_out = str(work / "sources.fits")
    ae = subprocess.run(["aegean", image2d, "--autoload", "--nocov", "--table", table_out],
                        capture_output=True, text=True, timeout=600)
    m = re.search(r"found (\d+) sources", ae.stdout + ae.stderr)
    n_sources = int(m.group(1)) if m else None
    cat_rows, brightest = [], None
    comp = str(work / "sources_comp.fits")
    if Path(comp).exists():
        from astropy.table import Table
        tcat = Table.read(comp)
        n_sources = len(tcat) if n_sources is None else n_sources
        cols = [c for c in ("ra", "dec", "peak_flux", "int_flux") if c in tcat.colnames]
        tcat.sort("peak_flux", reverse=True)
        for r in tcat[:10]:
            cat_rows.append({c: float(r[c]) for c in cols})
        if "peak_flux" in tcat.colnames:
            brightest = float(np.max(tcat["peak_flux"]))

    peak_yx = np.unravel_index(int(np.nanargmax(img)), img.shape)
    return {
        "image": img,
        "img_stats": {
            "shape": list(img.shape),
            "peak_jy_per_beam": float(np.nanmax(img)),
            "peak_pixel_yx": [int(peak_yx[0]), int(peak_yx[1])],
            "rms": float(np.nanstd(img)),
        },
        "injected_flux_jy": flux_jy,
        "freq_hz": freq,
        "n_sources": n_sources,
        "brightest_peak_flux_jy": brightest,
        "catalogue_head": cat_rows,
        "wsclean_tail": ws.stdout.strip().splitlines()[-3:],
    }
'''.strip()

cells = []

def md(text):
    cells.append({"cell_type": "markdown", "metadata": {}, "source": text.splitlines(keepends=True)})

def code(text):
    cells.append({"cell_type": "code", "metadata": {}, "execution_count": None,
                  "outputs": [], "source": text.strip("\n").splitlines(keepends=True)})

md("""# FLINT sample astronomy workflow on EASI

A self-contained continuum **predict → image → find** loop on FLINT's bundled
sample **ASKAP RACS** Measurement Set (`SB39400.RACS_0635-31.beam0`), distributed
onto a **Dask Gateway** worker that uses the same `flint-worker` image:

1. **Simulate** a 1 Jy point source at the phase centre onto the *real* ASKAP uvw
   coverage (the bundled MS is a fully-flagged code-path fixture, so we unflag it
   and inject model visibilities)
2. **Image** the visibilities with **WSClean**
3. **Find sources** with **Aegean**, then render the image and inspect the catalogue

The sample MS ships *inside* the image, so the workflow runs on the worker with no
shared filesystem.""")

code("""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from dask_gateway import GatewayCluster

cluster = GatewayCluster()      # pre-wired to the flint-worker image on EASI
cluster.scale(1)
client = cluster.get_client()
client.wait_for_workers(1, timeout=300)
print("Dask Gateway cluster:", cluster.name, "| workers:", len(client.scheduler_info()["workers"]))
""")

code(WORKFLOW_SRC)

code("""
# Distribute the simulate + image + source-finding onto the Dask worker
future = client.submit(run_imaging_workflow, flux_jy=1.0, noise_jy=0.1, size=512, scale="30asec", niter=2000)
result = future.result()
print("WSClean tail:", *result["wsclean_tail"], sep="\\n  ")
print("\\nInjected flux (Jy):", result["injected_flux_jy"])
print("Image stats:", result["img_stats"])
print("Frequency (Hz):", result["freq_hz"])
print("Aegean sources found:", result["n_sources"],
      "| brightest peak_flux (Jy/beam):", result["brightest_peak_flux_jy"])
""")

code("""
# Render the restored image: full field (stretched to the source) + a zoom
img = result["image"]
rms = result["img_stats"]["rms"]
peak = result["img_stats"]["peak_jy_per_beam"]
py, px = result["img_stats"]["peak_pixel_yx"]

fig, axes = plt.subplots(1, 2, figsize=(12, 5))
im0 = axes[0].imshow(img, origin="lower", cmap="cubehelix", vmin=-3*rms, vmax=peak)
axes[0].set_title("Full field (512 x 512)")
fig.colorbar(im0, ax=axes[0], label="Jy/beam")

h = 40
sub = img[py-h:py+h, px-h:px+h]
im1 = axes[1].imshow(sub, origin="lower", cmap="cubehelix", vmin=-3*rms, vmax=peak,
                     extent=[px-h, px+h, py-h, py+h])
axes[1].set_title(f"Zoom on recovered ~{peak:.2f} Jy source")
fig.colorbar(im1, ax=axes[1], label="Jy/beam")
for a in axes:
    a.set_xlabel("pixel"); a.set_ylabel("pixel")
fig.suptitle("SB39400.RACS_0635-31.beam0 -- WSClean restored image (simulated point source)")
fig.tight_layout()
fig.savefig("/tmp/flint_sample/restored_image.png", dpi=110)
print(f"peak {peak:.3f} Jy/beam at pixel ({py},{px}); image rms {rms:.2e} Jy/beam")
""")

code("""
# Peek at the Aegean component catalogue
import pandas as pd
rows = result["catalogue_head"]
print(pd.DataFrame(rows) if rows else "no catalogue rows returned")
""")

code("""
cluster.shutdown()
print("cluster shut down")
""")

nb = {
    "cells": cells,
    "metadata": {
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python", "version": "3.12"},
    },
    "nbformat": 4,
    "nbformat_minor": 5,
}

out = sys.argv[1] if len(sys.argv) > 1 else "flint_sample_workflow.ipynb"
with open(out, "w") as f:
    json.dump(nb, f, indent=1)
print("wrote", out)
