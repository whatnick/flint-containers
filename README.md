# Flint Containers

This be yer compass me-hearties. Find yer way about the seas when sailing with [flint](https://github.com/tjgalvin/flint)!

## What is this?

Build scripts for containers to use with the [flint pipeline](https://github.com/tjgalvin/flint).

## Where are the cotainers?

Each container is built and pushed to the Github Container Registry. The containers for each application are published as separated packages under the [flint-crew GitHub page](https://github.com/orgs/flint-crew/packages?repo_name=flint-containers).

We previously published to [DockerHub](https://hub.docker.com/r/alecthomson/flint-containers/tags), these are no longer updated.

Each application is available under its name with a tag attached from the latest release or git hash from this repository. See the [packages](https://github.com/orgs/flint-crew/packages?repo_name=flint-containers) to get the correct name and tag for your usage.

To get a container you can run

```bash
# for docker
docker pull ghcr.io/flint-crew/{application}:{tag}
```

or

```bash
# for singularity / apptainer
singularity pull docker://ghcr.io/flint-crew/{application}:{tag}
```

## Supported containers

- calibrate: André Offringa's calibrate. Modified by Emil Lenc for use with ASKAP.
- [ASKAPsoft](https://www.atnf.csiro.au/computing/software/askapsoft/sdp/docs/current): Interferometric applications developed for the ASKAP radio telescope by CSIRO.
- [AOFlagger](https://aoflagger.readthedocs.io/en/latest/): Automatic RFI flagger developed by André Offringa.
- [WSClean](https://wsclean.readthedocs.io/en/latest/): WSClean (w-stacking clean) is a fast generic widefield imager developed by André Offringa.
- [Aegean](https://github.com/PaulHancock/Aegean): AegeanTools source finding package developed by Paul Hancock.
- [PotatoPeel](https://gitlab.com/Sunmish/potato): Peel out that annoying, terrible object developed by Stefan Duchesne.
- [CASA](https://casa.nrao.edu/): The Common Astronomy Software Applications package, developed by NRAO.

## Cloud-native: the `flint-worker` image

`Dockerfile-flint-worker` builds a single, consolidated image that bundles the
[`askap-flint`](https://pypi.org/project/askap-flint/) pipeline together with the
radio tools it drives (`wsclean`, `aoflagger`, `casacore`, `calibrate`, `aegean`,
`potato`) plus a Dask + JupyterLab runtime.

It is built on a [Jupyter Docker Stacks](https://github.com/jupyter/docker-stacks)
base (`scipy-notebook`, Ubuntu 24.04 / noble), so it inherits the `jovyan` user,
conda/mamba, JupyterLab, the Zero-to-JupyterHub start scripts and `tini`.
`wsclean`/`aoflagger` come from the matching [KERN-10](https://kernsuite.info/)
apt repo, `calibrate` is built from the submodule, and the Python stack
(flint, dask, dask-kubernetes, python-casacore) is installed into the conda
environment.

It is built and published by `.github/workflows/flint-worker.yml` to:

```
ghcr.io/<owner>/flint-worker:<tag>
```

The image is designed to be used in two roles with an **identical** environment,
which Dask requires (scheduler, workers and the client/notebook must match):

1. **JupyterHub singleuser image** — set it as the Zero-to-JupyterHub
   `singleuser.image` (or a `profileList` entry).
2. **Dask worker/scheduler image** — pass it to the
   [dask-kubernetes operator](https://kubernetes.dask.org/) via
   `KubeCluster(image=...)`.

Running the radio tools directly on the Dask workers (rather than via
Singularity/Apptainer, as on HPC) removes the container-in-container requirement
and lets the pipeline run interactively on a Kubernetes platform such as
[EASI Hub](https://research.csiro.au/easi/). See `examples/` for a
`KubeCluster` notebook snippet and a JupyterHub values snippet.
