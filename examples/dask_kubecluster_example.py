"""Interactive FLINT on Kubernetes with the dask-kubernetes operator.

Run this from a JupyterHub notebook whose singleuser image is the SAME
``flint-worker`` image referenced below (homogeneous environment is required by
Dask). It spins up a Dask cluster of ``flint-worker`` pods that already contain
wsclean / calibrate / aoflagger etc. on PATH, so FLINT tasks execute the tools
locally on the workers instead of via Singularity/Apptainer.

Prereqs on the cluster:
  * the dask-kubernetes operator is installed (it is part of the EASI dask stack)
  * a shared filesystem (e.g. an EFS-backed PVC) is mounted into the notebook and
    the worker pods at the same path so all of them can see the Measurement Sets.
"""

from dask_kubernetes.operator import KubeCluster, make_cluster_spec

IMAGE = "ghcr.io/whatnick/flint-worker:latest"
SHARED_PVC = "flint-scratch"   # ReadWriteMany PVC (EFS) holding the MS / working dir
MOUNT_PATH = "/home/jovyan/work"

# Build a base spec, then attach the shared volume to scheduler + workers so the
# whole cluster sees the same Measurement Sets and outputs.
spec = make_cluster_spec(
    name="flint",
    image=IMAGE,
    n_workers=4,
    resources={
        "requests": {"cpu": "4", "memory": "16Gi"},
        "limits": {"cpu": "4", "memory": "16Gi"},
    },
)

volume = {"name": "scratch", "persistentVolumeClaim": {"claimName": SHARED_PVC}}
mount = {"name": "scratch", "mountPath": MOUNT_PATH}

for kind in ("worker", "scheduler"):
    pod_spec = spec["spec"][kind]["spec"]
    pod_spec.setdefault("volumes", []).append(volume)
    for container in pod_spec["containers"]:
        container.setdefault("volumeMounts", []).append(mount)

cluster = KubeCluster(custom_cluster_spec=spec)
cluster.adapt(minimum=1, maximum=8)   # scale-to-zero friendly
client = cluster.get_client()
print("Dask dashboard:", client.dashboard_link)

# --- Option A: drive FLINT functions directly against this Dask client ---------
# FLINT's tasks are plain callables; submit them to the cluster like any Dask work.
#
#   from flint.imager.wsclean import wsclean_imager
#   fut = client.submit(wsclean_imager, ...)
#   fut.result()

# --- Option B: hand the cluster to FLINT's Prefect DaskTaskRunner --------------
# Requires the KubeCluster-aware runner added in the companion `flint` branch
# (flint.prefect.clusters.get_dask_runner with a kubernetes backend), e.g.:
#
#   from prefect_dask import DaskTaskRunner
#   runner = DaskTaskRunner(address=client.scheduler.address)
#   # pass `runner` as the task_runner to a flint flow
