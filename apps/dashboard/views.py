from django.shortcuts import render
from webapp.slurm.clusters import get_clusters, get_default_cluster


def dashboard(request, cluster_slug=None):
    cluster = getattr(request, "slurm_cluster", None) or get_default_cluster()
    return render(request, "dashboard/index.html", {
        "clusters": get_clusters(),
        "active_cluster": cluster,
        "cluster_prefix": getattr(request, "slurm_cluster_prefix", ""),
    })
