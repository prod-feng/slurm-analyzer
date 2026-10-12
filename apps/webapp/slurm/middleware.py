"""Select the correct persistent DuckDB for each cluster URL."""
from django.http import HttpResponseNotFound
from slurm_analytics.storage import (set_current_database_path, reset_current_database_path,
                                     set_current_cluster_mode, reset_current_cluster_mode)
from .clusters import get_cluster, get_default_cluster


class ClusterDatabaseMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        parts = request.path_info.strip("/").split("/")
        if len(parts) >= 2 and parts[0] == "clusters":
            cluster = get_cluster(parts[1])
            if cluster is None:
                return HttpResponseNotFound("Unknown Slurm cluster: {}".format(parts[1]))
            prefix = "/clusters/{}".format(cluster["slug"])
        else:
            cluster = get_default_cluster()
            prefix = ""
        request.slurm_cluster = cluster
        request.slurm_cluster_prefix = prefix
        token = set_current_database_path(cluster["db_path"])
        mode_token = set_current_cluster_mode(cluster.get("mode", "live"))
        try:
            return self.get_response(request)
        finally:
            reset_current_cluster_mode(mode_token)
            reset_current_database_path(token)
