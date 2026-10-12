"""URL routes for the Slurm API."""

from functools import wraps

from django.urls import path

from . import api


def cluster_route(view):
    """Allow cluster URL prefixes without passing cluster_slug to API views.

    ClusterDatabaseMiddleware selects the database from the request path; the
    API view itself does not need the URL kwarg.
    """
    @wraps(view)
    def wrapped(request, *args, **kwargs):
        kwargs.pop("cluster_slug", None)
        return view(request, *args, **kwargs)
    return wrapped


urlpatterns = [
    path("metadata/", cluster_route(api.api_metadata), name="api-metadata"),
    path("summary/", cluster_route(api.api_summary), name="api-summary"),
    path("users/", cluster_route(api.api_users), name="api-users"),
    path("partitions/", cluster_route(api.api_partitions), name="api-partitions"),
    path("partitions/usage/", cluster_route(api.api_partition_usage), name="api-partition-usage"),
    path("partitions/gpu-types/", cluster_route(api.api_partition_gpu_types), name="api-partition-gpu-types"),
    path("gpu-types/usage/", cluster_route(api.api_gpu_type_usage), name="api-gpu-type-usage"),
    path("gpu/", cluster_route(api.api_gpu), name="api-gpu"),
    path("runtime/", cluster_route(api.api_runtime), name="api-runtime"),
    path("jobs/", cluster_route(api.api_jobs), name="api-jobs"),
    path("queue/", cluster_route(api.api_queue), name="api-queue"),
    path("timeseries/", cluster_route(api.api_timeseries), name="api-timeseries"),
    path("refresh/", cluster_route(api.api_refresh), name="api-refresh"),
    path("db-metadata/", cluster_route(api.api_db_metadata), name="api-db-metadata"),
    path("accounts/", cluster_route(api.api_account_usage), name="api-account-usage"),
    path("accounts/tree/", cluster_route(api.api_account_tree), name="api-account-tree"),
    path("users/usage/", cluster_route(api.api_user_usage), name="api-user-usage"),
]
