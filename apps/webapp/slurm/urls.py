"""URL routes for the Slurm API."""

from django.urls import path

from . import api


urlpatterns = [
    path("metadata/", api.api_metadata, name="api-metadata"),
    path("summary/", api.api_summary, name="api-summary"),
    path("users/", api.api_users, name="api-users"),
    path("partitions/", api.api_partitions, name="api-partitions"),
    path("partitions/usage/", api.api_partition_usage, name="api-partition-usage"),
    path("partitions/gpu-types/", api.api_partition_gpu_types, name="api-partition-gpu-types"),
    path("gpu/", api.api_gpu, name="api-gpu"),
    path("runtime/", api.api_runtime, name="api-runtime"),
    path("jobs/", api.api_jobs, name="api-jobs"),
    path("queue/", api.api_queue, name="api-queue"),
    path("timeseries/", api.api_timeseries, name="api-timeseries"),
    path("refresh/", api.api_refresh, name="api-refresh"),
    path("db-metadata/", api.api_db_metadata, name="api-db-metadata"),
    path("accounts/", api.api_account_usage, name="api-account-usage"),
    path("accounts/tree/", api.api_account_tree, name="api-account-tree"),
    path("users/usage/", api.api_user_usage, name="api-user-usage"),
]
