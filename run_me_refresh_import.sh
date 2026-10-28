#!/usr/bin/env bash

set -Eeuo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE="${PROJECT_ROOT}/apps"
IMAGE="${PROJECT_ROOT}/apptainer/slurm-manager-ldap-django.sif"
LOGFILE="${PROJECT_ROOT}/refresh_slurm_data.log"
LOCKFILE="${PROJECT_ROOT}/refresh_slurm_data.lock"

CLUSTER="cluster2"

# Log output to both the terminal and the log file.
exec > >(tee -a "$LOGFILE") 2>&1

set -x

echo "===== Refresh started: $(date -Is) ====="

# Ensure only one refresh runs at a time.
exec 9>"$LOCKFILE"
if ! flock -n 9; then
    echo "$(date -Is) Refresh already running; skipping."
    exit 0
fi

# Check that the container image exists.
if [[ ! -f "$IMAGE" ]]; then
    echo "ERROR: Container not found: $IMAGE"
    exit 1
fi

# Run a Django management command inside the container.
run_manage() {
    singularity exec \
        --pwd /workspace \
        --env "PATH=/cm/shared/apps/slurm/current/bin:${PATH}" \
        --env "SLURM_DUCKDB_PATH=/workspace/data/${CLUSTER}.duckdb" \
        --bind "${SOURCE}:/workspace" \
        --bind /usr/lib64/libreadline.so.7:/lib/x86_64-linux-gnu/libreadline.so.7:ro \
        --bind /usr/lib64/libhistory.so.7:/lib/x86_64-linux-gnu/libhistory.so.7:ro \
        --bind /etc/nsswitch.conf:/etc/nsswitch.conf:ro \
        --bind /run/nslcd:/run/nslcd:ro \
        --bind /usr/lib64/libmunge.so.2:/usr/lib64/libmunge.so.2:ro \
        --bind /etc/passwd:/etc/passwd:ro \
        --bind /etc/group:/etc/group:ro \
        --bind /cm/shared/apps:/cm/shared/apps:ro \
        --bind /run/munge:/run/munge \
        "$IMAGE" \
        python /workspace/manage.py "$@"
}

# Refresh the persistent Slurm database.
run_manage refresh_slurm_db  \
  --history-file "/workspace/export_data/${CLUSTER}/sacct_*.out" \
  --accounts-file /workspace/export_data/${CLUSTER}/accounts.json \
  --nodes-file /workspace/export_data/${CLUSTER}/nodes.json \
  --partitions-file /workspace/export_data/${CLUSTER}/partitions.json \
  --start 2026-10-01 --end 2026-10-27 

# Refresh the dashboard snapshot.
#run_manage refresh_slurm

echo "===== Refresh finished: $(date -Is) ====="
