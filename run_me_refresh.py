#!/usr/bin/env bash

set -Eeuo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE="${PROJECT_ROOT}/apps"
IMAGE="${PROJECT_ROOT}/apptainer/slurm-manager-ldap-django.sif"
LOGFILE="${PROJECT_ROOT}/refresh_slurm_data.log"
LOCKFILE="${PROJECT_ROOT}/refresh_slurm_data.lock"

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
        --env "SLURM_SNAPSHOT_PATH=/workspace/.slurm_snapshot.pkl" \
        --bind "${SOURCE}:/workspace" \
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
run_manage refresh_slurm_db --chunk-days 1 

# Refresh the dashboard snapshot.
run_manage refresh_slurm

echo "===== Refresh finished: $(date -Is) ====="
