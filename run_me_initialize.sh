#!/usr/bin/env bash

set -euo pipefail

BINDIP="172.17.120.8"
BINDPORT="8666"

export DJANGO_DEBUG=true

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

IMAGE="${PROJECT_ROOT}/apptainer/slurm-manager-ldap-django.sif"
#SOURCE="${PROJECT_ROOT}/app"
SOURCE="${PROJECT_ROOT}/apps"

PIDFILE="${PROJECT_ROOT}/abs.pid"
LOGFILE="${PROJECT_ROOT}/abs.log"

if [[ ! -f "$IMAGE" ]]; then
    echo "ERROR: Container not found:"
    echo "  $IMAGE"
    exit 1
fi

is_running() {
    [[ -f "$PIDFILE" ]] || return 1

    local pid
    pid="$(cat "$PIDFILE")"

    if [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null; then
        return 0
    fi

    # Stale PID file
    rm -f "$PIDFILE"
    return 1
}

start() {
    echo "Initializing Slurm database..."
    echo "Source:    $SOURCE"
    echo "Container: $IMAGE"
    echo "Log:       $LOGFILE"

    singularity exec \
    --env "PATH=/cm/shared/apps/slurm/current/bin:${PATH}" \
    --bind "${SOURCE}:/workspace" \
    --bind /usr/lib64/libreadline.so.7:/lib/x86_64-linux-gnu/libreadline.so.7:ro \
    --bind /usr//lib64/libhistory.so.7:/lib/x86_64-linux-gnu/libhistory.so.7:ro \
    --bind /etc/nsswitch.conf:/etc/nsswitch.conf:ro \
    --bind /run/nslcd:/run/nslcd:ro \
    --bind /usr/lib64/libmunge.so.2:/usr/lib64/libmunge.so.2:ro \
    --bind /etc/passwd:/etc/passwd:ro \
    --bind /etc/group:/etc/group:ro \
    --bind /cm/shared/apps:/cm/shared/apps:ro \
    --bind /run/munge:/run/munge \
    "${IMAGE}" \
    python /workspace/manage.py initialize_slurm \
    --history-file /workspace/rawdata/20260501T000000_20260601T000000.out\
    --start 2026-05-01T00:00:00
    #    --start 2026-05-01T00:00:00 --end 2026-05-07T00:00:00 --chunk-days 3 --timeout 1800


    # Give the process a moment to start and verify it is alive.
    sleep 1
}

start
