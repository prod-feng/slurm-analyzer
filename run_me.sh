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
    if is_running; then
        echo "Already running (PID $(cat "$PIDFILE"))."
        exit 0
    fi

    echo "Starting Django development server..."
    echo "Source:    $SOURCE"
    echo "Container: $IMAGE"
    echo "Log:       $LOGFILE"
 
 singularity exec \
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
    "${IMAGE}" \
    python /workspace/manage.py refresh_slurm


    nohup singularity exec \
	 --env "PATH=/cm/shared/apps/slurm/current/bin:${PATH}"\
         --env "SLURM_SNAPSHOT_PATH=/workspace/.slurm_snapshot.pkl" \
        --bind "${SOURCE}:/workspace" \
        --bind /etc/nsswitch.conf:/etc/nsswitch.conf:ro \
        --bind /run/nslcd:/run/nslcd:ro \
        --bind /usr/lib64/libmunge.so.2:/usr/lib64/libmunge.so.2:ro \
        --bind /etc/passwd:/etc/passwd:ro \
        --bind /etc/group:/etc/group:ro \
        --bind /cm/shared/apps:/cm/shared/apps:ro \
        --bind /run/munge:/run/munge \
        "${IMAGE}" \
        python /workspace/manage.py runserver ${BINDIP}:${BINDPORT} \
        >>"$LOGFILE" 2>&1 &

    local pid=$!
    echo "$pid" > "$PIDFILE"

    # Give the process a moment to start and verify it is alive.
    sleep 1

    if is_running; then
        echo "Started successfully (PID $pid)."
    else
        echo "ERROR: Failed to start."
        echo "Check the log:"
        echo "  $LOGFILE"
        rm -f "$PIDFILE"
        exit 1
    fi
}

stop() {
    if ! is_running; then
        echo "Not running."
        exit 0
    fi

    local pid
    pid="$(cat "$PIDFILE")"

    echo "Stopping (PID $pid)..."
    kill "$pid"

    # Wait up to 10 seconds for graceful shutdown.
    for _ in {1..20}; do
        if ! kill -0 "$pid" 2>/dev/null; then
            rm -f "$PIDFILE"
            echo "Stopped."
            return 0
        fi
        sleep 0.5
    done

    echo "Process did not stop gracefully; sending SIGKILL..."
    kill -9 "$pid" 2>/dev/null || true

    rm -f "$PIDFILE"
    echo "Stopped."
}

status() {
    if is_running; then
        local pid
        pid="$(cat "$PIDFILE")"

        echo "Running (PID $pid)."
        echo "URL: http://$BINDIP:$BINDPORT/"
        echo "Log: $LOGFILE"
    else
        echo "Not running."
    fi
}

case "${1:-}" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    status)
        status
        ;;
    restart)
        stop
        start
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status}"
        exit 1
        ;;
esac

