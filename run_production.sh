
#!/usr/bin/env bash
set -euo pipefail

BINDIP="10.10.10.211"
BINDPORT="8666"

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE="${PROJECT_ROOT}/apptainer/slurm-manager-ldap-django.sif"
SOURCE="${PROJECT_ROOT}/apps"

if [[ ! -f "$IMAGE" ]]; then
    echo "ERROR: Container not found: $IMAGE" >&2
    exit 1
fi

if [[ ! -f "${SOURCE}/config/gunicorn.conf.py" ]]; then
    echo "ERROR: Gunicorn configuration not found." >&2
    exit 1
fi

exec singularity exec \
    --env "PATH=/cm/shared/apps/slurm/current/bin:${PATH}" \
    --env "DJANGO_DEBUG=false" \
    --env "SLURM_CLUSTERS_CONFIG=/workspace/clusters.json" \
    --env "SLURM_SNAPSHOT_PATH=/workspace/.slurm_snapshot.pkl" \
    --env "GUNICORN_BIND=${BINDIP}:${BINDPORT}" \
    --env "GUNICORN_WORKERS=1" \
    --bind "${SOURCE}:/workspace" \
    --bind /etc/nsswitch.conf:/etc/nsswitch.conf:ro \
    --bind /run/nslcd:/run/nslcd:ro \
    --bind /usr/lib64/libmunge.so.2:/usr/lib64/libmunge.so.2:ro \
    --bind /etc/passwd:/etc/passwd:ro \
    --bind /etc/group:/etc/group:ro \
    --bind /cm/shared/apps:/cm/shared/apps:ro \
    --bind /run/munge:/run/munge \
    "${IMAGE}" \
    python -m gunicorn \
        --chdir /workspace \
        --config /workspace/config/gunicorn.conf.py \
        slurm_web.wsgi:application

