import multiprocessing
import os

# Network
bind = os.environ.get("GUNICORN_BIND", "10.10.10.211:8666")

# Worker configuration
# Start with one worker if Slurm data is stored in process-local memory.
workers = int(os.environ.get("GUNICORN_WORKERS", "1"))
worker_class = "sync"

# Request handling
timeout = 120
graceful_timeout = 30
keepalive = 5
max_requests = 2000
max_requests_jitter = 200

# Logging: stdout/stderr are captured by the launcher script.
accesslog = "-"
errorlog = "-"
loglevel = os.environ.get("GUNICORN_LOG_LEVEL", "info")

# Process management
preload_app = False
daemon = False
capture_output = True

