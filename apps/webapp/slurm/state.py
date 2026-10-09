"""Process-safe-ish persistent state for the current Slurm snapshot.

The analytical snapshot is kept in memory for fast API reads, but is also
persisted to a small local pickle file.  This is intentionally not a database:
it lets a management command/systemd timer refresh the snapshot in one process
and lets Django workers read it in another process.
"""

from __future__ import absolute_import

import os
import pickle
import tempfile
import threading
from pathlib import Path


DEFAULT_SNAPSHOT_PATH = (
    Path(__file__).resolve().parents[2] / ".slurm_snapshot.pkl"
)


class SlurmSnapshot(object):

    def __init__(
        self,
        ingestion,
        analytics,
        nodes=None,
        refreshed_at=None,
    ):
        self.ingestion = ingestion
        self.analytics = analytics
        self.nodes = nodes
        self.refreshed_at = refreshed_at


class SlurmState(object):

    def __init__(self, path=None):
        self._snapshot = None
        self._snapshot_mtime_ns = None
        self._lock = threading.RLock()
        self.path = Path(
            path
            or os.environ.get("SLURM_SNAPSHOT_PATH")
            or str(DEFAULT_SNAPSHOT_PATH)
        )

    def _file_mtime_ns(self):
        try:
            return self.path.stat().st_mtime_ns
        except OSError:
            return None

    def _load_if_newer(self):
        mtime_ns = self._file_mtime_ns()
        if mtime_ns is None:
            return

        if (
            self._snapshot is not None
            and self._snapshot_mtime_ns == mtime_ns
        ):
            return

        try:
            with self.path.open("rb") as handle:
                snapshot = pickle.load(handle)
        except (OSError, EOFError, pickle.PickleError, ValueError, TypeError):
            return

        if not isinstance(snapshot, SlurmSnapshot):
            return

        self._snapshot = snapshot
        self._snapshot_mtime_ns = mtime_ns

    def get(self):
        with self._lock:
            self._load_if_newer()
            return self._snapshot


    def get_or_build(self, builder):
        """Return the current snapshot, rebuilding it once if necessary."""
        with self._lock:
            self._load_if_newer()

            if self._snapshot is not None:
                return self._snapshot

            snapshot = builder()
            if snapshot is None:
                return None

            # RLock allows replace() to acquire the same lock safely.
            self.replace(snapshot)
            return self._snapshot

    def replace(self, snapshot):
        """Publish a snapshot atomically to memory and disk."""
        self.path.parent.mkdir(parents=True, exist_ok=True)

        fd, temp_name = tempfile.mkstemp(
            prefix=".slurm_snapshot-",
            suffix=".tmp",
            dir=str(self.path.parent),
        )
        try:
            with os.fdopen(fd, "wb") as handle:
                pickle.dump(snapshot, handle, protocol=pickle.HIGHEST_PROTOCOL)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_name, str(self.path))
        except Exception:
            try:
                os.unlink(temp_name)
            except OSError:
                pass
            raise

        with self._lock:
            self._snapshot = snapshot
            self._snapshot_mtime_ns = self._file_mtime_ns()

    def clear(self):
        """Clear memory and remove the persisted snapshot."""
        with self._lock:
            self._snapshot = None
            self._snapshot_mtime_ns = None
            try:
                self.path.unlink()
            except OSError:
                pass


state = SlurmState()
