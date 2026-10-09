"""Backward-compatible facade for the consolidated Django Slurm service."""

from webapp.slurm.services import get_snapshot, refresh_slurm


class SlurmAnalyticsService(object):
    """Compatibility facade; new code should use webapp.slurm.services."""

    def refresh(self, start=None, end=None):
        from slurm_analytics.sacct import SacctConfig
        return refresh_slurm(SacctConfig(clusters="all", start=start, end=end))

    def ensure_loaded(self):
        snapshot = get_snapshot()
        if snapshot is None:
            return self.refresh()
        return snapshot.ingestion

    @property
    def loaded_at(self):
        snapshot = get_snapshot()
        return snapshot.refreshed_at if snapshot else None

    @property
    def jobs(self):
        return self.ensure_loaded().consolidated_jobs

    @property
    def steps(self):
        return self.ensure_loaded().steps

    def analytics(self):
        snapshot = get_snapshot()
        if snapshot is None:
            self.refresh()
            snapshot = get_snapshot()
        return snapshot.analytics


service = SlurmAnalyticsService()
