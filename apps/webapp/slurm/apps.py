# webapp/slurm/apps.py

import logging
from django.apps import AppConfig

logger = logging.getLogger(__name__)


class SlurmConfig(AppConfig):

    default_auto_field = "django.db.models.BigAutoField"
    name = "webapp.slurm"

    def ready(self):
        # Prime the node -> GPU model inventory when Django starts. This is
        # intentionally separate from job ingestion; node inventory is quick
        # to read and lets historical jobs be classified by their stored NodeList.
        try:
            from .services import get_snapshot
            from .state import state
            from slurm_analytics.node_inventory import refresh_node_inventory

            snapshot = get_snapshot()
            if snapshot is not None:
                snapshot.nodes = refresh_node_inventory()
                state.replace(snapshot)
                logger.info(
                    "Loaded GPU model inventory for %d Slurm nodes at Django startup.",
                    snapshot.nodes.height,
                )
        except Exception:
            # Django must still start if Slurm is temporarily unavailable.
            logger.exception("Unable to load Slurm node inventory at Django startup.")
