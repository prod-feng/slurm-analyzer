# webapp/slurm/apps.py

from django.apps import AppConfig


class SlurmConfig(AppConfig):

    default_auto_field = (
        "django.db.models.BigAutoField"
    )

    name = "webapp.slurm"

    def ready(self):
        pass

