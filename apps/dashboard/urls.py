"""Legacy compatibility module.

The project-level URL configuration now mounts webapp.slurm.urls directly.
Keep this module importable for older deployments/tests, but do not duplicate
Slurm API implementations here.
"""

from webapp.slurm.urls import urlpatterns
