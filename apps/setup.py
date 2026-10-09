from setuptools import find_packages
from setuptools import setup


setup(
    name="slurm-analytics",
    version="0.1.0",
    description="Slurm accounting ingestion and analytics",
    packages=find_packages(),
    python_requires=">=3.8",
    entry_points={"console_scripts": ["slurm-analytics=slurm_analytics.cli:main"]},
)

