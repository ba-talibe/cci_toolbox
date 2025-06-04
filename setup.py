from pathlib import Path

from setuptools import find_packages, setup

ROOT = Path(__file__).parent
PACKAGE_ROOT = ROOT / "toolbox"

with open("requirements.txt", "r") as f:
    requirements = [pac[:-1] for pac in f.readlines()]



setup(
    name="toolbox",
    description="collection de scripts pour le traitement de données",
    version="0.4.6",
    packages=find_packages(),
    install_requires=requirements,
)