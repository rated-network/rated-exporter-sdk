import os
from pathlib import Path

from setuptools import find_packages, setup  # type: ignore

version = os.environ.get("RELEASE_VERSION", "0.0.1")


def read_requirements(filename="requirements.txt"):
    try:
        with open(filename) as req:
            return req.read().splitlines()
    except FileNotFoundError:
        return []


requirements = read_requirements("requirements.txt")

setup(
    name="rated_exporter_sdk",
    version=version,
    python_requires=">=3.6",
    install_requires=requirements,
    packages=find_packages(exclude=["tests", "tests.*"]),
    long_description=Path("README.md").read_text(),
    long_description_content_type="text/markdown",
)
