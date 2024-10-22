import os
from pathlib import Path

from setuptools import find_packages, setup  # type: ignore

version = os.environ.get("RELEASE_VERSION", "0.0.1")


def read_requirements():
    with open("requirements.txt") as req:
        return req.read().splitlines()


setup(
    name="rated_exporter_sdk",
    version=version,
    python_requires=">=3.6",
    install_requires=read_requirements(),
    package_dir={"": "src"},
    packages=find_packages(where="rated_exporter_sdk", exclude=["tests", "tests.*"]),
    long_description=Path("README.md").read_text(),
    long_description_content_type="text/markdown",
)
