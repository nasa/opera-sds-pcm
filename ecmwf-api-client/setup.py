"""Dummy setup.py for allowing this module to find packages.
"""
from setuptools import setup, find_packages

# adaptation_path = "folder/"

setup(
    name="ecmwf_api_client",
    version="0.0.1",
    packages=find_packages(),
    install_requires=[],
    extras_require={}
)
