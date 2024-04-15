#!/usr/bin/env python
import os
import re
from typing import Any, Dict

from setuptools import find_namespace_packages, setup

this_directory = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(this_directory, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

package_name = "dbt-athena-community"


# get version from a separate file
def _get_plugin_version_dict() -> Dict[str, Any]:
    _version_path = os.path.join(this_directory, "dbt", "adapters", "athena", "__version__.py")
    _semver = r"""(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)"""
    _pre = r"""((?P<prekind>a|b|rc)(?P<pre>\d+))?"""
    _version_pattern = rf"""version\s*=\s*["']{_semver}{_pre}["']"""
    with open(_version_path) as f:
        match = re.search(_version_pattern, f.read().strip())
        if match is None:
            raise ValueError(f"invalid version at {_version_path}")
        return match.groupdict()


def _get_package_version() -> str:
    parts = _get_plugin_version_dict()
    version = "{major}.{minor}.{patch}".format(**parts)
    if parts["prekind"] and parts["pre"]:
        version += parts["prekind"] + parts["pre"]
    return version


description = "The athena adapter plugin for dbt (data build tool)"


setup(
    name=package_name,
    version=_get_package_version(),
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    platforms="any",
    license="Apache License 2.0",
    license_files=("LICENSE.txt",),
    url="https://github.com/dbt-athena/dbt-athena",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    install_requires=[
        "dbt-common>=1.0.0b2,<2.0",
        "dbt-adapters>=1.0.0b2,<2.0",
        "boto3>=1.28",
        "boto3-stubs[athena,glue,lakeformation,sts]>=1.28",
        "pyathena>=2.25,<4.0",
        "mmh3>=4.0.1,<4.2.0",
        "pydantic>=1.10,<3.0",
        "tenacity~=8.2",
    ],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    python_requires=">=3.8,<3.13",
)
