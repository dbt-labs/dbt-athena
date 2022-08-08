#!/usr/bin/env python
from setuptools import find_namespace_packages, setup
import os
import re


this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md')) as f:
    long_description = f.read()


package_name = "dbt-athena-adapter"


# get this from a separate file
def _dbt_athena_version() -> str:
    _version_path = os.path.join(
        this_directory, "dbt", "adapters", "athena", "__version__.py"
    )
    _version_pattern = r"version\s*=\s*[\"'](.+)[\"']"
    with open(_version_path) as f:
        match = re.search(_version_pattern, f.read().strip())
        if match is None:
            raise ValueError(f'invalid version at {_version_path}')
        return match.group(1)


package_version = _dbt_athena_version()
description = """The athena adapter plugin for dbt (data build tool)"""


dbt_version = "1.0"

if not package_version.startswith(dbt_version):
    raise ValueError(
        f"Invalid setup.py: package_version={package_version} must start with "
        f"dbt_version={dbt_version}"
    )


setup(
    name=package_name,
    version=package_version,

    description=description,
    long_description=long_description,
    long_description_content_type='text/markdown',

    author="Thomas Elvey",
    author_email="tomelvey@googlemail.com",
    url="https://github.com/Tomme/dbt-athena",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    install_requires=[
        "dbt-core>=1.0.1",
        "pyathena>=2.2.0",
        "boto3>=1.18.12",
        "tenacity>=6.3.1",
    ]
)
