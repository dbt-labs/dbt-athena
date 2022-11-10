#!/usr/bin/env python
from setuptools import find_namespace_packages, setup
import os


this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md')) as f:
    long_description = f.read()


package_name = "dbt-athena-community"

dbt_version = "1.0"
package_version = "1.0.4"
description = """The athena adapter plugin for dbt (data build tool)"""

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
    url="https://github.com/dbt-athena/dbt-athena",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    install_requires=[
        "dbt-core>=1.0.1",
        "pyathena>=2.2.0",
        "boto3>=1.18.12",
        "tenacity>=6.3.1",
    ]
)
