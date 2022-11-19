#!/usr/bin/env python
import os

from setuptools import find_namespace_packages, setup

this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, "README.md")) as f:
    long_description = f.read()


package_name = "dbt-athena-community"

dbt_version = "1.3"
package_version = "1.3.1"
description = "The athena adapter plugin for dbt (data build tool)"

if not package_version.startswith(dbt_version):
    raise ValueError(f"Invalid setup.py: package_version={package_version} must start with dbt_version={dbt_version}")


setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/dbt-athena/dbt-athena",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    install_requires=[
        # In order to control dbt-core version and package version
        "dbt-core~=1.3.0",
        "pyathena~=2.14",
        "boto3~=1.26",
        "tenacity~=8.1",
    ],
)
