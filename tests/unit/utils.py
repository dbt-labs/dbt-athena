import os
import string
from typing import Optional

import agate
import boto3

from dbt.adapters.athena.utils import AthenaCatalogType
from dbt.config.project import PartialProject

from .constants import AWS_REGION, BUCKET, CATALOG_ID, DATA_CATALOG_NAME, DATABASE_NAME


class Obj:
    which = "blah"
    single_threaded = False


def profile_from_dict(profile, profile_name, cli_vars="{}"):
    from dbt.config import Profile
    from dbt.config.renderer import ProfileRenderer
    from dbt.config.utils import parse_cli_vars

    if not isinstance(cli_vars, dict):
        cli_vars = parse_cli_vars(cli_vars)

    renderer = ProfileRenderer(cli_vars)

    # in order to call dbt's internal profile rendering, we need to set the
    # flags global. This is a bit of a hack, but it's the best way to do it.
    from argparse import Namespace

    from dbt.flags import set_from_args

    set_from_args(Namespace(), None)
    return Profile.from_raw_profile_info(
        profile,
        profile_name,
        renderer,
    )


def project_from_dict(project, profile, packages=None, selectors=None, cli_vars="{}"):
    from dbt.config.renderer import DbtProjectYamlRenderer
    from dbt.config.utils import parse_cli_vars

    if not isinstance(cli_vars, dict):
        cli_vars = parse_cli_vars(cli_vars)

    renderer = DbtProjectYamlRenderer(profile, cli_vars)

    project_root = project.pop("project-root", os.getcwd())

    partial = PartialProject.from_dicts(
        project_root=project_root,
        project_dict=project,
        packages_dict=packages,
        selectors_dict=selectors,
    )
    return partial.render(renderer)


def config_from_parts_or_dicts(project, profile, packages=None, selectors=None, cli_vars="{}"):
    from copy import deepcopy

    from dbt.config import Profile, Project, RuntimeConfig
    from dbt.config.utils import parse_cli_vars

    if not isinstance(cli_vars, dict):
        cli_vars = parse_cli_vars(cli_vars)

    if isinstance(project, Project):
        profile_name = project.profile_name
    else:
        profile_name = project.get("profile")

    if not isinstance(profile, Profile):
        profile = profile_from_dict(
            deepcopy(profile),
            profile_name,
            cli_vars,
        )

    if not isinstance(project, Project):
        project = project_from_dict(
            deepcopy(project),
            profile,
            packages,
            selectors,
            cli_vars,
        )

    args = Obj()
    args.vars = cli_vars
    args.profile_dir = "/dev/null"
    return RuntimeConfig.from_parts(project=project, profile=profile, args=args)


def inject_plugin(plugin):
    from dbt.adapters.factory import FACTORY

    key = plugin.adapter.type()
    FACTORY.plugins[key] = plugin


def inject_adapter(value, plugin):
    """Inject the given adapter into the adapter factory, so your hand-crafted
    artisanal adapter will be available from get_adapter() as if dbt loaded it.
    """
    inject_plugin(plugin)
    from dbt.adapters.factory import FACTORY

    key = value.type()
    FACTORY.adapters[key] = value


def clear_plugin(plugin):
    from dbt.adapters.factory import FACTORY

    key = plugin.adapter.type()
    FACTORY.plugins.pop(key, None)
    FACTORY.adapters.pop(key, None)


class TestAdapterConversions:
    def _get_tester_for(self, column_type):
        from dbt_common.clients import agate_helper

        if column_type is agate.TimeDelta:  # dbt never makes this!
            return agate.TimeDelta()

        for instance in agate_helper.DEFAULT_TYPE_TESTER._possible_types:
            if isinstance(instance, column_type):  # include child types
                return instance

        raise ValueError(f"no tester for {column_type}")

    def _make_table_of(self, rows, column_types):
        column_names = list(string.ascii_letters[: len(rows[0])])
        if isinstance(column_types, type):
            column_types = [self._get_tester_for(column_types) for _ in column_names]
        else:
            column_types = [self._get_tester_for(typ) for typ in column_types]
        table = agate.Table(rows, column_names=column_names, column_types=column_types)
        return table


class MockAWSService:
    def create_data_catalog(
        self,
        catalog_name: str = DATA_CATALOG_NAME,
        catalog_type: AthenaCatalogType = AthenaCatalogType.GLUE,
        catalog_id: str = CATALOG_ID,
    ):
        athena = boto3.client("athena", region_name=AWS_REGION)
        parameters = {}
        if catalog_type == AthenaCatalogType.GLUE:
            parameters = {"catalog-id": catalog_id}
        else:
            parameters = {"catalog": catalog_name}
        athena.create_data_catalog(Name=catalog_name, Type=catalog_type.value, Parameters=parameters)

    def create_database(self, name: str = DATABASE_NAME, catalog_id: str = CATALOG_ID):
        glue = boto3.client("glue", region_name=AWS_REGION)
        glue.create_database(DatabaseInput={"Name": name}, CatalogId=catalog_id)

    def create_view(self, view_name: str):
        glue = boto3.client("glue", region_name=AWS_REGION)
        glue.create_table(
            DatabaseName=DATABASE_NAME,
            TableInput={
                "Name": view_name,
                "StorageDescriptor": {
                    "Columns": [
                        {
                            "Name": "id",
                            "Type": "string",
                        },
                        {
                            "Name": "country",
                            "Type": "string",
                        },
                        {
                            "Name": "dt",
                            "Type": "date",
                        },
                    ],
                    "Location": "",
                },
                "TableType": "VIRTUAL_VIEW",
                "Parameters": {
                    "TableOwner": "John Doe",
                },
            },
        )

    def create_table(
        self,
        table_name: str,
        database_name: str = DATABASE_NAME,
        catalog_id: str = CATALOG_ID,
        location: Optional[str] = "auto",
    ):
        glue = boto3.client("glue", region_name=AWS_REGION)
        if location == "auto":
            location = f"s3://{BUCKET}/tables/{table_name}"
        glue.create_table(
            CatalogId=catalog_id,
            DatabaseName=database_name,
            TableInput={
                "Name": table_name,
                "StorageDescriptor": {
                    "Columns": [
                        {
                            "Name": "id",
                            "Type": "string",
                        },
                        {
                            "Name": "country",
                            "Type": "string",
                        },
                    ],
                    "Location": location,
                },
                "PartitionKeys": [
                    {
                        "Name": "dt",
                        "Type": "date",
                    },
                ],
                "TableType": "table",
                "Parameters": {
                    "compressionType": "snappy",
                    "classification": "parquet",
                    "projection.enabled": "false",
                    "typeOfData": "file",
                },
            },
        )

    def create_table_without_type(self, table_name: str, database_name: str = DATABASE_NAME):
        glue = boto3.client("glue", region_name=AWS_REGION)
        glue.create_table(
            DatabaseName=database_name,
            TableInput={
                "Name": table_name,
                "StorageDescriptor": {
                    "Columns": [
                        {
                            "Name": "id",
                            "Type": "string",
                        },
                        {
                            "Name": "country",
                            "Type": "string",
                        },
                    ],
                    "Location": f"s3://{BUCKET}/tables/{table_name}",
                },
                "Parameters": {
                    "compressionType": "snappy",
                    "classification": "parquet",
                    "projection.enabled": "false",
                    "typeOfData": "file",
                },
            },
        )

    def create_table_without_partitions(self, table_name: str):
        glue = boto3.client("glue", region_name=AWS_REGION)
        glue.create_table(
            DatabaseName=DATABASE_NAME,
            TableInput={
                "Name": table_name,
                "StorageDescriptor": {
                    "Columns": [
                        {
                            "Name": "id",
                            "Type": "string",
                        },
                        {
                            "Name": "country",
                            "Type": "string",
                        },
                        {
                            "Name": "dt",
                            "Type": "date",
                        },
                    ],
                    "Location": f"s3://{BUCKET}/tables/{table_name}",
                },
                "PartitionKeys": [],
                "TableType": "table",
                "Parameters": {
                    "compressionType": "snappy",
                    "classification": "parquet",
                    "projection.enabled": "false",
                    "typeOfData": "file",
                },
            },
        )

    def create_iceberg_table(self, table_name: str):
        glue = boto3.client("glue", region_name=AWS_REGION)
        glue.create_table(
            DatabaseName=DATABASE_NAME,
            TableInput={
                "Name": table_name,
                "StorageDescriptor": {
                    "Columns": [
                        {
                            "Name": "id",
                            "Type": "string",
                        },
                        {
                            "Name": "country",
                            "Type": "string",
                        },
                        {
                            "Name": "dt",
                            "Type": "date",
                        },
                    ],
                    "Location": f"s3://{BUCKET}/tables/data/{table_name}",
                },
                "PartitionKeys": [
                    {
                        "Name": "dt",
                        "Type": "date",
                    },
                ],
                "TableType": "EXTERNAL_TABLE",
                "Parameters": {
                    "metadata_location": f"s3://{BUCKET}/tables/metadata/{table_name}/123.json",
                    "table_type": "ICEBERG",
                },
            },
        )

    def create_table_without_table_type(self, table_name: str):
        glue = boto3.client("glue", region_name=AWS_REGION)
        glue.create_table(
            DatabaseName=DATABASE_NAME,
            TableInput={
                "Name": table_name,
                "StorageDescriptor": {
                    "Columns": [
                        {
                            "Name": "id",
                            "Type": "string",
                        },
                    ],
                    "Location": f"s3://{BUCKET}/tables/{table_name}",
                },
                "Parameters": {
                    "TableOwner": "John Doe",
                },
            },
        )

    def create_work_group_with_output_location_enforced(self, work_group_name: str):
        athena = boto3.client("athena", region_name=AWS_REGION)
        athena.create_work_group(
            Name=work_group_name,
            Configuration={
                "ResultConfiguration": {
                    "OutputLocation": "s3://pre-configured-output-location/",
                },
                "EnforceWorkGroupConfiguration": True,
                "PublishCloudWatchMetricsEnabled": True,
                "EngineVersion": {
                    "SelectedEngineVersion": "Athena engine version 2",
                    "EffectiveEngineVersion": "Athena engine version 2",
                },
            },
        )

    def create_work_group_with_output_location_not_enforced(self, work_group_name: str):
        athena = boto3.client("athena", region_name=AWS_REGION)
        athena.create_work_group(
            Name=work_group_name,
            Configuration={
                "ResultConfiguration": {
                    "OutputLocation": "s3://pre-configured-output-location/",
                },
                "EnforceWorkGroupConfiguration": False,
                "PublishCloudWatchMetricsEnabled": True,
                "EngineVersion": {
                    "SelectedEngineVersion": "Athena engine version 2",
                    "EffectiveEngineVersion": "Athena engine version 2",
                },
            },
        )

    def create_work_group_no_output_location(self, work_group_name: str):
        athena = boto3.client("athena", region_name=AWS_REGION)
        athena.create_work_group(
            Name=work_group_name,
            Configuration={
                "EnforceWorkGroupConfiguration": True,
                "PublishCloudWatchMetricsEnabled": True,
                "EngineVersion": {
                    "SelectedEngineVersion": "Athena engine version 2",
                    "EffectiveEngineVersion": "Athena engine version 2",
                },
            },
        )

    def add_data_in_table(self, table_name: str):
        s3 = boto3.client("s3", region_name=AWS_REGION)
        s3.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": AWS_REGION})
        s3.put_object(Body=b"{}", Bucket=BUCKET, Key=f"tables/{table_name}/dt=2022-01-01/data1.parquet")
        s3.put_object(Body=b"{}", Bucket=BUCKET, Key=f"tables/{table_name}/dt=2022-01-01/data2.parquet")
        s3.put_object(Body=b"{}", Bucket=BUCKET, Key=f"tables/{table_name}/dt=2022-01-02/data.parquet")
        s3.put_object(Body=b"{}", Bucket=BUCKET, Key=f"tables/{table_name}/dt=2022-01-03/data1.parquet")
        s3.put_object(Body=b"{}", Bucket=BUCKET, Key=f"tables/{table_name}/dt=2022-01-03/data2.parquet")
        partition_input_list = [
            {
                "Values": [dt],
                "StorageDescriptor": {
                    "Columns": [
                        {
                            "Name": "id",
                            "Type": "string",
                        },
                        {
                            "Name": "country",
                            "Type": "string",
                        },
                        {
                            "Name": "dt",
                            "Type": "date",
                        },
                    ],
                    "Location": f"s3://{BUCKET}/tables/{table_name}/dt={dt}",
                },
            }
            for dt in ["2022-01-01", "2022-01-02", "2022-01-03"]
        ]
        glue = boto3.client("glue", region_name=AWS_REGION)
        glue.batch_create_partition(
            DatabaseName="test_dbt_athena", TableName=table_name, PartitionInputList=partition_input_list
        )

    def add_partitions_to_table(self, database, table_name):
        partition_input_list = [
            {
                "Values": [dt],
                "StorageDescriptor": {
                    "Columns": [
                        {
                            "Name": "id",
                            "Type": "string",
                        },
                        {
                            "Name": "country",
                            "Type": "string",
                        },
                        {
                            "Name": "dt",
                            "Type": "date",
                        },
                    ],
                    "Location": f"s3://{BUCKET}/tables/{table_name}/dt={dt}",
                },
                "Parameters": {"compressionType": "snappy", "classification": "parquet"},
            }
            for dt in [f"2022-01-{day:02d}" for day in range(1, 27)]
        ]
        glue = boto3.client("glue", region_name=AWS_REGION)
        glue.batch_create_partition(
            DatabaseName=database, TableName=table_name, PartitionInputList=partition_input_list
        )

    def add_table_version(self, database, table_name):
        glue = boto3.client("glue", region_name=AWS_REGION)
        table = glue.get_table(DatabaseName=database, Name=table_name).get("Table")
        new_table_version = {
            "Name": table_name,
            "StorageDescriptor": table["StorageDescriptor"],
            "PartitionKeys": table["PartitionKeys"],
            "TableType": table["TableType"],
            "Parameters": table["Parameters"],
        }
        glue.update_table(DatabaseName=database, TableInput=new_table_version)
