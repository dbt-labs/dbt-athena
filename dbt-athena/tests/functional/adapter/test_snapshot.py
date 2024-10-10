"""
These are the test suites from `dbt.tests.adapter.basic.test_snapshot_check_cols`
and `dbt.tests.adapter.basic.test_snapshot_timestamp`, but slightly adapted.
The original test suites for snapshots didn't work out-of-the-box, because Athena
has no support for UPDATE statements on hive tables. This is required by those
tests to update the input seeds.

This file also includes test suites for the iceberg tables.
"""
import pytest

from dbt.tests.adapter.basic.files import (
    cc_all_snapshot_sql,
    cc_date_snapshot_sql,
    cc_name_snapshot_sql,
    seeds_added_csv,
    seeds_base_csv,
    ts_snapshot_sql,
)
from dbt.tests.util import relation_from_name, run_dbt


def check_relation_rows(project, snapshot_name, count):
    """Assert that the relation has the given number of rows"""
    relation = relation_from_name(project.adapter, snapshot_name)
    result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
    assert result[0] == count


def check_relation_columns(project, snapshot_name, count):
    """Assert that the relation has the given number of columns"""
    relation = relation_from_name(project.adapter, snapshot_name)
    result = project.run_sql(f"select * from {relation}", fetch="one")
    assert len(result) == count


seeds_altered_added_csv = """
11,Mateo,2014-09-08T17:04:27
12,Julian,2000-02-05T11:48:30
13,Gabriel,2001-07-11T07:32:52
14,Isaac,2002-11-25T03:22:28
15,Levi,2009-11-16T11:57:15
16,Elizabeth,2005-04-10T03:50:11
17,Grayson,2019-08-07T19:28:17
18,Dylan,2014-03-02T11:50:41
19,Jayden,2009-06-07T07:12:49
20,Luke,2003-12-06T21:42:18
""".lstrip()


seeds_altered_base_csv = """
id,name,some_date
1,Easton_updated,1981-05-20T06:46:51
2,Lillian_updated,1978-09-03T18:10:33
3,Jeremiah_updated,1982-03-11T03:59:51
4,Nolan_updated,1976-05-06T20:21:35
5,Hannah_updated,1982-06-23T05:41:26
6,Eleanor_updated,1991-08-10T23:12:21
7,Lily_updated,1971-03-29T14:58:02
8,Jonathan_updated,1988-02-26T02:55:24
9,Adrian_updated,1994-02-09T13:14:23
10,Nora_updated,1976-03-01T16:51:39
""".lstrip()


iceberg_cc_all_snapshot_sql = """
{% snapshot cc_all_snapshot %}
    {{ config(
        check_cols='all',
        unique_key='id',
        strategy='check',
        target_database=database,
        target_schema=schema,
        table_type='iceberg'
    ) }}
    select
        id,
        name,
        cast(some_date as timestamp(6)) as some_date
    from {{ ref(var('seed_name', 'base')) }}
{% endsnapshot %}
""".strip()


iceberg_cc_name_snapshot_sql = """
{% snapshot cc_name_snapshot %}
    {{ config(
        check_cols=['name'],
        unique_key='id',
        strategy='check',
        target_database=database,
        target_schema=schema,
        table_type='iceberg'
    ) }}
    select
        id,
        name,
        cast(some_date as timestamp(6)) as some_date
    from {{ ref(var('seed_name', 'base')) }}
{% endsnapshot %}
""".strip()


iceberg_cc_date_snapshot_sql = """
{% snapshot cc_date_snapshot %}
    {{ config(
        check_cols=['some_date'],
        unique_key='id',
        strategy='check',
        target_database=database,
        target_schema=schema,
        table_type='iceberg'
    ) }}
    select
        id,
        name,
        cast(some_date as timestamp(6)) as some_date
    from {{ ref(var('seed_name', 'base')) }}
{% endsnapshot %}
""".strip()


iceberg_ts_snapshot_sql = """
{% snapshot ts_snapshot %}
    {{ config(
        strategy='timestamp',
        unique_key='id',
        updated_at='some_date',
        target_database=database,
        target_schema=schema,
        table_type='iceberg',
    )}}
    select
        id,
        name,
        cast(some_date as timestamp(6)) as some_date
    from {{ ref(var('seed_name', 'base')) }}
{% endsnapshot %}
""".strip()


class TestSnapshotCheckColsHive:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
            "added.csv": seeds_added_csv,
            "updated_1.csv": seeds_base_csv + seeds_altered_added_csv,
            "updated_2.csv": seeds_altered_base_csv + seeds_altered_added_csv,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "hive_snapshot_strategy_check_cols"}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "cc_all_snapshot.sql": cc_all_snapshot_sql,
            "cc_date_snapshot.sql": cc_date_snapshot_sql,
            "cc_name_snapshot.sql": cc_name_snapshot_sql,
        }

    def test_snapshot_check_cols(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 4

        # snapshot command
        results = run_dbt(["snapshot"])
        for result in results:
            assert result.status == "success"

        check_relation_columns(project, "cc_all_snapshot", 7)
        check_relation_columns(project, "cc_name_snapshot", 7)
        check_relation_columns(project, "cc_date_snapshot", 7)

        # check rowcounts for all snapshots
        check_relation_rows(project, "cc_all_snapshot", 10)
        check_relation_rows(project, "cc_name_snapshot", 10)
        check_relation_rows(project, "cc_date_snapshot", 10)

        relation_from_name(project.adapter, "cc_all_snapshot")

        # point at the "added" seed so the snapshot sees 10 new rows
        results = run_dbt(["--no-partial-parse", "snapshot", "--vars", "seed_name: added"])
        for result in results:
            assert result.status == "success"

        # check rowcounts for all snapshots
        check_relation_rows(project, "cc_all_snapshot", 20)
        check_relation_rows(project, "cc_name_snapshot", 20)
        check_relation_rows(project, "cc_date_snapshot", 20)

        # re-run snapshots, using "updated_1"
        results = run_dbt(["snapshot", "--vars", "seed_name: updated_1"])
        for result in results:
            assert result.status == "success"

        # check rowcounts for all snapshots
        check_relation_rows(project, "cc_all_snapshot", 30)
        check_relation_rows(project, "cc_date_snapshot", 30)
        # unchanged: only the timestamp changed
        check_relation_rows(project, "cc_name_snapshot", 20)

        # re-run snapshots, using "updated_2"
        results = run_dbt(["snapshot", "--vars", "seed_name: updated_2"])
        for result in results:
            assert result.status == "success"

        # check rowcounts for all snapshots
        check_relation_rows(project, "cc_all_snapshot", 40)
        check_relation_rows(project, "cc_name_snapshot", 30)
        # does not see name updates
        check_relation_rows(project, "cc_date_snapshot", 30)


class TestSnapshotTimestampHive:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
            "added.csv": seeds_added_csv,
            "updated_1.csv": seeds_base_csv + seeds_altered_added_csv,
            "updated_2.csv": seeds_altered_base_csv + seeds_altered_added_csv,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "ts_snapshot.sql": ts_snapshot_sql,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "hive_snapshot_strategy_timestamp"}

    def test_snapshot_timestamp(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 4

        # snapshot command
        results = run_dbt(["snapshot"])
        assert len(results) == 1

        check_relation_columns(project, "ts_snapshot", 7)

        # snapshot has 10 rows
        check_relation_rows(project, "ts_snapshot", 10)

        # point at the "added" seed so the snapshot sees 10 new rows
        results = run_dbt(["snapshot", "--vars", "seed_name: added"])
        for result in results:
            assert result.status == "success"

        # snapshot now has 20 rows
        check_relation_rows(project, "ts_snapshot", 20)

        # re-run snapshots, using "updated_1"
        results = run_dbt(["snapshot", "--vars", "seed_name: updated_1"])
        for result in results:
            assert result.status == "success"

        # snapshot now has 30 rows
        check_relation_rows(project, "ts_snapshot", 30)

        # re-run snapshots, using "updated_2"
        results = run_dbt(["snapshot", "--vars", "seed_name: updated_2"])
        for result in results:
            assert result.status == "success"

        # snapshot still has 30 rows because timestamp not updated
        check_relation_rows(project, "ts_snapshot", 30)


class TestIcebergSnapshotTimestamp(TestSnapshotTimestampHive):
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "ts_snapshot.sql": iceberg_ts_snapshot_sql,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "iceberg_snapshot_strategy_timestamp"}


class TestIcebergSnapshotCheckCols(TestSnapshotCheckColsHive):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "iceberg_snapshot_strategy_check_cols"}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "cc_all_snapshot.sql": iceberg_cc_all_snapshot_sql,
            "cc_date_snapshot.sql": iceberg_cc_date_snapshot_sql,
            "cc_name_snapshot.sql": iceberg_cc_name_snapshot_sql,
        }
