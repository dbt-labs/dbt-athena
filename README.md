# dbt-athena

* Supports dbt version `0.19.0`
* Supports [Seeds][seeds]
* Correctly detects views and their columns
* **Experimental** support for [incremental models][incremental]
  * **Only** inserts
  * Does **not** support the use of `unique_key`
* **Only** supports Athena engine 2
  * [Changing Athena Engine Versions][engine-change]

[seeds]: https://docs.getdbt.com/docs/building-a-dbt-project/seeds
[incremental]: https://docs.getdbt.com/docs/building-a-dbt-project/building-models/configuring-incremental-models
[engine-change]: https://docs.aws.amazon.com/athena/latest/ug/engine-versions-changing.html

### Installation

`pip install git+https://github.com/Tomme/dbt-athena.git`

### Configuring your profile

A dbt profile can be configured to run against AWS Athena using the following configuration:

| Option         	| Description                                               	| Required? 	| Example            	|
|----------------	|-----------------------------------------------------------	|-----------	|--------------------	|
| s3_staging_dir 	| S3 location to store Athena query results and metadata    	| Required  	| `s3://bucket/dbt/` 	|
| region_name    	| AWS region of your Athena instance                        	| Required  	| `eu-west-1`        	|
| schema         	| Specify the schema (Athena database) to build models into 	| Required  	| `dbt`              	|
| database       	| Specify the database (Data catalog) to build models into  	| Required  	| `awsdatacatalog`   	|

**Example profiles.yml entry:**
```yaml
athena:
  target: dev
  outputs:
    dev:
      type: athena
      s3_staging_dir: s3://athena-query-results/dbt/
      region_name: eu-west-1
      schema: dbt
      database: awsdatacatalog
```

_Additional information_
* `threads` is supported
* `database` and `catalog` can be used interchangeably

### Usage notes

### Models

#### Table Configuration

* `external_location` (`default=none`)
  * The location where Athena saves your table in Amazon S3
  * If `none` then it will default to `{s3_staging_dir}/tables`
* `partitioned_by` (`default=none`)
  * An array list of columns by which the table will be partitioned
  * Limited to creation of 100 partitions (_currently_)
* `bucketed_by` (`default=none`)
  * An array list of columns to bucket data
* `bucket_count` (`default=none`)
  * The number of buckets for bucketing your data
* `format` (`default='parquet'`)
  * The data format for the table
  * Supports `ORC`, `PARQUET`, `AVRO`, `JSON`, or `TEXTFILE`
* `field_delimiter` (`default=none`)
  * Custom field delimiter, for when format is set to `TEXTFILE`
  
More information: [CREATE TABLE AS][create-table-as]

[create-table-as]: https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html

#### Supported functionality

**Experimental** support for incremental models.
* **Only** inserts
* Does **not** support the use of `unique_key`

Due to the nature of AWS Athena, not all core dbt functionality is supported.
The following features of dbt are not implemented on Athena:
* Snapshots

#### Known issues

* Quoting is not currently supported
* **Only** supports Athena engine 2
  * [Changing Athena Engine Versions][engine-change]

### Running tests

#### `pytest-dbt-adapter`

* `pip install -r dev_requirements.txt`
* Set required environmental variables
  * `DBT_TEST_ATHENA_S3_STAGING_DIR`
  * `DBT_TEST_ATHENA_REGION`
  * `DBT_TEST_ATHENA_DATABASE`
  * `DBT_TEST_ATHENA_SCHEMA`
* `pytest test/integration/athena.dbtspec`

### Community

* [fishtown-analytics/dbt][fishtown-analytics/dbt]
* [fishtown-analytics/dbt-presto][fishtown-analytics/dbt-presto]
* [Dandandan/dbt-athena][Dandandan/dbt-athena]
* [laughingman7743/PyAthena][laughingman7743/PyAthena]

[fishtown-analytics/dbt]: https://github.com/fishtown-analytics/dbt
[fishtown-analytics/dbt-presto]: https://github.com/fishtown-analytics/dbt-presto
[Dandandan/dbt-athena]: https://github.com/Dandandan/dbt-athena
[laughingman7743/PyAthena]: https://github.com/laughingman7743/PyAthena
