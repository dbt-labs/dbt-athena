{%- macro athena__py_save_table_as(compiled_code, target_relation, optional_args={}) -%}
    {% set location = optional_args.get("location") %}
    {% set format = optional_args.get("format", "parquet") %}
    {% set mode = optional_args.get("mode", "overwrite") %}
    {% set write_compression = optional_args.get("write_compression", "snappy") %}
    {% set partitioned_by = optional_args.get("partitioned_by") %}
    {% set bucketed_by = optional_args.get("bucketed_by") %}
    {% set sorted_by = optional_args.get("sorted_by") %}
    {% set merge_schema = optional_args.get("merge_schema", true) %}
    {% set bucket_count = optional_args.get("bucket_count") %}
    {% set field_delimiter = optional_args.get("field_delimiter") %}

import pyspark


{{ compiled_code }}
def materialize(spark_session, df, target_relation):
    import pandas
    try:
        if isinstance(df, pyspark.sql.dataframe.DataFrame):
            pass
        elif isinstance(df, pandas.core.frame.DataFrame):
            df = spark_session.createDataFrame(df)
        else:
            msg = f"{type(df)} is not a supported type for dbt Python materialization"
            raise Exception(msg)
        writer = df.write \
        .format("{{ format }}") \
        .mode("{{ mode }}") \
        .option("path", "{{ location }}") \
        .option("compression", "{{ write_compression }}") \
        .option("mergeSchema", "{{ merge_schema }}") \
        .option("delimiter", "{{ field_delimiter }}")
        if {{ partitioned_by }} is not None:
            writer = writer.partitionBy({{ partitioned_by }})
        if {{ bucketed_by }} is not None:
            writer = writer.bucketBy({{ bucket_count }},{{ bucketed_by }})
        if {{ sorted_by }} is not None:
            writer = writer.sortBy({{ sorted_by }})
        writer.saveAsTable(
            name="{{ target_relation.schema}}.{{ target_relation.identifier }}",
        )
        return "OK"
    except Exception:
        raise

{{ athena__py_get_spark_dbt_object() }}

dbt = SparkdbtObj()
df = model(dbt, spark)
materialize(spark, df, dbt.this)
{%- endmacro -%}

{%- macro athena__py_execute_query(query) -%}
import pyspark 


{{ athena__py_get_spark_dbt_object() }}

def execute_query(spark_session):
    import pandas
    try:
        spark_session.sql("""{{ query }}""")
        return "OK"
    except Exception:
        raise

dbt = SparkdbtObj()
execute_query(spark)
{%- endmacro -%}

{%- macro athena__py_get_spark_dbt_object() -%}
def get_spark_df(identifier):
    """
    Override the arguments to ref and source dynamically

    spark.table('awsdatacatalog.analytics_dev.model')
    Raises pyspark.sql.utils.AnalysisException:
    spark_catalog requires a single-part namespace,
    but got [awsdatacatalog, analytics_dev]

    So the override removes the catalog component and only
    provides the schema and identifer to spark.table()
    """
    return spark.table(identifier.split(".", 1)[1])

class SparkdbtObj(dbtObj):
    def __init__(self):
        super().__init__(load_df_function=get_spark_df)
        self.source = lambda *args: source(*args, dbt_load_df_function=get_spark_df)
        self.ref = lambda *args: ref(*args, dbt_load_df_function=get_spark_df)

{%- endmacro -%}