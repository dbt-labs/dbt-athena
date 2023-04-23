{%- macro athena__py_save_table_as(compiled_code, target_relation , **kwargs) -%}
    {% set location = kwargs.get("location") %}
    {% set format = kwargs.get("format", "parquet") %}
    {% set mode = kwargs.get("mode", "overwrite") %}
    {% set write_compression = kwargs.get("write_compression", "snappy") %}
    {% set partitioned_by = kwargs.get("partitioned_by") %}
    {% set bucketed_by = kwargs.get("bucketed_by") %}
    {% set sorted_by = kwargs.get("sorted_by") %}
    {% set merge_schema = kwargs.get("merge_schema", true) %}
    {% set bucket_count = kwargs.get("bucket_count") %}
    {% set field_delimiter = kwargs.get("field_delimiter") %}
    {% set table_type = kwargs.get("table_type") %}
    {% set extra_table_properties = kwargs.get("extra_table_properties") %}

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
        df.write \
        .format("{{ format }}") \
        .mode("{{ mode }}") \
        .partitionBy("{{ partitioned_by }}") \
        .bucketBy("{{ bucketed_by }}") \
        .sortBy(" {{ sorted_by }}") \
        .option("path", "{{ location }}") \
        .option("compression", "{{ write_compression }}") \
        .option("mergeSchema", "{{ merge_schema }}") \
        .option("numBuckets", "{{ bucket_count }}") \
        .option("delimiter", "{{ field_delimiter }}") \
        .option("tableType", "{{ table_type }}") \
        .option("extra", "{{ extra_table_properties }}") \
        .saveAsTable(
            name="{{ target_relation.schema}}.{{ target_relation.identifier }}",
        )
        return "OK"
    except Exception:
        raise

{{ athena__py_get_spark_dbt_object() }}

dbt = SparkdbtObj(spark)
df = model(dbt, dbt.spark_session)
materialize(dbt.spark_session, df, dbt.this)
{%- endmacro -%}

{%- macro athena__py_execute_query(query) -%}
{{ athena__py_get_spark_dbt_object() }}

def execute_query(spark_session):
    import pandas
    try:
        spark_session.sql("""{{ query }}""")
        return "OK"
    except Exception:
        raise

dbt = SparkdbtObj(spark)
execute_query(dbt, dbt.spark_session)
{%- endmacro -%}

{%- macro athena__py_get_spark_dbt_object() -%}
class SparkdbtObj(dbtObj, spark_session):
    def __init__(self) -> None:
        super().__init__(load_df_function=spark.table)
        self.spark_session = spark_session
        
    @property
    def source(self, *args):
        """
        Override the source attribute dynamically

        spark.table('awsdatacatalog.analytics_dev.model')
        Raises pyspark.sql.utils.AnalysisException:
        spark_catalog requires a single-part namespace,
        but got [awsdatacatalog, analytics_dev]

        So the override removes the catalog component and only
        provides the schema and identifer to spark.table()
        """
        args = [arg[arg.index('.')+1:] for arg in args]
        return lambda *args: source(*args, dbt_load_df_function=spark.table)
    
    @property
    def ref(self, *args):
        """
        Override the ref attribute dynamically

        spark.table('awsdatacatalog.analytics_dev.model')
        Raises pyspark.sql.utils.AnalysisException:
        spark_catalog requires a single-part namespace,
        but got [awsdatacatalog, analytics_dev]

        So the override removes the catalog component and only
        provides the schema and identifer to spark.table()
        """
        args = [arg[arg.index('.')+1:] for arg in args]
        return lambda *args: ref(*args, dbt_load_df_function=spark.table)

{%- endmacro -%}