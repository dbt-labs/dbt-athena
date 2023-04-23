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

dbt = dbtObj(spark.table)
df = model(dbt, spark)
materialize(spark, df, dbt.this)
{%- endmacro -%}

{%- macro athena__py_execute_query(query) -%}
def execute_query(spark_session):
    import pandas
    try:
        spark_session.sql("""{{ query }}""")
        return "OK"
    except Exception:
        raise

dbt = dbtObj(spark.table)
execute_query(spark)
{%- endmacro -%}
