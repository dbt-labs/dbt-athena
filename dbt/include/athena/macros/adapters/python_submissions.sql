{%- macro athena__py_save_table_as(compiled_code, target_relation, format, location, mode="overwrite") -%}
{{ compiled_code }}
def materialize(spark_session, df, target_relation):
    import pandas
    try:
        if isinstance(df, pandas.core.frame.DataFrame):
            df = spark_session.createDataFrame(df)
        df.write \
        .format("{{ format }}") \
        .option("path", "{{ location }}") \
        .mode("{{ mode }}") \
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
