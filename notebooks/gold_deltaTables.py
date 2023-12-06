# Databricks notebook source
from pyspark.sql import SparkSession
spark = (
    SparkSession.builder
    .appName("delta-tables")
    .getOrCreate()
)

# COMMAND ----------

def parquet_to_deltaTable(schema_name):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    tables_name = dbutils.fs.ls(f'/mnt/data/{schema_name}/sql_server/SalesLT/')
    for table in tables_name:
        url = f'/mnt/data/{schema_name}/sql_server/SalesLT/' + table.name
        schema = schema_name  + '.'
        table_name = schema + table.name.split('/')[0].lower()
        spark.sql(f"DROP TABLE IF EXISTS {table_name};")
        df = spark.read.load(url)
        df.write.saveAsTable(table_name)

# COMMAND ----------

parquet_to_deltaTable('silver')

# COMMAND ----------

parquet_to_deltaTable('gold')

# COMMAND ----------


