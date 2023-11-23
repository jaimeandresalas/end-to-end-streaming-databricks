# Databricks notebook source
from pyspark.sql import SparkSession
from src.classes.DataCleaning import DataProcessing
spark = (SparkSession.builder
         .appName("brz-to-silver")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# COMMAND ----------

dbutils.fs.ls('/mnt/data/bronze/sql_server/SalesLT/')

# COMMAND ----------

dbutils.fs.ls('dbfs:/mnt/data/bronze/sql_server/SalesLT/Address/2023-11-19T02:47:35.0844257Z /')

# COMMAND ----------

base_path = '/mnt/data/bronze/sql_server/SalesLT/'
processing = DataProcessing(spark=spark,base_path = base_path, dbutils=dbutils)

# COMMAND ----------

processing.bronze_to_silver()

# COMMAND ----------

from datetime import datetime
#datetime.now().strftime("yyyy-mm-dd")
datetime.now().strftime('%Y-%m-%d')

# COMMAND ----------


