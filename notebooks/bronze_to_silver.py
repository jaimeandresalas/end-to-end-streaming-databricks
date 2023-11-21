# Databricks notebook source
from pyspark.sql import SparkSession
from src.classes.DataCleaning import DataProcessing
spark = (SparkSession.builder
         .appName("brz-to-silver")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# COMMAND ----------

base_path = '/mnt/data/bronze/sql_server/SalesLT/'
DataProcessing(spark=spark,base_path = base_path, dbutils=dbutils).bronze_to_silver()

# COMMAND ----------

processing.bronze_to_silver()

# COMMAND ----------


