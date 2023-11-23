# Databricks notebook source
from pyspark.sql import SparkSession
from src.classes.DataCleaning import DataProcessing
spark = (SparkSession.builder
         .appName("silver-to-gold")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# COMMAND ----------

dbutils.widgets.text(name="base_path", defaultValue = '/mnt/data/bronze/sql_server/SalesLT/', label = 'base_path')
base_path = dbutils.widgets.get(name="base_path")

# COMMAND ----------

processing = DataProcessing(spark=spark,base_path = base_path, dbutils=dbutils)
processing.silver_to_gold()
