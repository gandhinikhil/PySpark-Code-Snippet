# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Connection to DB").getOrCreate()
url = "jdbc:postgresql://localhost:5432/Sample_db"
properties = {
    "user": "postgres",
    "password": "Admin123",
    "driver": "org.postgresql.Driver"
}
table_name = "states"
df = spark.read \
    .jdbc(url=url, table=table_name, properties=properties)
df.show(10)

# COMMAND ----------


