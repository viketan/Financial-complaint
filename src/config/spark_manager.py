import os
from pyspark.sql import SparkSession


#spark_session = SparkSession.builder.master('local[*]').appName('finance_complaint') .getOrCreate()
spark_session = SparkSession.builder \
    .appName("finance_complaint") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.instances", "4") \
    .config("spark.executor.cores", "4") \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()