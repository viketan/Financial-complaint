import os
from pyspark.sql import SparkSession


#spark_session = SparkSession.builder.master('local[*]').appName('finance_complaint') .getOrCreate()
spark_session = SparkSession.builder \
    .appName("finance_complaint") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.instances", "2") \
    .config("spark.executor.cores", "2") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()