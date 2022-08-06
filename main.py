from pyspark.sql import SparkSession
from data import *
import os
import json

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('PySpark_Tutorial2') \
    .getOrCreate()

a = Cell()

rows = []
for i in range(100):
    rows.append(generate(a).values())

df = spark.createDataFrame(rows, tuple(list(fields_names(a))))

df.show()