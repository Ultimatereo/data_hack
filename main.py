# from pyspark.sql import SparkSession
#
# spark = SparkSession.builder \
#     .master("local[*]") \
#     .appName('PySpark_Tutorial') \
#     .getOrCreate()
#
# data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
# data = range(20)
# rdd = spark.sparkContext.parallelize(data)
# rdd.saveAsTextFile("output2")
# rdd1, rdd2 = rdd.randomSplit([2, 3])
# print(rdd1.collect())
# print(rdd2.collect())
# df = spark.createDataFrame(data)
# df.show()
# rdd = spark.sparkContext.textFile("output2")
# print(rdd.collect())
# df = rdd.toDF()
# df.show()
# spark.table()


from data import *
import os
import json

a = Cell()

b = Cell2()

print(generate_paired(a, b, {'float3': 'float3', 'integer4': 'integer4', "selectList":"selectList"}))
print(generate(a))
print(generate(b))
print("_______")

load_config(a, "Cell.json")

print("_______")
print(generate_paired(a, b, {'float3': 'float3', 'integer4': 'integer4'}))
print(generate(a))
print(generate(b))
