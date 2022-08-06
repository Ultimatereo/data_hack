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


import importlib
import os
import json
from pprint import pprint

from fields import *
from generator import *


def get_table_class(path, class_name):
    m = importlib.import_module(path)
    c = getattr(m, class_name)
    return c


def load_config(tables: dict, config_path="config.json"):
    try:
        with open(config_path, "r") as config:
            apply_changes(tables, json.load(config))
    except FileNotFoundError:
        print("Config not found")
    except Exception:
        print("Error during applying config")


def apply_changes(tables: dict, changes: dict):
    for table_name in tables:
        if table_name not in changes:
            continue
        table = tables.get(table_name)
        table_changes = changes.get(table_name)
        for field_name in table_changes:
            setattr(table, field_name, getattr(table, field_name).apply_changes(table_changes.get(field_name)))


# init tables
tables = {}

table_script = "CellClass"
table_class_name = "Cell"
table_class = get_table_class(table_script, table_class_name)
tables[table_class_name] = table_class()

# load config
load_config(tables)

# generate data
data = generator(tables)
pprint(data)

"""
a = Cell()

b = Cell2()

print(generate_paired(a, b, ['float3', 'integer4']))
print(generate(a))
print(generate(b))
print("_______")
# setattr(a, 'integer3', getattr(a, 'integer3').set_range(10, 20))

load_config(a, "Cell.json")

print("_______")
print(generate_paired(a, b, ['float3', 'integer4']))
print(generate(a))

"""
