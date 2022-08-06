from pyspark.sql import SparkSession
import importlib
import json
from pprint import pprint
from fields import *
from generator import *


def get_table_class(path, class_name):
    m = importlib.import_module(path)
    c = getattr(m, class_name)
    return c


def load_table(table_script: str, table_class_name: str):
    table_class = get_table_class(table_script, table_class_name)
    return table_class()


def load_config(table, table_class_name: str, config_path="config.json"):
    try:
        with open(config_path, "r") as config:
            apply_changes(table, table_class_name, json.load(config))
    except FileNotFoundError:
        print("Config not found")
    except Exception:
        print("Error during applying config")


def apply_changes(table, table_class_name, changes: dict):
    if table_class_name not in changes:
        return
    table_changes = changes.get(table_class_name)
    for field_name in table_changes:
        setattr(table, field_name, getattr(table, field_name).apply_changes(table_changes.get(field_name)))


def solo_generate():
    table = load_table("CellClass", "Cell")
    load_config(table, "Cell")
    data = generator(table)
    spark = SparkSession.builder.appName('Data_hack').getOrCreate()
    rdd = spark.sparkContext.parallelize(data)
    df = rdd.toDF(list(fields_names(table)))
    df.write.parquet("parq")


def show_data():
    spark = SparkSession.builder.appName('Data_hack').getOrCreate()
    df = spark.read.parquet("parq1")
    df.show()
    print(df.count())

    df = spark.read.parquet("parq2")
    df.show()
    print(df.count())


def intersect_generate():
    table1 = load_table("CellClass", "Cell")
    table2 = load_table("Cell2Class", "Cell2")
    load_config(table1, "Cell")
    load_config(table2, "Cell2")
    data1, data2 = paired_generator(table1, table2, {"integer1": "iid", "float2": "acceleration", "abcword": "word"})
    spark = SparkSession.builder.appName('Data_hack').getOrCreate()
    rdd = spark.sparkContext.parallelize(data1)
    df = rdd.toDF(list(fields_names(table1)))
    df.write.parquet("parq1")
    rdd = spark.sparkContext.parallelize(data2)
    df = rdd.toDF(list(fields_names(table2)))
    df.write.parquet("parq2")
def play_around():
    def validate(date_text):
        try:
            datetime.strptime(date_text, '%Y-%m-%d')
            return True
        except ValueError:
            raise ValueError("Incorrect data format, should be YYYY-MM-DD")

    d = datetime(2022, 11, 28, 11, 30)
    print(d)
    print(d.date())
    dt = date(year=2022, month=11, day=28)
    print(dt)
    # print(dt.timestamp)
    dt2 = date(year=2021, month=11, day=23)
    dt3 = datetime(2021, 11, 23, 23, 59)
    print(dt < dt2)
    print(date.today())
    fake = Faker()
    print(fake.date_between(dt2, dt))
    print(validate(str(date.today())))
    print(dt3.date())
if __name__ == '__main__':
    show_data()
    # solo_generate()
    # intersect_generate()
    # play_around()