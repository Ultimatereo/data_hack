from pyspark.sql import SparkSession
import importlib
from pprint import pprint
from fields import *
from generator import *
import json

DATA_TYPES = {"csv", "json", "parquet"}
EXPORT_MODES = {"append", "overwrite", "ignore"}

spark = SparkSession.builder.appName('Data_hack').getOrCreate()


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
    except FileNotFoundError as e:
        print(f"Config '{config_path}' not found")
        print(e)
    except Exception as e:
        print(f"Error during applying config '{config_path}' to table '{table_class_name}'")
        print(e)
        raise Exception("Can't continue working")


def apply_changes(table, table_class_name, changes: dict):
    if table_class_name not in changes:
        return
    table_changes = changes.get(table_class_name)
    for field_name in table_changes:
        if field_name not in all_fields_names(table):
            raise Exception(f"Found config for unknown field '{field_name}'")
        setattr(table, field_name, getattr(table, field_name).apply_changes(table_changes.get(field_name)))


def export_dataframe(df, path, export_type, export_mode):
    if export_type not in DATA_TYPES:
        raise Exception(f"Unknown export_type '{export_type}'. It must be one of {DATA_TYPES}")
    if export_mode not in EXPORT_MODES:
        raise Exception(f"Unknown export_mode '{export_mode}'. It must be one of {EXPORT_MODES}")
    df.write.format(export_type).mode(export_mode).save(path)


def solo_generate(table_script_path, table_class_name, export_path='output', export_type='parquet',
                  export_mode='overwrite'):
    table = load_table(table_script_path, table_class_name)
    load_config(table, table_class_name)
    data = generator(table)
    rdd = spark.sparkContext.parallelize(data)
    df = rdd.toDF(list(fields_names(table)))
    export_dataframe(df, export_path, export_type, export_mode)


def intersect_generate(table1_script_path, table1_class_name,
                       table2_script_path, table2_class_name,
                       intersect_keys: dict,
                       export_path1='output1', export_path2='output2',
                       export_type='parquet', export_mode='overwrite'):
    table1 = load_table(table1_script_path, table1_class_name)
    table2 = load_table(table2_script_path, table2_class_name)
    load_config(table1, table1_class_name)
    load_config(table2, table2_class_name)
    data1, data2 = paired_generator(table1, table2, intersect_keys)
    rdd = spark.sparkContext.parallelize(data1)
    df1 = rdd.toDF(list(fields_names(table1)))
    export_dataframe(df1, export_path1, export_type, export_mode)
    rdd = spark.sparkContext.parallelize(data2)
    df2 = rdd.toDF(list(fields_names(table2)))
    export_dataframe(df2, export_path2, export_type, export_mode)


def show_data(path, data_type="parquet", count=20):
    if data_type not in DATA_TYPES:
        raise Exception(f"Unknown data_type '{data_type}'. It must be one of {DATA_TYPES}")
    df = spark.read.format(data_type).load(path)
    df.show(count)
    print(df.count())
    df.printSchema()


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
    try:
        # solo_generate("CellClass", "Cell", "CellGenCSV", "csv")
        intersect_generate("CellClass", "Cell",
                           "Cell2Class", "Cell2",
                           {"integer1": "iid", "float2": "acceleration", "abcword": "word"},
                           export_type="parquet")
        # show_data("CellGen", "parquet")
        show_data("output1", "parquet")
        show_data("output2", "parquet")
    except Exception as e:
        print(e)
