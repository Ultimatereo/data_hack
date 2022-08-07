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
    if path.endswith(".py"):
        path = path[:-3]
    m = importlib.import_module("dataclass." + path)
    c = getattr(m, class_name)
    return c


def load_table(table_script: str, table_class_name: str):
    table_class = get_table_class(table_script, table_class_name)
    return table_class()


def load_table_config(table, table_class_name: str, config_path: str):
    if config_path is None:
        return
    try:
        with open(f"config/table/{config_path}", "r") as config:
            apply_changes(table, table_class_name, json.load(config))
    except FileNotFoundError as e:
        print(f"Config '{config_path}' not found")
        print(e)
        print("Continue with default settings...")
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


def export_dataframe(df, dir_name, export_type, export_mode):
    if export_type not in DATA_TYPES:
        raise Exception(f"Unknown export_type '{export_type}'. It must be one of {DATA_TYPES}")
    if export_mode not in EXPORT_MODES:
        raise Exception(f"Unknown export_mode '{export_mode}'. It must be one of {EXPORT_MODES}")
    df.write.format(export_type).mode(export_mode).save(dir_name)


def solo_generate(table_script_name, table_class_name, table_config, dir_name, export_type, export_mode):
    if dir_name is None:
        dir_name = "output"
    if export_type is None:
        export_type = "parquet"
    if export_mode is None:
        export_mode = "overwrite"
    table = load_table(table_script_name, table_class_name)
    load_table_config(table, table_class_name, table_config)
    data = generator(table)
    rdd = spark.sparkContext.parallelize(data)
    df = rdd.toDF(list(fields_names(table)))
    export_dataframe(df, dir_name, export_type, export_mode)


def intersect_generate(table1_script_name, table1_class_name,
                       table2_script_name, table2_class_name,
                       intersect_keys: dict,
                       table_config,
                       export_path1, export_path2,
                       export_type, export_mode):
    if export_path1 is None:
        export_path1 = "output1"
    if export_path2 is None:
        export_path2 = "output2"
    if export_type is None:
        export_type = "parquet"
    if export_mode is None:
        export_mode = "overwrite"

    table1 = load_table(table1_script_name, table1_class_name)
    table2 = load_table(table2_script_name, table2_class_name)
    load_table_config(table1, table1_class_name, table_config)
    load_table_config(table2, table2_class_name, table_config)
    data1, data2 = paired_generator(table1, table2, intersect_keys)
    rdd = spark.sparkContext.parallelize(data1)
    df1 = rdd.toDF(list(fields_names(table1)))
    export_dataframe(df1, export_path1, export_type, export_mode)
    rdd = spark.sparkContext.parallelize(data2)
    df2 = rdd.toDF(list(fields_names(table2)))
    export_dataframe(df2, export_path2, export_type, export_mode)


def show_data(dir_name, data_type, count):
    if count is None:
        count = 20
    if data_type not in DATA_TYPES:
        raise Exception(f"Unknown data_type '{data_type}'. It must be one of {DATA_TYPES}")
    df = spark.read.format(data_type).load(dir_name)
    df.show(count)
    print("Total rows:", df.count())
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


def make_task(task: dict):
    def check_required_args(names: List[str], args: dict):
        for name in names:
            if name not in args:
                raise Exception(f"You must give '{name}' in 'args'")

    def make_show_data(args: dict):
        check_required_args(["dir_name", "data_type"], args)
        dir_name = args.get("dir_name")
        data_type = args.get("data_type")
        count = args.get("count", None)
        show_data(dir_name, data_type, count)

    def make_solo_generate(args: dict):
        check_required_args(["table", "export"], args)
        table = args.get("table")
        check_required_args(["script_name", "class_name"], table)
        export = args.get("export")
        check_required_args(["dir_name"], export)
        script_name = table.get("script_name")
        class_name = table.get("class_name")
        dir_name = export.get("dir_name")
        export_type = export.get("export_type", None)
        export_mode = export.get("export_mode", None)
        table_config = args.get("config", None)
        solo_generate(script_name, class_name, table_config, dir_name, export_type, export_mode)

    MAKERS = {"show_data": make_show_data,
              "solo_generate": make_solo_generate}

    if "job" not in task:
        raise Exception("There is no job in task")
    job = task.get("job")
    if job not in MAKERS:
        raise Exception(f"Unknown job '{job}'. You can only call {MAKERS.keys()}")
    maker = MAKERS.get(job)
    args = task.get("args", {})
    print(f"{job} started")
    maker(args)
    print(f"{job} finished")


if __name__ == '__main__':
    try:
        with open("config/app/default.json", "r") as app_config_file:
            app_config = json.load(app_config_file)
        if "tasks" not in app_config:
            raise Exception("There are no tasks in the app config")
        for task in app_config.get("tasks"):
            make_task(task)
    except Exception as e:
        print(e)
