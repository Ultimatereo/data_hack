import importlib
import json
from pprint import pprint

from fields import *
from generator import *


def get_table_class(path, class_name):
    m = importlib.import_module(path)
    c = getattr(m, class_name)
    return c


def load_table(tables: dict, table_script: str, table_class_name: str):
    table_class = get_table_class(table_script, table_class_name)
    tables[table_class_name] = table_class()


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


def main_generate():
    # init tables
    tables = {}

    load_table(tables, "CellClass", "Cell")
    # load_table(tables, "Cell2Class", "Cell2")

    # load config
    load_config(tables)

    if True:
        # generate solo data
        data = generator(tables)
        # print("Data generated")
        for table in data:
            print(f"TABLE {table}")
            for row in data.get(table):
                print(row)
    else:
        data1, data2 = generate_paired(tables.get("Cell"), tables.get("Cell2"), {"integer1": "iid"})
        pprint(data1)
        pprint(data2)


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
    main_generate()
    # play_around()
