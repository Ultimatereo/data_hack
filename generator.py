from dataclasses import fields
from typing import List
from fields import fields_names


def generate(table):
    """
    Generates random row in table
    :param table:
    :return:
    """
    return tuple(getattr(table, name).get() for name in fields_names(table))


def generate_paired(table1, table2, keys: dict):
    """
    Generates random row in two tables that paired by keys
    Pairing: {keyFirstTable: keySecondTable}
    :param table1:
    :param table2:
    :param keys:
    :return:
    """
    assert len(keys) > 0
    keys_table1 = keys.keys()
    keys_table2 = keys.values()
    assert all(map(lambda key: key in fields_names(table1), keys_table1))
    assert all(map(lambda key: key in fields_names(table2), keys_table2))

    intersect_data_first = {}
    intersect_data_second = {}
    for key in keys:
        intersect_generator = getattr(table1, key).intersect(getattr(table2, keys.get(key)))
        val = None
        if not (intersect_generator is None):
            val = intersect_generator.get()
        intersect_data_first[key] = intersect_data_second[keys.get(key)] = val

    first = {i: getattr(table1, i).get() for i in fields_names(table1) if i not in keys_table1}
    second = {i: getattr(table2, i).get() for i in fields_names(table2) if i not in keys_table2}
    first = {**first, **intersect_data_first}
    second = {**second, **intersect_data_second}
    assert all(map(lambda key: first.get(key) == second.get(keys.get(key)), keys))
    return first, second


def generator(tables: dict):
    return {table_class_name: [generate(table) for _ in range(getattr(table, "__cnt__").get())]
            for (table_class_name, table) in tables.items()}
