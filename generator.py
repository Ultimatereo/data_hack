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


def generate_paired(table1, table2, keys: List[str]):
    """
    Generates random row in two tables that paired by keys
    :param table1:
    :param table2:
    :param keys:
    :return:
    """
    assert len(keys) > 0
    assert all(map(lambda key: key in fields_names(table1), keys))
    assert all(map(lambda key: key in fields_names(table2), keys))
    intersect_data = {}
    for key in keys:
        intersect_generator = getattr(table1, key).intersect(getattr(table2, key))
        val = None
        if not (intersect_generator is None):
            val = intersect_generator.get()
        intersect_data[key] = val
    first = {i: getattr(table1, i).get() for i in fields_names(table1) if i not in keys}
    second = {i: getattr(table2, i).get() for i in fields_names(table2) if i not in keys}
    first = {**first, **intersect_data}
    second = {**second, **intersect_data}
    assert all(map(lambda key: first.get(key) == second.get(key), keys))
    return first, second


def generator(tables: dict):
    return {table_class_name: [generate(table) for _ in range(getattr(table, "__cnt__").get())]
            for (table_class_name, table) in tables.items()}
