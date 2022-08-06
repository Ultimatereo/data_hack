from dataclasses import fields
import random
from typing import Optional, List


def fields_names(table):
    return map(lambda field: field.name, fields(table))


def generate(table):
    """
    Generates random row in table
    :param table:
    :return:
    """
    return {name: getattr(table, name).get() for name in fields_names(table)}


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


class SparkField:
    def intersect(self, other):
        """
        :param other:
        :return:
        new SparkField which generates data that satisfies both conditions
        """
        return None


class NumberRange(SparkField):
    """
    Generates random integer from a to b
    """

    def __init__(self, a, b):
        assert a <= b
        self.a = None
        self.b = None
        self.set_range(a, b)

    def set_range(self, a, b) -> SparkField:
        self.a = a
        self.b = b
        return self

    def get(self) -> int:
        return random.randint(self.a, self.b)

    def intersect(self, other) -> Optional[SparkField]:
        if isinstance(other, NumberRange):
            if self.b < other.a or other.b < self.a:
                return None
            return NumberRange(max(self.a, other.a), min(self.b, other.b))
        return super().intersect(other)
