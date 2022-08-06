import typing
from dataclasses import fields
import random
from typing import Optional, List
import os
import json
import string


def fields_names(table):
    return map(lambda field: field.name, fields(table))


def load_config(table, fp):
    if fp in os.listdir():
        with open(fp, "r") as conf:
            apply_changes(table, json.load(conf))


def apply_changes(table, changes: dict):
    for field_name in changes:
        setattr(table, field_name, getattr(table, field_name).apply_changes(changes.get(field_name, {})))


def generate(table):
    """
    Generates random row in table
    :param table:
    :return:
    """
    return {name: getattr(table, name).get() for name in fields_names(table)}


def generate_paired(table1, table2, keys: typing.Dict):
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


class SparkField:
    def intersect(self, other):
        """
        :param other:
        :return:
        new SparkField which generates data that satisfies both conditions
        """
        return None

    def apply_changes(self, changes: dict):
        """
        applies config to field
        :param changes:
        :return:
        """
        pass


class IntegerRange(SparkField):
    """
    Generates random integer from a to b
    """

    def __init__(self, a, b):
        assert a <= b
        self.a = None
        self.b = None
        self.set_range(a, b)

    def set_range(self, a, b) -> SparkField:
        assert a <= b
        self.a = a
        self.b = b
        return self

    def get(self) -> int:
        return random.randint(self.a, self.b)

    def intersect(self, other) -> Optional[SparkField]:
        if isinstance(other, IntegerRange):
            if self.b < other.a or other.b < self.a:
                return None
            return IntegerRange(max(self.a, other.a), min(self.b, other.b))
        return super().intersect(other)

    def apply_changes(self, changes: dict) -> SparkField:
        new_a, new_b = changes.get("a", self.a), changes.get("b", self.b)
        if new_a > new_b:
            return self
        return IntegerRange(new_a, new_b)


class FloatRange(SparkField):
    """
    Generates random float from a to b
    """

    def __init__(self, a, b):
        assert a <= b
        self.a = None
        self.b = None
        self.set_range(a, b)

    def set_range(self, a, b) -> SparkField:
        assert a <= b
        self.a = a
        self.b = b
        return self

    def get(self) -> int:
        return random.random() * (self.b - self.a) + self.a

    def intersect(self, other) -> Optional[SparkField]:
        if isinstance(other, FloatRange):
            if self.b < other.a or other.b < self.a:
                return None
            return FloatRange(max(self.a, other.a), min(self.b, other.b))
        return super().intersect(other)

    def apply_changes(self, changes: dict) -> SparkField:
        new_a, new_b = changes.get("a", self.a), changes.get("b", self.b)
        if new_a > new_b:
            return self
        return FloatRange(new_a, new_b)


class StringRange(SparkField):
    def __init__(self, f_length, to_length, alphabet=string.printable):
        assert f_length >= 0 and to_length >= 0
        assert f_length <= to_length
        assert len(alphabet) > 0
        self.a = None
        self.b = None
        self.alphabet = None
        self.set_range(f_length, to_length, alphabet)

    def set_range(self, f_length, to_length, alphabet) -> SparkField:
        assert f_length >= 0 and to_length >= 0
        assert f_length <= to_length
        assert len(alphabet) > 0
        self.a = f_length
        self.b = to_length
        self.alphabet = alphabet
        return self

    def get(self) -> str:
        return ''.join(random.choices(self.alphabet, k=random.randint(self.a, self.b)))

    def intersect(self, other):
        if isinstance(other, StringRange):
            if self.b < other.a or other.b < self.a:
                return None
            alphabet_intersection = ''.join(set(self.alphabet).intersection(other.alphabet))
            if len(alphabet_intersection) == 0:
                return None
            return StringRange(max(self.a, other.a), min(self.b, other.b), alphabet_intersection)
        return super().intersect(other)

    def apply_changes(self, changes: dict) -> SparkField:
        new_a, new_b, new_alphabet = changes.get("a", self.a), changes.get("b", self.b), changes.get("a", self.alphabet)
        return StringRange(new_a, new_b, new_alphabet)


class StringLen(StringRange):
    def __init__(self, length, alphabet=string.printable):
        super(StringLen, self).__init__(length, length, alphabet)


class FromList(SparkField):
    def __init__(self, select: List):
        assert all(map(lambda key: isinstance(key, int), select)) or all(map(lambda key: isinstance(key, str), select))
        assert len(select) > 0
        self.select = select




    def get(self) -> str:
        print(self.select[random.randint(0, len(self.select) - 1)])
        return self.select[random.randint(0, len(self.select) - 1)]

    def intersect(self, other):
        if isinstance(other, FromList):
            if type(self.select[0]) != type(other.select[0]):
                return None
            list_intersection = list((set(self.select).intersection(other.select)))
            if len(list_intersection) == 0:
                return None
            return FromList(list_intersection)
        return super().intersect(other)

    def apply_changes(self, changes: dict) -> SparkField:
        new_select = changes.get("select", self.select)
        return FromList(new_select)
