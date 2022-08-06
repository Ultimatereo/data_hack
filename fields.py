from dataclasses import fields
import random
from typing import *
import os
import json
from typing import Optional
import string


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
    def create_new(self, *args, **kwargs):
        """
        Creates new SparkField with such args
        :param args:
        :param kwargs:
        :return:
        """
        pass

    def intersect(self, other):
        """
        :param other:
        :return:
        new SparkField which generates data that satisfies both conditions
        """
        pass

    def apply_changes(self, changes: dict):
        """
        applies config to field
        :param changes:
        :return:
        """
        pass


class Range(SparkField):
    def __init__(self, data_type, start, stop):
        self.data_type = data_type
        assert start <= stop
        self.start = start
        self.stop = stop

    def get(self) -> Union[int, float]:
        pass

    def intersect(self, other) -> Optional[SparkField]:
        if isinstance(other, Range):
            if self.stop < other.start or other.stop < self.start or not (self.data_type is other.data_type):
                return None
            return self.create_new(max(self.start, other.start), min(self.stop, other.stop))
        return super().intersect(other)

    def apply_changes(self, changes: dict) -> SparkField:
        new_start, new_stop = changes.get("start", self.start), changes.get("stop", self.stop)
        return self.create_new(new_start, new_stop)


class IntegerRange(Range):
    """
    Generates random integer from start to stop
    """

    def __init__(self, start, stop):
        super().__init__(int, start, stop)

    @staticmethod
    def create_new(*args, **kwargs) -> Range:
        return IntegerRange(*args, **kwargs)

    def get(self) -> int:
        return random.randint(self.start, self.stop)


class FloatRange(Range):
    """
    Generates random float from start to stop
    """

    def __init__(self, start, stop):
        super().__init__(float, start, stop)

    @staticmethod
    def create_new(*args, **kwargs) -> Range:
        return FloatRange(*args, **kwargs)

    def get(self) -> float:
        return random.random() * (self.stop - self.start) + self.start


class Constant(SparkField):
    """
    Makes constant value
    """

    def __init__(self, value):
        self.value = value

    def apply_changes(self, changes: dict) -> SparkField:
        return Constant(changes.get("value", self.value))

    def get(self):
        return self.value



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


class Select(SparkField):
    def __init__(self, select: set):
        assert all(map(lambda key: isinstance(key, int), select)) or all(map(lambda key: isinstance(key, str), select))
        assert len(select) > 0
        self.select = list(select)

    def get(self) -> str:
        return random.choice(tuple(self.select))

    def intersect(self, other):
        if isinstance(other, Select):
            if isinstance(self.select[0], type(other.select[0])):
                return None
            list_intersection = set(self.select).intersection(other.select)
            if len(list_intersection) == 0:
                return None
            return Select(list_intersection)
        return super().intersect(other)

    def apply_changes(self, changes: dict) -> SparkField:
        new_select = changes.get("select", self.select)
        return Select(new_select)
