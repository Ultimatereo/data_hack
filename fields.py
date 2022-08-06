import json
import os
import random
import string
import typing
from typing import *

from dataclasses import fields


def fields_names(table):
    return filter(lambda name: not (name.startswith("__") or name.endswith("__")),
                  map(lambda field: field.name, fields(table)))


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

    def validate(self, validate_val: str | int | float):
        """
        checks the ability to generate validate_val
        :param validate_val:
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

    def validate(self, validate_val: str | int | float):
        return isinstance(validate_val, int) and self.a <= validate_val <= self.b


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

    def validate(self, validate_val: str | int | float):
        return isinstance(validate_val, float) and self.a <= validate_val <= self.b

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
    def __init__(self, from_length, to_length, alphabet=string.printable):
        assert from_length >= 0 and to_length >= 0
        assert from_length <= to_length
        assert len(alphabet) > 0
        self.from_length = from_length
        self.to_length = to_length
        self.alphabet = alphabet

    def get(self) -> str:
        return ''.join(random.choices(self.alphabet, k=random.randint(self.from_length, self.to_length)))

    def intersect(self, other):
        if isinstance(other, StringRange):
            if self.to_length < other.from_length or other.to_length < self.from_length:
                return None
            alphabet_intersection = ''.join(set(self.alphabet).intersection(other.alphabet))
            if len(alphabet_intersection) == 0:
                return None
            return StringRange(max(self.from_length, other.from_length), min(self.to_length, other.to_length),
                               alphabet_intersection)
        return super().intersect(other)

    def apply_changes(self, changes: dict) -> SparkField:
        new_a, new_b, new_alphabet = changes.get("from_length", self.from_length), changes.get("to_length",
                                                                                               self.to_length), changes.get(
            "alphabet", self.alphabet)
        return StringRange(new_a, new_b, new_alphabet)

    def validate(self, validate_val: str | int | float):
        return isinstance(validate_val, str) \
               and self.from_length <= len(validate_val) <= self.to_length \
               and all(map(lambda ch: ch in self.alphabet, validate_val))


class StringLen(StringRange):
    def __init__(self, length, alphabet=string.printable):
        super(StringLen, self).__init__(length, length, alphabet)


class Select(SparkField):
    def __init__(self, select: set):
        assert len(select) > 0
        list_select = list(select)
        set_type = type(list_select[0])
        assert all(map(lambda key: isinstance(key, set_type), list_select))
        self.select = list_select

    def get(self) -> str | int | float:
        return random.choice(tuple(self.select))

    def intersect(self, other):
        if isinstance(other, Select):
            if not isinstance(self.select[0], type(other.select[0])):
                return None
            list_intersection = set(self.select).intersection(other.select)
            if len(list_intersection) == 0:
                return None
            return Select(list_intersection)
        elif isinstance(other, (Range, StringRange)):
            res_select = set()
            for item in self.select:
                if other.validate(item):
                    res_select.add(item)
            if len(res_select) > 0:
                return Select(res_select)
            return None
        return super().intersect(other)

    def apply_changes(self, changes: dict) -> SparkField:
        new_select = changes.get("select", self.select)
        return Select(new_select)


class Mask(SparkField):
    def __init__(self, mask, alphabet):
        assert len(mask) > 0
        self.mask = mask
        self.alphabet = alphabet

    def get(self):
        c = self.mask.count("#")
        answer = ""
        for i in range(len(self.mask)):
            if self.mask[i] == '#':
                answer += random.choice(self.alphabet)
            else:
                answer += self.mask[i]
        return answer

    def apply_changes(self, changes: dict) -> SparkField:
        new_mask = changes.get("mask", self.mask)
        new_alphabet = changes.get("alphabet", self.alphabet)
        return Mask(new_mask, new_alphabet)


class IntegerMask(Mask):
    def __init__(self, mask, alphabet="0123456789"):
        super().__init__(mask, alphabet)


class StringMask(Mask):
    def __init__(self, mask, alphabet=string.printable):
        super().__init__(mask, alphabet)
