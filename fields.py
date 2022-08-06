from dataclasses import fields
import random
from typing import *
import os
import json
from typing import Optional


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
