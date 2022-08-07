import random
import string
from datetime import datetime, date, time
from typing import *

from dataclasses import fields
from faker import Faker


def all_fields_names(table):
    return map(lambda field: field.name, fields(table))


def fields_names(table):
    return filter(lambda name: not (name.startswith("__") or name.endswith("__")),
                  all_fields_names(table))


class SparkField:
    fake = Faker()

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

    def validate(self, validate_val: Union[str, int, float]):
        """
        checks the ability to generate validate_val
        :param validate_val:
        """
        pass

    def get(self):
        """
        gets a new value of SparkField
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

    def validate(self, validate_val: Union[str, int, float]):
        return isinstance(validate_val, int) and self.start <= validate_val <= self.stop


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

    def validate(self, validate_val: Union[str, int, float]):
        return isinstance(validate_val, float) and self.start <= validate_val <= self.stop


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
        new_a, new_b, new_alphabet = changes.get("from_length", self.from_length), \
                                     changes.get("to_length", self.to_length), \
                                     changes.get("alphabet", self.alphabet)
        return StringRange(new_a, new_b, new_alphabet)

    def validate(self, validate_val: Union[str, int, float]):
        return isinstance(validate_val, str) \
               and self.from_length <= len(validate_val) <= self.to_length \
               and all(map(lambda ch: ch in self.alphabet, validate_val))


class StringLen(StringRange):
    def __init__(self, length, alphabet=string.printable):
        super(StringLen, self).__init__(length, length, alphabet)

    def apply_changes(self, changes: dict) -> SparkField:
        length, new_alphabet = changes.get("length", self.from_length), changes.get("alphabet", self.alphabet)
        return StringRange(length, new_alphabet)


class WeightSelect(SparkField):
    def __init__(self, select: dict):
        assert len(select) > 0
        list_select = []
        list_weight = []
        for k, v in select.items():
            list_select.append(k)
            list_weight.append(v)
        set_type = type(list_select[0])
        assert isinstance(list_select[0], (int, float, str))
        assert all(map(lambda key: isinstance(key, set_type), list_select))
        assert all(map(lambda val: isinstance(val, (int, float, str)), list_weight))
        self.select = list_select
        self.weight = list_weight
        self.defaultdict = select

    def get(self) -> Union[str, int, float]:
        choise = random.choices(self.select, weights=self.weight)
        assert len(choise) > 0
        return choise[0]

    def intersect(self, other):
        if isinstance(other, WeightSelect):
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

    def validate(self, validate_val: Union[str, int, float]):
        assert len(self.select) == 0
        return isinstance(validate_val, type(self.select[0])) and validate_val in self.select

    def apply_changes(self, changes: dict) -> SparkField:
        new_select = changes.get("select", self.defaultdict)
        assert isinstance(new_select, dict) and len(new_select) > 0
        return WeightSelect(new_select)


class Select(WeightSelect):
    def __init__(self, select: set):
        assert len(select) > 0
        super().__init__(dict.fromkeys(select, 1))

    def apply_changes(self, changes: dict) -> SparkField:
        new_select = changes.get("select", self.select)
        assert isinstance(new_select, list) and len(new_select) > 0
        return Select(new_select)


class Mask(SparkField):
    def __init__(self, mask, alphabet):
        assert len(mask) > 0
        self.mask = mask
        self.alphabet = alphabet

    def get_impl(self):
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
        return self.create_new(new_mask, new_alphabet)

    def validate(self, validate_val: Union[str, int, float]):
        validate_val = str(validate_val)
        if len(self.mask) != len(validate_val):
            return False
        for i in range(len(self.mask)):
            if self.mask[i] == "#" and not validate_val[i] in self.alphabet:
                return False
            if self.mask[i] != "#" and validate_val[i] != self.mask[i]:
                return False
        return True


class IntegerMask(Mask):
    def __init__(self, mask, alphabet=string.digits):
        if not alphabet.isnumeric():
            raise Exception("Alphabet of Integer Mask should contain just numbers! But there were some letters found.")
        super().__init__(mask, alphabet)

    def get(self):
        return int(self.get_impl())

    def create_new(self, *args, **kwargs):
        return IntegerMask(*args, **kwargs)


class StringMask(Mask):
    def __init__(self, mask, alphabet=string.printable):
        super().__init__(mask, alphabet)

    def get(self):
        return self.get_impl()

    def create_new(self, *args, **kwargs):
        return StringMask(*args, **kwargs)


class Time(SparkField):
    def __init__(self):
        self.start = None
        self.stop = None

    def validate(self, validate_val: Union[str, int, float]):
        if (isinstance(validate_val, str)):
            try:
                self.check(validate_val)
                return True
            except ValueError:
                return False

    def intersect(self, other):
        if isinstance(other, Time):
            first_start1 = date(self.start.year, self.start.month, self.start.day)
            first_stop1 = date(self.stop.year, self.start.month, self.start.day)
            second_start1 = date(other.start.year, other.start.month, other.start.day)
            second_stop1 = date(other.stop.year, other.stop.month, other.stop.day)
            first_start2 = time(0, 0)
            first_stop2 = time(0, 0)
            second_start2 = time(0, 0)
            second_stop2 = time(0, 0)
            if isinstance(self, TimeStamp):
                first_start2 = time(self.start.hour, self.start.minute)
                first_stop2 = time(self.stop.hour, self.stop.minute)
            if isinstance(other, TimeStamp):
                second_start2 = time(other.start.hour, other.start.minute)
                second_stop2 = time(other.stop.hour, other.stop.minute)

            max_start = max(datetime.combine(first_start1, first_start2),
                            datetime.combine(second_start1, second_start2))
            min_stop = min(datetime.combine(first_stop1, first_stop2), datetime.combine(second_stop1, second_stop2))
            if max_start <= min_stop:
                return self.create_new(max_start, min_stop)
        return None

    def apply_changes(self, changes: dict):
        new_start_year = changes.get("start_year", self.start.year)
        new_start_month = changes.get("start_month", self.start.month)
        new_start_day = changes.get("start_day", self.start.day)
        new_start_hour = changes.get("start_hour", 0)
        new_start_minute = changes.get("start_minute", 0)
        new_stop_year = changes.get("stop_year", self.stop.year)
        new_stop_month = changes.get("stop_month", self.stop.month)
        new_stop_day = changes.get("stop_day", self.stop.day)
        new_stop_hour = changes.get("stop_hour", 0)
        new_stop_minute = changes.get("stop_minute", 0)
        return self.create_new(
            datetime(new_start_year, new_start_month, new_start_day, new_start_hour, new_start_minute),
            datetime(new_stop_year, new_stop_month, new_stop_day, new_stop_hour, new_stop_minute))

    def check(self, validate_val):
        pass


class Date(Time):
    def __init__(self, start=date(1970, 1, 1), stop=date.today()):
        super().__init__()
        assert start <= stop
        self.start = start
        self.stop = stop

    def get(self):
        return self.fake.date_between(self.start, self.stop)

    def create_new(self, start, end):
        return Date(start.date(), end.date())

    def check(self, validate_val):
        datetime.strptime(validate_val, '%Y-%m-%d')


class TimeStamp(Time):
    def __init__(self, start=datetime(1970, 1, 1, 0, 0, 0), stop=datetime.today()):
        super().__init__()
        assert start <= stop
        self.start = start
        self.stop = stop

    def get(self):
        return self.fake.date_time_between(self.start, self.stop)

    def create_new(self, *args, **kwargs):
        return TimeStamp(*args, **kwargs)

    def check(self, validate_val):
        datetime.strptime(validate_val, '%Y-%m-%d %H:%M:%S')
