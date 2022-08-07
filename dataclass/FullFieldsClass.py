from dataclasses import dataclass
from fields import *


@dataclass
class FullFields:
    __cnt__: int = Constant(100)
    integerRange: int = IntegerRange(1, 10)
    floatRange: float = FloatRange(0, 1)
    constant: int = Constant(123)
    stringRange: str = StringRange(3, 5)
    stringLen: str = StringLen(3)
    selectInt: int = Select({1, 555, -1})
    selectFloat: float = Select({1.234, 5.678, 9.0123})
    weightSelectStr: str = WeightSelect({"often": 6, "common": 3, "rare": 1})
    integerMask: int = IntegerMask("123#####")
    stringMask: str = StringMask("say <####>")
    dateField: date = Date(date(2020, 10, 3))
    timestampField: datetime = TimeStamp(datetime(1999, 10, 3, 10, 10, 10))
