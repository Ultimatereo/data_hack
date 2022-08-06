from dataclasses import dataclass

from fields import *


@dataclass
class Cell:
    __cnt__: int = Constant(100)
    integer1: int = IntegerRange(1, 5)
    float2: float = FloatRange(1, 5)
    select: int = Select({1, 3, 2})
    mymask: int = IntegerMask("123###321##2", "01234")
    date1: str = Date()
    date2: str = TimeStamp()
