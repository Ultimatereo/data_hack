from dataclasses import dataclass

from fields import *


@dataclass
class Cell:
    integer1: int = IntegerRange(1, 5)
    float2: float = FloatRange(1, 5)
    float3: float = FloatRange(1, 5)
    integer4: int = IntegerRange(1, 5)
    select: int = Select({1, 3, 2})
    mask1: string = StringMask("123fdsf####456###", "ABCD")
    mask2: int = IntegerMask("123###321##2", "01234")
