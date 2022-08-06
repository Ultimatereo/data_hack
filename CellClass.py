from dataclasses import dataclass, fields
from fields import *


@dataclass
class Cell:
    __cnt__: int = Constant(100)
    integer1: int = IntegerRange(1, 5)
    float2: float = FloatRange(1, 5)
    float3: float = FloatRange(1, 5)
    integer4: int = IntegerRange(1, 5)
    select: int = Select({1, 3, 2})
