from dataclasses import dataclass
from fields import *


@dataclass
class Cell:
    integer1: int = IntegerRange(1, 5)
    float2: float = FloatRange(1, 5)
    float3: float = FloatRange(1, 5)
    integer4: int = IntegerRange(1, 5)


@dataclass
class Cell2:
    float3: float = FloatRange(-10.5, 3)
    integer4: int = IntegerRange(2, 3)
    integer5: int = IntegerRange(1, 5)
    integer6: int = IntegerRange(1, 5)
