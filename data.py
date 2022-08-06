from dataclasses import dataclass
from fields import *


@dataclass
class Cell:
    integer1: int = NumberRange(1, 5)
    integer2: int = NumberRange(1, 5)
    integer3: int = NumberRange(1, 5)
    integer4: int = NumberRange(1, 5)


@dataclass
class Cell2:
    integer3: int = NumberRange(-100, 100)
    integer4: int = NumberRange(2, 3)
    integer5: int = NumberRange(1, 5)
    integer6: int = NumberRange(1, 5)


