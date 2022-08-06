from dataclasses import dataclass
from fields import *


@dataclass
class Cell2:
    __cnt__: int = Constant(100)
    iid: int = IntegerRange(-10, 10)
    acceleration: float = FloatRange(-100, 100)
    word: str = StringRange(2, 5)
    select1: int = Select({1, 2})
