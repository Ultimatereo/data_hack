from dataclasses import dataclass
from fields import *


@dataclass
class Cell2:
    __cnt__: int = Constant(100)
    iid: int = IntegerRange(-10, 10)
    word: str = StringRange(2, 5)
