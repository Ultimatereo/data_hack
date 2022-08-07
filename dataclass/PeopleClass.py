from dataclasses import dataclass
from fields import *


@dataclass
class People:
    __cnt__: int = Constant(100)
    iid: int = IntegerRange(0, 1000)
    sex: str = Select({"male", "female"})
    birth_date: date = Date()
    name: str = StringRange(3, 10, string.ascii_letters)
    middle_name: str = StringRange(3, 10, string.ascii_letters)
    surname: str = StringRange(3, 10, string.ascii_letters)
