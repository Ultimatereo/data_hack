from dataclasses import dataclass
from fields import *


@dataclass
class Bank:
    __cnt__: int = Constant(100)
    people_id: int = IntegerRange(0, 1000)
    money: int = IntegerRange(0, 1000000)
    security_code: str = StringMask("####", "0123456789")
