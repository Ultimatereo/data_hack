from dataclasses import dataclass
from fields import *


@dataclass
class Cat:
    __cnt__: int = Constant(1000)
    color: str = Select({"black", "white", "orange", "gray"})
    action: str = Select({"shaking", "laying", "jumping", "running", "playing"})
    birth_date: date = Date()
    pet_name: str = StringRange(5, 7, string.ascii_letters)
    meow_volume: int = IntegerRange(0, 100)
