from dataclasses import dataclass
from fields import *


@dataclass
class AnimeFilm:
    __cnt__: int = Constant(100)
    film_name: str = StringRange(10, 30, string.ascii_letters)
    film_duration: int = IntegerRange(90, 120)
    sequels_number: int = IntegerRange(5, 10)
