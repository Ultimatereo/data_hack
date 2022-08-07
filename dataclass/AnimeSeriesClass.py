from dataclasses import dataclass
from fields import *


@dataclass
class AnimeSeries:
    __cnt__: int = Constant(100)
    series_name: str = StringRange(10, 30, string.ascii_letters)
    series_duration: int = IntegerRange(20, 25)
    episodes_number_in_season: int = IntegerRange(10, 12)
    seasons_number: int = IntegerRange(6, 12)
    main_tyan_number: int = IntegerRange(4, 7)
    producer_id: int = IntegerRange(1, 50)
    release_date: date = Date()