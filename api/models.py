from pydantic import BaseModel
from typing import Tuple

class Coordinates(BaseModel):
    point_1: Tuple[float, float]
    point_2: Tuple[float, float]
    point_3: Tuple[float, float]
    point_4: Tuple[float, float]

