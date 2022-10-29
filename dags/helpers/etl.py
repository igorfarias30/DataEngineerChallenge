from typing import Tuple
import pandas as pd

def my_funcion(x):
    return x +  " hello D.E."

def split_points(point: str) -> Tuple[str]:
    if point.startswith("POINT"):
        point = point.split("POINT")[-1]
        points = point.strip().replace("(", "").replace(")", "").split(" ")
        return (points[0], points[1])

def process_data(filename: str):
    data = pd.read_csv(filename)
    columns = data.columns