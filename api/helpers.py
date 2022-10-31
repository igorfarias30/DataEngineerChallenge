from typing import Tuple, List
from datetime import datetime
import pandas as pd


def ray_tracing_method(x, y, bounding_box) -> bool:

    n = len(bounding_box)
    inside = False
    point1_x, point1_y = bounding_box[0]
    for i in range(n+1):
        point2_x, point2_y = bounding_box[i % n]
        if y > min(point1_y, point2_y):
            if y <= max(point1_y, point2_y):
                if x <= max(point1_x, point2_x):
                    if point1_y != point2_y:
                        xints = (y-point1_y) * (point2_x-point1_x) / (point2_y-point1_y) + point1_x
                    if point1_x == point2_x or x <= xints:
                        inside = not inside
        point1_x,point1_y = point2_x, point2_y

    return inside


def weekly_mean_by_coordinate(bounding_box: List[Tuple[float, float]], dataframe: pd.DataFrame) -> float:
    
    def create_tuple(x, y): return (x, y)

    # bounding_box = sorted(coordinates, key=lambda x: x[0])
    dataframe["destination_points"] = dataframe[["destination_coord_x", "destination_coord_y"]].apply(lambda x: create_tuple(*x), axis=1)
    
    # filtering the dataframe to only sample that the points lies in bounding_box
    area_data = dataframe[dataframe["destination_points"].apply(lambda x: ray_tracing_method(x[0], x[1], bounding_box) )]
    if(area_data.size == 0): return 0

    # get the week number of the year
    area_data["week"] = area_data["datetime"].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S').isocalendar().week)
    
    # summarize the number of trips by weekly and divide by the number of weeks
    return area_data["week"].value_counts().values.mean()


def weekly_mean_by_region(dataset: pd.DataFrame, region_name: str) -> float:
    region_data = dataset[dataset["region"] == region_name]
    if(region_data.size == 0): return 0

    # get the week number of the year
    region_data["week"] = region_data["datetime"]\
        .apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S').isocalendar().week)
    
    # summarize the number of trips by weekly and divide by the number of weeks
    return region_data["week"]\
        .value_counts()\
        .values.mean()

