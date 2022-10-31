from typing import Union, Dict, Tuple, List

from fastapi import FastAPI
from sqlalchemy import create_engine
import pandas as pd

from .api.models import Coordinates
from .api.helpers import weekly_mean_by_region, weekly_mean_by_coordinate

SQLALCHEMY_DATABASE_URL = 'postgresql+psycopg2://airflow:airflow@postgres/airflow'

app = FastAPI(
    title="Jobsity Challenge API",
    description="Rest API to solve Jobsity challenge. ",
    version="0.0.1",
    contact={
        "name": "Igor Farias",
        "email": "igorfariasmi@gmail.com",
    }
)


@app.post("/weekly-average-by-cordinates")
async def get_weekly_average_by_coordinates(coordinates: Coordinates):
    dataset = pd.read_sql('SELECT * FROM trips', SQLALCHEMY_DATABASE_URL)
    polygon = [coordinates.point_1, coordinates.point_2, coordinates.point_3, coordinates.point_4]
    average = weekly_mean_by_coordinate(polygon, dataset)
    return {"average": average}


@app.get("/weekly-average-by-region/{region}")
async def get_weekly_average_by_region(region: str) -> Dict[str, Union[str, float]]:
    dataset = pd.read_sql('SELECT * FROM trips', SQLALCHEMY_DATABASE_URL)
    average = weekly_mean_by_region(dataset, region)
    return {
        "region": region,
        "weekly_average": average
    }

