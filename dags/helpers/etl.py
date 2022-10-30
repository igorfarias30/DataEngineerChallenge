from sqlalchemy import create_engine
from typing import Tuple
import pandas as pd
import logging


def split_points(point: str) -> Tuple[str]:
    if not point.startswith("POINT"):
        return ('0', '0')

    point = point.split("POINT")[-1]
    points = point.strip().replace("(", "").replace(")", "").split(" ")
    return (points[0], points[1])


def process_data(filename: str, table_name:str):
    logging.info(f"Filename => {filename} | Table Name => {table_name}")

    dataset = pd.read_csv(filename)
    origin_coord_tuples = dataset["origin_coord"].apply(lambda x: split_points(x))
    dataset["origin_coord_x"] = pd.Series(origin_coord_tuples[:][0])
    dataset["origin_coord_y"] = pd.Series(origin_coord_tuples[:][1])

    destination_coord_tuples = dataset["destination_coord"].apply(lambda x: split_points(x))
    dataset["destination_coord_x"] = pd.Series(destination_coord_tuples[:][0])
    dataset["destination_coord_y"] = pd.Series(destination_coord_tuples[:][1])
    dataset.drop(columns=["origin_coord", "destination_coord"], inplace=True)

    conn_string = 'postgresql+psycopg2://airflow:airflow@localhost/airflow'
    db = create_engine(conn_string)
    conn = db.connect()
    
    dataset.to_sql(table_name, con=conn, if_exists='append', index=False)
