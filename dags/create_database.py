from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import datetime
import pendulum

from typing import Tuple
import logging


# downloaded the data source and save on repository
DATA_PATH = "/opt/airflow/dags/data/trips.csv"

def split_points(point: str) -> Tuple[str, str]:
    if not point.startswith("POINT"):
        return ('0', '0')

    point = point.split("POINT")[-1]
    points = point.strip().replace("(", "").replace(")", "").split(" ")
    return (points[0], points[1])


@dag(
    dag_id="process-trips-data",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def ProcessTripData():
    create_trip_table = PostgresOperator(
        task_id="create_trips_table",
        postgres_conn_id="tutorial_pg_conn",
        sql="sql/create_trip_table.sql",
    )

    create_trip_staging_table = PostgresOperator(
        task_id="create_trips_staging_table",
        postgres_conn_id="tutorial_pg_conn",
        sql="sql/create_trip_staging_table.sql",
    )


    @task
    def get_data():
        postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
        connection = postgres_hook.get_conn()
        cursor = connection.cursor()
        with open(DATA_PATH, "r") as file:
            cursor.copy_expert(
                "COPY trips_staging FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )
        connection.commit()
        connection.close()


    @task
    def store_proccessed_data():
        destination = PostgresHook(postgres_conn_id='tutorial_pg_conn')
        destination_conn = destination.get_conn()
        destination_cursor = destination_conn.cursor()
        destination_cursor.execute("SELECT * FROM trips_staging;")
        data = destination_cursor.fetchall()
        logging.info(f"data => {data[0]} | Type => {type(data)} size => {len(data)}")
        
        for row in data:
            origin_x, origin_y = split_points(row[1])
            destin_x, destin_y = split_points(row[2])

            destination_cursor.execute("""
                INSERT INTO public.trips (region, origin_coord_x, origin_coord_y, destination_coord_x, destination_coord_y, datetime, datasource)
                VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}')
            """.format(row[0], float(origin_x), float(origin_y), float(destin_x), float(destin_y), row[3], row[4]))

        destination_conn.commit()
        destination_conn.close()

    
    [create_trip_table, create_trip_staging_table] >> get_data() >> store_proccessed_data()


dag = ProcessTripData()