CREATE TABLE IF NOT EXISTS trips (
    "id" SERIAL PRIMARY KEY,
    "region" TEXT,
    "origin_coord_x" TEXT,
    "origin_coord_y" TEXT,
    "destination_coord_x" TEXT,
    "destination_coord_y" TEXT,
    "datetime" TEXT,
    "datasource" TEXT,
    "creation_date" DATE NOT NULL DEFAULT CURRENT_DATE
);