CREATE TABLE IF NOT EXISTS trips (
    "id" SERIAL PRIMARY KEY,
    "region" TEXT,
    "origin_coord_x" DECIMAL(17,15),
    "origin_coord_y" DECIMAL(17,15),
    "destination_coord_x" DECIMAL(17,15),
    "destination_coord_y" DECIMAL(17,15),
    "datetime" TEXT,
    "datasource" TEXT,
    "creation_date" DATE NOT NULL DEFAULT CURRENT_DATE
);