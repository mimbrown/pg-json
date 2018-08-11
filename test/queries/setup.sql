BEGIN;

CREATE SCHEMA temp_json_test_schema;
-- CREATE SCHEMA temp_json_test_subschema_1;
-- CREATE SCHEMA temp_json_test_subschema_2;

SET search_path TO temp_json_test_schema;

CREATE TABLE movie_series (
  id serial NOT NULL PRIMARY KEY,
  name text
);

CREATE TABLE movie (
  id serial NOT NULL PRIMARY KEY,
  name text,
  movie_series_id integer REFERENCES movie_series (id)
);

COMMIT;