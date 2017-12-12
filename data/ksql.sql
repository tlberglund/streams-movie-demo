SET 'auto.offset.reset' = 'earliest';

CREATE STREAM ratings
(
  movie_id long, 
  rating double
)
WITH
(
  value_format = 'JSON', 
  kafka_topic='ratings'
);

CREATE STREAM ratings (movie_id long, rating double) WITH (value_format = 'JSON', kafka_topic='ratings');


CREATE STREAM movies
(
  movie_id long,
  title varchar,
  release_year int,
  country varchar,
  rating double,
  genres array<varchar>,
  directors array<varchar>,
  composers array<varchar>,
  screenwriters array<varchar>,
  production_companies<varchar>
  cinematographer varchar
)
WITH
(
  VALUE_FORMAT='JSON',
  KAFKA_TOPIC='movies'
);


CREATE STREAM movies (movie_id LONG, title VARCHAR, release_year INT, country VARCHAR, rating DOUBLE, genres ARRAY<VARCHAR>, directors ARRAY<VARCHAR>, composers ARRAY<VARCHAR>, screenwriters ARRAY<VARCHAR>, production_companies ARRAY<VARCHAR>, cinematographer VARCHAR) WITH (VALUE_FORMAT='JSON', KAFKA_TOPIC='movies');

CREATE STREAM movies (movie_id LONG, title VARCHAR, release_year INT) WITH (VALUE_FORMAT='JSON', KAFKA_TOPIC='movies');

CREATE TABLE movies (movie_id LONG, title VARCHAR, release_year INT) WITH (VALUE_FORMAT='JSON', KAFKA_TOPIC='movies', KEY='movie_id');


CREATE TABLE movies (movie_id LONG, title VARCHAR, release_year int, country VARCHAR, rating double, cinematographer varchar) WITH (VALUE_FORMAT='JSON', KAFKA_TOPIC='movies', KEY='movie_id');

CREATE TABLE movies (movie_id LONG, title VARCHAR, release_year INT, country VARCHAR, rating DOUBLE, cinematographer VARCHAR, genres ARRAY<VARCHAR>, directors ARRAY<VARCHAR>, composers ARRAY<varchar>, screenwriters ARRAY<VARCHAR>, production_companies ARRAY<VARCHAR>) WITH (VALUE_FORMAT='JSON', KAFKA_TOPIC='raw-movies', KEY='movie_id');

CREATE STREAM movies (movie_id LONG, title VARCHAR, release_year INT, country VARCHAR, rating DOUBLE, cinematographer VARCHAR, genres ARRAY<VARCHAR>, directors ARRAY<VARCHAR>, composers ARRAY<varchar>, screenwriters ARRAY<VARCHAR>, production_companies ARRAY<VARCHAR>) WITH (VALUE_FORMAT='JSON', KAFKA_TOPIC='movies');

SELECT movie_id,SUM(rating)/COUNT(*) AS avg FROM ratings GROUP BY movie_id;

CREATE TABLE average_ratings AS SELECT movie_id, SUM(rating)/COUNT(*) AS avg FROM ratings GROUP BY movie_id;

CREATE TABLE rated_movies AS SELECT 



