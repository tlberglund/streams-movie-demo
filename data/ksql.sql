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


-------------------------------------------------------------------------------------

head -n1 ratings-json.js | kafkacat -b localhost:9092 -t ratings -P
head -n1 movies-json.js  | kafkacat -b localhost:9092 -t movies -P
SET 'auto.offset.reset' = 'earliest';

CREATE STREAM movies_src (movie_id LONG, title VARCHAR, release_year INT, country VARCHAR, rating DOUBLE, cinematographer VARCHAR, genres ARRAY<VARCHAR>, directors ARRAY<VARCHAR>, composers ARRAY<varchar>, screenwriters ARRAY<VARCHAR>, production_companies ARRAY<VARCHAR>) WITH (VALUE_FORMAT='JSON', KAFKA_TOPIC='movies');

CREATE STREAM movies_rekeyed AS SELECT * FROM movies_src PARTITION BY movie_id;

kafkacat -C  -K: -b localhost:9092 -f 'Key:    %k\nValue:  %s\n' -t movies
kafkacat -C  -K: -b localhost:9092 -f 'Key:    %k\nValue:  %s\n' -t MOVIES_REKEYED2

CREATE TABLE movies_ref (movie_id LONG, title VARCHAR, release_year INT, country VARCHAR, rating DOUBLE, cinematographer VARCHAR, genres ARRAY<VARCHAR>, directors ARRAY<VARCHAR>, composers ARRAY<varchar>, screenwriters ARRAY<VARCHAR>, production_companies ARRAY<VARCHAR>) WITH (VALUE_FORMAT='JSON', KAFKA_TOPIC='MOVIES_REKEYED');

CREATE STREAM ratings (movie_id LONG, rating DOUBLE) WITH (VALUE_FORMAT = 'JSON', KAFKA_TOPIC='ratings');

cat movies-json.js | kafkacat -b localhost:9092 -t movies -P

SELECT m.title, m.release_year, r.rating FROM ratings r LEFT OUTER JOIN movies_ref m on r.movie_id = m.movie_id;

head -n1000 ratings-json.js | kafkacat -b localhost:9092 -t ratings -P

CREATE TABLE movie_ratings AS SELECT m.title, SUM(r.rating)/COUNT(r.rating) AS avg_rating, COUNT(r.rating) AS num_ratings FROM ratings r LEFT OUTER JOIN movies_ref m ON m.movie_id = r.movie_id GROUP BY m.title;


SELECT ROWKEY, ROWTIME, title, avg_rating FROM movie_ratings;





CREATE STREAM movies_src (movie_id LONG, title VARCHAR, release_year INT, country VARCHAR, rating DOUBLE, cinematographer VARCHAR, genres ARRAY<VARCHAR>, directors ARRAY<VARCHAR>, composers ARRAY<varchar>, screenwriters ARRAY<VARCHAR>, production_companies ARRAY<VARCHAR>) WITH (VALUE_FORMAT='JSON', KAFKA_TOPIC='movies');
CREATE STREAM movies_rekeyed AS SELECT * FROM movies_src PARTITION BY movie_id;
CREATE TABLE movies_ref (movie_id LONG, title VARCHAR, release_year INT, country VARCHAR, rating DOUBLE, cinematographer VARCHAR, genres ARRAY<VARCHAR>, directors ARRAY<VARCHAR>, composers ARRAY<varchar>, screenwriters ARRAY<VARCHAR>, production_companies ARRAY<VARCHAR>) WITH (VALUE_FORMAT='JSON', KAFKA_TOPIC='MOVIES_REKEYED');
CREATE STREAM ratings (movie_id LONG, rating DOUBLE) WITH (VALUE_FORMAT = 'JSON', KAFKA_TOPIC='ratings');
CREATE TABLE movie_ratings AS SELECT m.title, SUM(r.rating)/COUNT(r.rating) AS avg_rating, COUNT(r.rating) AS num_ratings FROM ratings r LEFT OUTER JOIN movies_ref m ON m.movie_id = r.movie_id GROUP BY m.title;









-------------

docker-compose up -d
docker-compose logs -f control-center | grep -e HTTP
scripts/01-start-twitter-connector.sh
scripts/consume-twitter-feed.sh
scripts/02_start-ksql-server-with-props-and-commands.sh
scripts/start-ksql-cli.sh
