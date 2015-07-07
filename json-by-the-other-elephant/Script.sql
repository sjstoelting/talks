-- Select each row as JSON
WITH t1 AS (
	SELECT *
	FROM sakila.actor AS a
)
SELECT row_to_json(t1) AS json_result
FROM t1;



-- Select the whole table as one JSON (200 rows)
WITH t1 AS (
	SELECT a.*
	FROM sakila.actor AS a
)
SELECT array_to_json(array_agg(row_to_json(t1))) AS json_result
FROM t1;



-- Select joined tables as one JSON(5,462 rows)
WITH t1 AS (
	SELECT a.*
		, f.*
	FROM sakila.actor AS a
	LEFT OUTER JOIN sakila.film_actor AS fa
		ON a.actor_id = fa.actor_id
	LEFT OUTER JOIN sakila.film AS f
		ON fa.film_id = f.film_id
)
SELECT array_to_json(array_agg(row_to_json(t1))) AS json_result
FROM t1;

-- Just a count of records
WITH t1 AS (
	SELECT a.*
		, f.*
	FROM sakila.actor AS a
	LEFT OUTER JOIN sakila.film_actor AS fa
		ON a.actor_id = fa.actor_id
	LEFT OUTER JOIN sakila.film AS f
		ON fa.film_id = f.film_id
)
SELECT count(*) FROM t1
;



-- I have to drop the table
drop table jsonb.actor;



-- A table with a jsonb field
CREATE TABLE jsonb.actor(id serial NOT NULL, jsondata jsonb);



-- Fill that table with data
WITH t1 AS (
	SELECT *
	FROM sakila.actor
),
t2 AS (
	SELECT CAST(row_to_json(t1) AS jsonb) AS jsonb_row
	FROM t1
) 
INSERT INTO jsonb.actor(jsondata)
SELECT jsonb_row
FROM t2
;



-- See the results from the insert
SELECT * FROM jsonb.actor;



-- Get a key value from the jsonb data
SELECT jsondata->>'actor_id' AS actor_identifier
FROM jsonb.actor;



-- Index the complete content with a GIN index
CREATE INDEX idx_1 ON jsonb.actor USING GIN (jsondata);



-- Create a unique index on a jsonb key
CREATE UNIQUE INDEX actor_id ON jsonb.actor((jsondata->'actor_id'::TEXT));



-- To show, that the unique index previously created does work
INSERT INTO jsonb.actor(jsondata) values ('{"actor_id": 2, "last_name": "WAHLBERG", "first_name": "NICK", "last_update": "2006-02-15T04:34:33+01:00"}');



-- Join the JSON results with relational tables
SELECT CAST(jsondata->>'actor_id' AS INTEGER) AS actor_id
	, a.jsondata->>'last_name' AS last_name
	, a.jsondata->>'first_name' AS first_name
	, f.title
	, f.release_year
FROM jsonb.actor AS a
LEFT OUTER JOIN sakila.film_actor AS fa
	ON CAST(jsondata->>'actor_id' AS INTEGER) = fa.actor_id
LEFT OUTER JOIN sakila.film AS f
	ON fa.film_id = f.film_id
WHERE a.jsondata->>'last_name' = 'DAVIS'
;



-- Pretty JSON result (human readable)
SELECT jsonb_pretty(jsondata)
FROM jsonb.actor
WHERE jsondata->>'last_name' = 'GUINESS'
;



-- More select on the JSON data
WITH actors AS (
	SELECT CAST(jsondata->>'actor_id' AS INTEGER) AS actor_id
		, a.jsondata->>'last_name' AS lastname
		, a.jsondata->>'first_name' AS firstname
		, CAST(a.jsondata->>'last_update' AS TIMESTAMP) AS lastupdate
	FROM jsonb.actor AS a
), films AS (
	SELECT fa.actor_id
		, f.*
	FROM sakila.film_actor AS fa
	INNER JOIN sakila.film AS f
		ON fa.film_id = f.film_id
), actor_film AS (
	SELECT a.actor_id
		, a.firstname
		, a.lastname
		, a.lastupdate
		, AGE(a.lastupdate) since_lastupdate
		, EXTRACT(year FROM NOW()) - EXTRACT(year FROM a.lastupdate) in_years
		, array_to_json(array_agg(row_to_json(f))) AS films
	FROM actors AS a
	INNER JOIN films AS f
		ON a.actor_id = f.actor_id
	WHERE a.actor_id = 4
	GROUP BY a.actor_id
		, a.firstname
		, a.lastname
		, a.lastupdate
		, AGE(a.lastupdate) 
		, EXTRACT(year FROM NOW()) - EXTRACT(year FROM a.lastupdate) 
), json_result AS (
	SELECT array_to_json(array_agg(row_to_json(actor_film))) AS jsondata
	FROM actor_film
)
SELECT jsonb_pretty(CAST(jsondata AS jsonb))
FROM json_result
;
