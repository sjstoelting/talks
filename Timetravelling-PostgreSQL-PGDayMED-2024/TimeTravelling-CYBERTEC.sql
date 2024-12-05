/**
 * SQL script about PostgreSQL timetravel.
 * 
 * Blog post: https://ProOpenSource.it/blog/postgresql-time-travel
 * 
 * Author: Hans-Jürgen Schönig
 * Author: Stefanie Janine Stölting, Stefanie@ProOpenSource.eu
 * Origin: https://www.cybertec-postgresql.com/en/implementing-as-of-queries-in-postgresql/
 */

-- A schema to test the scipts
DROP SCHEMA IF EXISTS asof CASCADE;

CREATE SCHEMA IF NOT EXISTS asof;

SET SEARCH_PATH TO asof;


-- Orginal code from the blog post
CREATE EXTENSION IF NOT EXISTS btree_gist;
 
CREATE TABLE t_object
(
	id		int8,
	valid		tstzrange,
	some_data1	text,
	some_data2	text,
	EXCLUDE USING gist (id WITH =, valid WITH &&)
);

CREATE INDEX idx_some_index1 ON t_object (some_data1);
CREATE INDEX idx_some_index2 ON t_object (some_data2);


CREATE VIEW t_object_recent AS
	SELECT 	id, some_data1, some_data2
	FROM 	t_object
	WHERE 	current_timestamp <@ VALID
;

SELECT * 
FROM t_object_recent
;


CREATE VIEW t_object_historic AS
	SELECT 	id, some_data1, some_data2
	FROM 	t_object
	WHERE 	current_setting('timerobot.as_of_time')::timestamptz <@ valid;
;

SELECT * FROM t_object_historic

-- Comment: The creation fails in PostgreSQL 17
CREATE FUNCTION version_trigger() RETURNS trigger AS
$
BEGIN
	IF TG_OP = 'UPDATE'
	THEN
		IF NEW.id <> OLD.id
		THEN
			RAISE EXCEPTION 'the ID must not be changed';
		END IF;

		UPDATE 	t_object
		SET 	valid = tstzrange(lower(valid), current_timestamp)
		WHERE	id = NEW.id
			AND current_timestamp <@ valid;

		IF NOT FOUND THEN
			RETURN NULL;
		END IF;
	END IF;

	IF TG_OP IN ('INSERT', 'UPDATE')
	THEN
		INSERT INTO t_object (id, valid, some_data1, some_data2)
			VALUES (NEW.id,
				tstzrange(current_timestamp, TIMESTAMPTZ 'infinity'),
				NEW.some_data1,
				NEW.some_data2);

		RETURN NEW;
	END IF;

	IF TG_OP = 'DELETE'
	THEN
		UPDATE 	t_object
		SET 	valid = tstzrange(lower(valid), current_timestamp)
		WHERE id = OLD.id
			AND current_timestamp <@ valid;

		IF FOUND THEN
			RETURN OLD;
		ELSE
			RETURN NULL;
		END IF;
	END IF;
END;
$ LANGUAGE plpgsql;

CREATE TRIGGER object_trig
	INSTEAD OF INSERT OR UPDATE OR DELETE
	ON t_object_recent
	FOR EACH ROW
	EXECUTE PROCEDURE version_trigger()
;


-- Changed function for PostgreSQL 17
CREATE FUNCTION version_trigger()
  RETURNS TRIGGER
  LANGUAGE PLPGSQL
  AS
$$
BEGIN
	IF TG_OP = 'UPDATE'
	THEN
		IF NEW.id <> OLD.id
		THEN
			RAISE EXCEPTION 'the ID must not be changed';
		END IF;

		UPDATE 	t_object
		SET 	valid = tstzrange(lower(valid), current_timestamp)
		WHERE	id = NEW.id
			AND current_timestamp <@ valid;

		IF NOT FOUND THEN
			RETURN NULL;
		END IF;
	END IF;

	IF TG_OP IN ('INSERT', 'UPDATE')
	THEN
		INSERT INTO t_object (id, valid, some_data1, some_data2)
			VALUES (NEW.id,
				tstzrange(current_timestamp, TIMESTAMPTZ 'infinity'),
				NEW.some_data1,
				NEW.some_data2);

		RETURN NEW;
	END IF;

	IF TG_OP = 'DELETE'
	THEN
		UPDATE 	t_object
		SET 	valid = tstzrange(lower(valid), current_timestamp)
		WHERE id = OLD.id
			AND current_timestamp <@ valid;

		IF FOUND THEN
			RETURN OLD;
		ELSE
			RETURN NULL;
		END IF;
	END IF;
END;
$$

 
CREATE TRIGGER object_trig
	INSTEAD OF INSERT OR UPDATE OR DELETE
	ON t_object_recent
	FOR EACH ROW
	EXECUTE PROCEDURE version_trigger()
;


/*************** Added scripts ***************/

-- Insert records into to view
INSERT INTO t_object_recent (id, some_data1, some_data2)
SELECT s
	, substr(md5(random()::text), 1, 25) AS some_data1
	, substr(md5(random()::text), 1, 25) AS some_data2
FROM generate_series(1, 100000) s(i)
;

-- The view does not contain the timestamp, 
-- when the data has been changed
SELECT min(lower(valid)) AS min_date_from
	, max(lower(valid)) AS max_date_from
	, count(*) AS count_of
FROM t_object
; 


-- Update existing records
WITH t1 AS
	(
		SELECT floor(random() * (100000-1+1) + 1)::bigint AS id
			, substr(md5(random()::text), 1, 25) AS some_data1
			, substr(md5(random()::text), 1, 25) AS some_data2
		FROM generate_series(1, 100000) s(i)
	)
, t2 AS
	(
		SELECT DISTINCT ON (id) id
			, some_data1
			, some_data2
		FROM t1
	)
UPDATE t_object_recent SET
	some_data1 = t2.some_data1,
	some_data2 = t2.some_data2
FROM t2
WHERE t_object_recent.id = t2.id
;


SELECT *
FROM t_object
ORDER BY id
;
