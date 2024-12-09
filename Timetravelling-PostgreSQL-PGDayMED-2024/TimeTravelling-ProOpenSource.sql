/**
 * SQL script about PostgreSQL timetravel.
 *
 * Talk at PGDay/MED 2024: Time Travelling PostgreSQL
 *
 * Author: Stefanie Janine Stölting, Stefanie@ProOpenSource.eu
 */

-- Drops the Schema used in this script with all objects
DROP SCHEMA IF EXISTS timetravel CASCADE;

CREATE SCHEMA IF NOT EXISTS timetravel;

SET SEARCH_PATH TO timetravel;

-- Creates the btree GIST index which will be used for the column changed
CREATE EXTENSION IF NOT EXISTS btree_gist;








/**TABLES*****************************************/

-- Creates the primary key table which will also keep the creation timestamp
CREATE TABLE timetravel_pk (
  timetravelid BIGINT GENERATED ALWAYS AS IDENTITY,
  created timestamp with time zone NOT NULL,
  CONSTRAINT timetravle_pk_pk PRIMARY KEY (timetravelid)
);



-- Creates the partitioned table which will store all versions of records
CREATE TABLE timetravel (
  timetravelid BIGINT NOT NULL,
  changed TSTZRANGE NOT NULL DEFAULT tstzrange(clock_timestamp(), 'INFINITY', '[)'),
  data_text TEXT,
  data_json JSONB,
  deleted BOOLEAN NOT NULL DEFAULT FALSE,
  CONSTRAINT timetravelid_fk FOREIGN KEY (timetravelid) REFERENCES timetravel_pk(timetravelid)
) PARTITION BY RANGE (lower(changed))
;



-- Creating indexes
CREATE INDEX timetravel_changed_idx
  ON timetravel
  USING gist
  (changed)
;

CREATE INDEX timetravel_timetravelid_fk_idx
  ON timetravel
  USING btree
  (timetravelid)
;

CREATE INDEX timetravel_not_deleted_idx
  ON timetravel
  USING btree
  (deleted)
  WHERE NOT deleted
;



-- This table is needed to handle partition information.
-- It will later be used to check, if a partition has to be created
CREATE TABLE timetravel_part_vals (
  part_year SMALLINT NOT NULL,
  start_value TIMESTAMP WITH TIME ZONE NOT NULL,
  end_value TIMESTAMP WITH TIME ZONE NOT NULL,
  CONSTRAINT timetravel_part_vals_pk PRIMARY KEY (part_year)
);




/**FUNCTIONS*****************************************/

-- Function to handle partitions
CREATE OR REPLACE FUNCTION timetravel_partition (in_changed TIMESTAMPTZ)
  RETURNS void
  LANGUAGE PLPGSQL
  AS
$$
DECLARE
  query TEXT;
  in_range BOOLEAN = FALSE;
  year_p SMALLINT;
  start_v TIMESTAMP WITH TIME ZONE;
  end_v TIMESTAMP WITH TIME ZONE;
  part_name TEXT;
BEGIN
  -- Check the changed date to be in an existing partition
  EXECUTE 'SELECT count(*) > 0
  FROM timetravel_part_vals
  WHERE part_year = extract(year from $1)'
  INTO in_range
  USING in_changed;

  IF (NOT in_range) THEN
    -- Update the range data
    EXECUTE 'INSERT INTO timetravel_part_vals (part_year, start_value, end_value)
    SELECT extract(year from $1),
      date_trunc(''year'', $1),
      date_trunc(''year'', $1) + INTERVAL ''1 year''
    RETURNING *'
    INTO year_p, start_v, end_v
    USING in_changed;

    -- Create a new partition
    part_name := 'timetravel_' || year_p::TEXT;

    EXECUTE 'CREATE TABLE ' || part_name ||
      ' PARTITION OF timetravel FOR VALUES FROM (''' || start_v::text || ''') TO (''' || end_v::text || ''')';

    RAISE NOTICE 'Partition % created.', part_name;

  END IF;
END;
$$
;


-- Create the partition for the current year
SELECT timetravel_partition (now());





-- Trigger function for inserts and updates
CREATE OR REPLACE FUNCTION trigger_timetravel_in_upd ()
  RETURNS TRIGGER
  LANGUAGE PLPGSQL
  AS
$$
BEGIN
  -- Setting default values
  NEW.deleted = false;
  NEW.changed = tstzrange(clock_timestamp(), 'INFINITY', '[)');

  -- On UPDATE a new record is inserted
  CASE WHEN TG_OP = 'UPDATE' THEN

    IF NEW.timetravelid <> OLD.timetravelid THEN
      RAISE EXCEPTION 'The identity column timetravelid can not be changed!';
    END IF;

    IF NOT OLD.deleted THEN
      IF upper(OLD.changed) = 'infinity' THEN
        -- Updated the latest version of a record
        INSERT INTO timetravel (timetravelid, data_text, data_json, changed)
        SELECT NEW.timetravelid
          , NEW.data_text
          , NEW.data_json
          , NEW.changed
        ;

        -- Only the range for the old record is changed, it has an end now
        NEW.data_text = OLD.data_text;
        NEW.data_json = OLD.data_json;
        NEW.changed = tstzrange(lower(OLD.changed), lower(NEW.changed));

        RETURN NEW;
      ELSE
	    -- It is not the newest version, therefore there is nothing to do
        RETURN NULL;
      END IF;
    ELSE
      -- An already deleted record cannot be changed
      RETURN NULL;
    END IF;

  -- The new record needs its id created by inserting into the pk table
  WHEN TG_OP = 'INSERT' THEN
    INSERT INTO timetravel_pk (created)
      VALUES (clock_timestamp())
      RETURNING timetravelid
      INTO NEW.timetravelid;

  	RETURN NEW;
  ELSE
    RETURN NULL;
  END CASE;
END;
$$
;

-- Attach the trigger function for inserts
CREATE OR REPLACE TRIGGER timetravel_insert
  BEFORE INSERT
  ON timetravel
  FOR EACH ROW
  WHEN (pg_trigger_depth() < 1)
  EXECUTE PROCEDURE trigger_timetravel_in_upd()
;

-- Attach the trigger function for updates
CREATE OR REPLACE TRIGGER timetravel_update
  BEFORE UPDATE
  ON timetravel
  FOR EACH ROW
  EXECUTE PROCEDURE trigger_timetravel_in_upd()
;


-- The trigger inserts two records, one with the old data but with an end
-- timestamp in column changed, one with deleted = true but end timestamp
-- is INFINITY
CREATE OR REPLACE FUNCTION trigger_timetravel_del ()
  RETURNS TRIGGER
  LANGUAGE PLPGSQL
  AS
$$
DECLARE
  ts timestamp with time zone;
BEGIN
  -- When a record has already been deleted, an error is raised and no data is changed
  IF OLD.deleted THEN
     RAISE EXCEPTION 'Timetravel record with the timetravelid % is already deleted.', OLD.timetravelid;
  END IF;

  IF upper(OLD.changed) = 'infinity' THEN
  -- Only the latest version has to be taken care off
    ts = clock_timestamp();

    INSERT INTO timetravel (timetravelid, changed, data_text, data_json, deleted)
    VALUES (
      OLD.timetravelid,
      tstzrange(ts, 'INFINITY'),
      OLD.data_text,
      OLD.data_json,
      true
    ),
    (
      OLD.timetravelid,
      tstzrange(lower(OLD.changed), ts),
      OLD.data_text,
      OLD.data_json,
      false
    );

    RETURN OLD;
  ELSE
    -- All other records stay as they are
    RETURN NULL;
  END IF;
END;
$$
;

-- Attach the trigger function for deletions
CREATE OR REPLACE TRIGGER timetravel_delete
  BEFORE DELETE
  ON timetravel
  FOR EACH ROW
  EXECUTE PROCEDURE trigger_timetravel_del()
;



/**VIEWS*****************************************/

-- View
CREATE OR REPLACE VIEW timetravel_v AS
WITH rec_v AS
  (
    SELECT t.timetravelid
      , ROW_NUMBER() OVER (PARTITION BY t.timetravelid ORDER BY lower(t.changed)) AS rec_version
    FROM timetravel AS t
  )
SELECT DISTINCT ON (t.timetravelid)
  t.timetravelid
  , rec_v.rec_version
  , t.data_text
  , t.data_json
  , pk.created
  , t.changed
  , lower(t.changed) AS valid_from
  , upper(t.changed) AS valid_until
FROM timetravel_pk AS pk
INNER JOIN timetravel AS t
  ON pk.timetravelid = t.timetravelid
INNER JOIN rec_v
  ON pk.timetravelid = rec_v.timetravelid
WHERE NOT deleted
AND upper(t.changed) = 'infinity'::TIMESTAMPTZ
;

-- Querying view
SELECT *
FROM timetravel_v
ORDER BY timetravelid
;

-- Querying table
SELECT *
  , ROW_NUMBER() OVER (PARTITION BY t.timetravelid ORDER BY lower(changed)) AS rec_version
FROM timetravel AS t
ORDER BY timetravelid
	, rec_version
;



/**TEST*DATA*****************************************/


-- Insert records
INSERT INTO timetravel (data_text, data_json)
SELECT substr(md5(random()::text), 1, 25) AS data_text
	, to_jsonb(substr(md5(random()::text), 1, 25)) AS data_json
FROM generate_series(1, 100000) s(i)
;

-- Update existing records
WITH t1 AS
	(
		SELECT floor(random() * (100000-1+1) + 1)::bigint AS timetravelid
			, substr(md5(random()::text), 1, 25) AS data_text
			, to_jsonb(substr(md5(random()::text), 1, 25)) AS data_json
		FROM generate_series(1, 100000) s(i)
	)
, t2 AS
	(
		SELECT DISTINCT ON (timetravelid) timetravelid
			, data_text
			, data_json
		FROM t1
	)
UPDATE timetravel SET
	data_text = t2.data_text,
	data_json = t2.data_json
FROM t2
WHERE timetravel.timetravelid = t2.timetravelid
;



-- Delete some records
DELETE FROM timetravel WHERE timetravelid IN (99, 654, 5698);



-- Table statistics
WITH rec_v AS
  (
    SELECT t.timetravelid
      , ROW_NUMBER() OVER (PARTITION BY t.timetravelid ORDER BY lower(t.changed)) AS rec_version
    FROM timetravel AS t
  )
SELECT count(t.timetravelid) AS recordcount
  , min(t.timetravelid) AS min_primary_key
  , max(t.timetravelid) AS max_primary_key
  , min(rec_v.rec_version) AS min_version_number
  , max(rec_v.rec_version) AS max_version_number
  , count(t.timetravelid) FILTER (WHERE t.deleted) AS deleted_records
FROM timetravel AS t
INNER JOIN rec_v
  ON t.timetravelid = rec_v.timetravelid
;





-- Get current data
SELECT *
FROM timetravel_v AS tv
ORDER BY tv.timetravelid
;



-- Get all records of a deleted one
SELECT t.timetravelid
  , ROW_NUMBER() OVER (PARTITION BY t.timetravelid ORDER BY lower(t.changed)) AS rec_version
  , t.data_text
  , t.data_json
  , t.deleted
  , t.changed
  , lower(t.changed)
FROM timetravel AS t
WHERE t.timetravelid = 99
ORDER BY lower(t.changed)
;



-- Result for a certain point in time
SELECT t.timetravelid
  , ROW_NUMBER() OVER (PARTITION BY t.timetravelid ORDER BY lower(t.changed)) AS rec_version
  , t.data_text
  , t.data_json
  , pk.created
  , t.changed
  , lower(t.changed) AS valid_from
  , upper(t.changed) AS valid_until
  , t.deleted
FROM timetravel_pk AS pk
INNER JOIN timetravel AS t
  ON pk.timetravelid = t.timetravelid
WHERE '2024-11-16 09:45:06.821 +0100'::timestamptz <@ t.changed
ORDER BY t.timetravelid
  , lower(t.changed)
;





/**BONUS:*TRIGGER*ON*VIEW****************************/

-- One trigger to bind them all
CREATE OR REPLACE FUNCTION trigger_timetravel_view_in_upd_del ()
  RETURNS TRIGGER
  LANGUAGE PLPGSQL
  AS
$$
BEGIN
  CASE
    WHEN TG_OP = 'INSERT' THEN
      EXECUTE 'INSERT INTO timetravel (data_text, data_json) VALUES ($1, $2)'
        USING NEW.data_text, NEW.data_json;
    WHEN TG_OP = 'UPDATE' THEN
      EXECUTE 'UPDATE timetravel SET data_text = $1, data_json = $2 WHERE timetravelid = $3'
        USING NEW.data_text, NEW.data_json, OLD.timetravelid;
    WHEN TG_OP = 'DELETE' THEN
      EXECUTE 'DELETE FROM timetravel WHERE timetravelid = $1'
        USING OLD.timetravelid;
    ELSE
      RAISE EXCEPTION 'Operation not supported.';
  END CASE;

  RETURN NEW;
END;
$$
;

-- Attach the trigger function to the view
CREATE OR REPLACE TRIGGER timetravel_v_trigger
	INSTEAD OF INSERT OR UPDATE OR DELETE
	ON timetravel_v
	FOR EACH ROW
	EXECUTE PROCEDURE trigger_timetravel_view_in_upd_del ()
;



-- Query the view
SELECT *
FROM timetravel_v
;



-- Deleting from the view
DELETE
FROM timetravel_v
WHERE timetravelid = 118
;



-- Query the deleted record
SELECT *
FROM timetravel_v
WHERE timetravelid = 118
ORDER BY rec_version
;


SELECT t.timetravelid
  , ROW_NUMBER() OVER (PARTITION BY t.timetravelid ORDER BY lower(t.changed)) AS rec_version
  , t.data_text
  , t.data_json
  , t.deleted
  , t.changed
  , lower(t.changed) AS valid_from
FROM timetravel AS t
WHERE t.timetravelid = 118
ORDER BY lower(t.changed)
;



-- Check records before update
SELECT t.timetravelid
  , ROW_NUMBER() OVER (PARTITION BY t.timetravelid ORDER BY lower(t.changed)) AS rec_version
  , t.data_text
  , t.data_json
  , t.deleted
  , t.changed
  , lower(t.changed) AS valid_from
FROM timetravel AS t
WHERE t.timetravelid = 324
ORDER BY lower(t.changed) DESC
;



-- Update a record
UPDATE timetravel_v SET
  data_text = substr(md5(random()::text), 1, 25),
  data_json = to_jsonb(substr(md5(random()::text), 1, 25))
WHERE timetravelid = 324
;



-- Query the updated record
SELECT now() - valid_from AS changed_diff
  , *
FROM timetravel_v
WHERE timetravelid = 324
;



SELECT t.timetravelid
  , ROW_NUMBER() OVER (PARTITION BY t.timetravelid ORDER BY lower(t.changed)) AS rec_version
  , t.data_text
  , t.data_json
  , t.deleted
  , t.changed
  , lower(t.changed) AS valid_from
  , now() - lower(t.changed) AS changed_diff
FROM timetravel AS t
WHERE t.timetravelid = 324
ORDER BY lower(t.changed) DESC
;



-- Remember the deleted record?
SELECT t.timetravelid
  , ROW_NUMBER() OVER (PARTITION BY t.timetravelid ORDER BY lower(t.changed)) AS rec_version
  , t.data_text
  , t.data_json
  , t.deleted
  , t.changed
  , lower(t.changed) AS valid_from
FROM timetravel AS t
WHERE t.timetravelid = 118
ORDER BY lower(t.changed) DESC

;



-- Update an already deleted record with the view
UPDATE timetravel_v SET
  data_text = substr(md5(random()::text), 1, 25),
  data_json = to_jsonb(substr(md5(random()::text), 1, 25))
WHERE timetravelid = 118
;



-- Update an already deleted with the table
UPDATE timetravel SET
  data_text = substr(md5(random()::text), 1, 25),
  data_json = to_jsonb(substr(md5(random()::text), 1, 25))
WHERE timetravelid = 118
;
