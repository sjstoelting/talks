/**
 * This script is part of the talk PostgreSQL As Data Integration Tool
 * Given at the PostgreSQL Meetup in Berlin on 2025-06-19
 * 
 * The following extensions are needed:
 * - sqlite_fdw:   https://github.com/pgspider/sqlite_fdw/
 * - postgres_fdw: https://www.postgresql.org/docs/current/postgres-fdw.html
 * - file_fdw:     https://www.postgresql.org/docs/current/file-fdw.html
 * - multicorn2:   https://github.com/pgsql-io/multicorn2
 * - pg_cron:      https://github.com/citusdata/pg_cron
 *
 * Author:  Stefanie Janine Stölting, stefanie@proopensource.eu
 * License: Creative Commons Attribution 4.0 International
 *          http://creativecommons.org/licenses/by/4.0/
 */

-- Create the SQLite foreign data wrapper extension in the current database
CREATE EXTENSION sqlite_fdw;






-- Create a mapping for the SQLite file as server
-- Important:
-- The user postgres must have read and write access
-- not only on the file but also on the folder
CREATE SERVER sqlite_server
	FOREIGN DATA WRAPPER sqlite_fdw
	OPTIONS (database '/var/sqlite/Chinook_Sqlite.sqlite')
;





-- Create a schema for the SQLite tables
CREATE SCHEMA IF NOT EXISTS sqlite;





-- Create a foreing table pointing to the SQLite database
-- The column definition must be correct to the one in SQLite
CREATE FOREIGN TABLE sqlite.artist(
	"ArtistId" integer,
	"Name" text
)
SERVER sqlite_server
OPTIONS(
	table 'Artist'
);






--  Query data
SELECT * FROM sqlite.artist;





-- Join SQLite with PostgreSQL 17
SELECT artist."Name" AS album_name
	, album.title
FROM sqlite.artist AS artist
INNER JOIN public.album
	ON artist."ArtistId" = album.artist_id 
;





-- Create the PostgreSQL foreign data wrapper
CREATE EXTENSION postgres_fdw;





-- Create a connection to another PostgreSQL instance
-- This one is a PostgreSQL 14
CREATE SERVER pg_localhost_chinook
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (host '127.0.0.1', port '5432', dbname 'chinook')
;





-- Create a user for the user mapping
CREATE USER MAPPING FOR stefanie
        SERVER pg_localhost_chinook
        OPTIONS (user 'stefanie', password 'password')
;





-- Create a schema for the foreign PostgreSQL tables
CREATE SCHEMA IF NOT EXISTS pg14;




 
-- Create the foreign table
IMPORT FOREIGN SCHEMA public LIMIT TO("Track")
FROM SERVER pg_localhost_chinook
INTO pg14
;





-- Show data from PostgreSQL 14
SELECT * FROM pg14."Track";





-- Join SQLite and PostgreSQL tables
SELECT artist."Name"
	, album.title
	, track."Name"
FROM sqlite.artist AS artist
INNER JOIN public.album
	ON artist."ArtistId" = album.artist_id
INNER JOIN pg14."Track" AS track
	ON album.album_id = track."AlbumId"
;






-- Update data in a foreign table
UPDATE pg14."Track" SET
	"Name" = 'Der Track Name ist geändert!'
WHERE "TrackId" = 6;







-- Show the changed data
SELECT *
FROM pg14."Track"
WHERE "TrackId" = 6;







-- Update data in a foreign table
UPDATE pg14."Track" SET
	"Name" = 'Put The Finger On You'
WHERE "TrackId" = 6;







-- Show the changed data
SELECT *
FROM pg14."Track" SET
WHERE "TrackId" = 6;







-- Create the file extension
CREATE EXTENSION file_fdw;





-- It does need a server object, that can be used forall CSV files
-- The user postgres must have read and write access
-- not only on the file but also on the folder
CREATE SERVER chinook_csv
	FOREIGN DATA WRAPPER file_fdw
;





-- Create a schema for the CSV files
CREATE SCHEMA IF NOT EXISTS CSV;




-- Create a foreign table based on a CSV file
-- The options are the same as of the COPY command
CREATE FOREIGN TABLE csv.genre (
	"GenreId" integer,
	"Name" text
) SERVER chinook_csv
OPTIONS (
	filename '/var/sqlite/Genre.csv',
	format 'csv',
	HEADER 'true'
);




-- Show data
SELECT * FROM csv.genre;





-- Join data from SQLite, zwei PostgreSQL Servern, and
-- a CSV file
SELECT artist."Name"
	, album.title
	, track."Name"
	, genre."Name"
FROM sqlite.artist AS artist
INNER JOIN public.album
	ON artist."ArtistId" = album.artist_id 
INNER JOIN pg14."Track" AS track
	ON album.album_id = track."AlbumId"
INNER JOIN csv.genr.alie AS genre
	ON track."GenreId" = genre."GenreId"
;







-- Create a materialized view with foreign tables
CREATE MATERIALIZED VIEW mv_album_artist AS
WITH album AS
	(
		SELECT artist_id
			, array_agg(title) AS album_titles
		FROM public.album
		GROUP BY artist_id
	)
SELECT artist."Name" AS artist
	, album.album_titles
	, SUM(ARRAY_LENGTH(album_titles, 1))
FROM sqlite.artist AS artist
LEFT OUTER JOIN album
	ON artist."ArtistId" = album.artist_id
GROUP BY artist."Name"
	, album.album_titles
;






-- Show data
SELECT *
FROM mv_album_artist
WHERE upper(artist) LIKE 'A%'
ORDER BY artist
;







-- Multicorn Examples
-- Create the Multicorn extension
CREATE EXTENSION multicorn;





-- Create a server, it is only a place holder
CREATE SERVER rss_srv foreign data wrapper multicorn options (
    wrapper 'multicorn.rssfdw.RssFdw'
)
;





-- Create a schema for the multicorn tables
CREATE SCHEMA IF NOT EXISTS multicorn;





-- Create a foreign table based on a RSS feed
CREATE FOREIGN TABLE multicorn.rss_mi2nbandnews (
	title text,
	link text,
	description text,
	"pubDate" TIMESTAMPTZ,
	guid text
) server rss_srv OPTIONS (
	url 'https://www.visions.de/feeds/news.rss'
)
;





-- Show data from an RSS feed directly queried from the web
SELECT *
FROM multicorn.rss_mi2nbandnews;





-- Join the RSS data with an existing table
SELECT a."Name" AS artist
	, r.*
FROM multicorn.rss_mi2nbandnews AS r
INNER JOIN sqlite.artist AS a
	ON r.description ilike '%' || a."Name" || '%'
;





-- Create a materialized view
CREATE MATERIALIZED VIEW multicorn.rss_mi2nbandnews_mv AS
SELECT ROW_NUMBER() OVER() AS rn
	, r.title
	, r.link
	, r.description
	, r."pubDate" AS data_publication
	, r.guid
FROM multicorn.rss_mi2nbandnews AS r;




-- Create a unique index on the materialized view
CREATE UNIQUE INDEX rss_mi2nbandnews_mv_udx
	ON multicorn.rss_mi2nbandnews_mv
	USING btree
		(
			rn
		);




-- Show data
SELECT *
FROM multicorn.rss_mi2nbandnews_mv
;





-- Link the RSS data in the materialized view with a table
SELECT a."Name" AS artist
	, r.*
FROM multicorn.rss_mi2nbandnews_mv AS r
INNER JOIN sqlite.artist AS a
	ON r.description ilike '%' || a."Name" || '%'
;





-- Create a cron extension to schedule refreshs
CREATE EXTENSION pg_cron;




-- This table is used to log cron calls
CREATE TABLE cron.log (
	log_id bigserial NOT NULL,
	time_stamp_begin timestamp WITH time ZONE NOT NULL,
	time_stamp_end timestamp WITH time ZONE NOT NULL,
	executed text NOT NULL,
	CONSTRAINT log_pk PRIMARY KEY (log_id) 
)
;






-- Create a procedure to refresh and log the call
CREATE OR REPLACE PROCEDURE refresh_every_minute() AS $$
DECLARE
	ts_start timestamp WITH time ZONE DEFAULT current_timestamp;
BEGIN
	-- Refresh the materialized view
	REFRESH MATERIALIZED VIEW CONCURRENTLY multicorn.rss_mi2nbandnews_mv;

	-- Add a log entry
	INSERT INTO cron.log (executed, time_stamp_begin, time_stamp_end)
	VALUES (
		'REFRESH MATERIALIZED VIEW CONCURRENTLY multicorn.rss_mi2nbandnews_mv',
		ts_start,
		current_timestamp
	);
END;
$$
LANGUAGE plpgsql
;






-- Create a cron job inside PostgreSQL that will refresh the
-- Materialized view with RSS feed data every one minute
INSERT INTO cron.job (schedule, command, nodename, nodeport, database, username)
VALUES (
	'* * * * *',
	'CALL refresh_every_minute()',
	'',
	5435,
	'chinook',
	'postgres'
);






-- Show the cron job
SELECT *
FROM cron.job;




-- Show the cron execution in the log table
SELECT *
FROM cron.log;




-- With the job id one can cancle a cron job
SELECT cron.unschedule(1);




-- Create another foreign table from a RSS feed
CREATE FOREIGN TABLE multicorn.rss_postgresql_events (
	title text,
	link text,
	description text,
	"pubDate" TIMESTAMPTZ,
	guid text
) server rss_srv OPTIONS (
	url 'https://www.postgresql.org/events.rss'
);





-- Show the upcoming PostgreSQL events
SELECT title
	, "pubDate"::DATE AS "Conference Start Date"
	, description
FROM multicorn.rss_postgresql_events
WHERE "pubDate"::DATE > NOW()::DATE
ORDER BY "pubDate" ASC
;





