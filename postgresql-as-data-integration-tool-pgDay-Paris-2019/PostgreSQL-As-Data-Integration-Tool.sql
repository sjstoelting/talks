DROP EXTENSION IF EXISTS sqlite_fdw CASCADE;
-- Create the SQLite foreign data wrapper extension in the current database
CREATE EXTENSION sqlite_fdw;






-- Create the mapping to the foreign SQLite file
-- IMPORTANT:
-- Take care that the postgres user has the rights to write
-- not only on the SQLite file, but on the folder, too
CREATE SERVER sqlite_server_srv
	FOREIGN DATA WRAPPER sqlite_fdw
	OPTIONS (database '/var/sqlite/Chinook_Sqlite.sqlite')
;












DROP SCHEMA IF EXISTS chinook_sqlite CASCADE;
-- Create a schema where the SQLite table is stored
CREATE SCHEMA chinook_sqlite;






-- Create the SQLite foreign table, column definitions have to match
CREATE FOREIGN TABLE chinook_sqlite."Artist"(
	"ArtistId" integer OPTIONS (key 'true'),
	"Name" text
)
SERVER sqlite_server_srv
OPTIONS(
	table 'Artist'
);






-- Select some data
SELECT *
FROM chinook_sqlite."Artist"
;





-- Update one record inside the SQLite DB
UPDATE chinook_sqlite."Artist" SET
	"Name" = lower("Name")
WHERE "ArtistId" = 1
;






-- Select the updated record
SELECT *
FROM chinook_sqlite."Artist"
WHERE "ArtistId" = 1
;






-- Revert the update
UPDATE chinook_sqlite."Artist" SET
	"Name" = 'AC/DC'
WHERE "ArtistId" = 1
;






-- Check the revert of the updated record
SELECT *
FROM chinook_sqlite."Artist"
WHERE "ArtistId" = 1
;






DROP EXTENSION IF EXISTS mysql_fdw CASCADE;
-- Create the foreign data wrapper extension in the current database
CREATE EXTENSION mysql_fdw;






-- Create the mapping to the foreign MariaDB server
CREATE SERVER mariadb_server_srv
	FOREIGN DATA WRAPPER mysql_fdw
	OPTIONS (host '127.0.0.1', port '3306')
;






-- Create a user mapping with user and password of the foreign table
-- PostgreSQL gives you options to connect this user with its own users
CREATE USER MAPPING FOR PUBLIC SERVER mariadb_server_srv
OPTIONS (username 'stefanie', password 'secret')
;






DROP SCHEMA IF EXISTS chinook_mariadb CASCADE;
-- Create a schema where the MariaDB table is stored
CREATE SCHEMA chinook_mariadb;






-- Create the MariaDB foreign table, column definitions have to match
CREATE FOREIGN TABLE chinook_mariadb."Album"(
	"AlbumId" integer,
	"Title" character varying(160),
	"ArtistId" integer
)
SERVER mariadb_server_srv
OPTIONS(
	dbname 'Chinook',
	table_name 'Album'
);






-- Select some data
SELECT *
FROM chinook_mariadb."Album"
;






-- Join SQLite with MariaDB
SELECT artist."Name"
	, album."Title"
FROM chinook_sqlite."Artist" AS artist
INNER JOIN chinook_mariadb."Album" AS album
	ON artist."ArtistId" = album."ArtistId"
;






-- Select one album to check it before an update
SELECT *
FROM chinook_mariadb."Album"
WHERE "AlbumId" = 1
;






-- Update one album in MariaDB from PostgreSQL
UPDATE chinook_mariadb."Album" SET
"Title" = 'Updated by PostgreSQL'
WHERE "AlbumId" = 1
;






-- Control the result of the updated album in MariaDB
SELECT *
FROM chinook_mariadb."Album"
WHERE "AlbumId" = 1
;






-- Revert the update in MariaDB by the same record in PostgreSQL
UPDATE chinook_mariadb."Album" AS mariadb_album SET
"Title" = pg_album."Title"
FROM "Album" AS pg_album
WHERE pg_album."AlbumId" = mariadb_album."AlbumId"
	AND mariadb_album."AlbumId" = 1
;






-- Control the result of the updated album in MariaDB
SELECT *
FROM chinook_mariadb."Album"
WHERE "AlbumId" = 1
;






DROP EXTENSION IF EXISTS postgres_fdw CASCADE;
-- Create the PostgreSQL extension to link other PostgreSQL databases
CREATE EXTENSION postgres_fdw;






-- Create a connection to the other database PostgreSQL on the same server (9.6)
CREATE SERVER postgresql_9_6_localhost_chinook_srv
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (host '127.0.0.1', port '5432', dbname 'chinook')
;






-- Create a user mapping
CREATE USER MAPPING FOR stefanie
        SERVER postgresql_9_6_localhost_chinook_srv
        OPTIONS (user 'stefanie', password 'password')
;






DROP SCHEMA IF EXISTS chinook_postgresql_9_6 CASCADE; 
-- Create a schema where the PostgreSQL 9.6 table is stored
CREATE SCHEMA chinook_postgresql_9_6;






-- Link foreign tables into the current database and schema
IMPORT FOREIGN SCHEMA public LIMIT TO("Track")
FROM SERVER postgresql_9_6_localhost_chinook_srv
INTO chinook_postgresql_9_6
;






-- Try to select some data
SELECT *
FROM chinook_postgresql_9_6."Track"
;






-- Join SQLite and PostgreSQL tables
SELECT artist."Name"
	, album."Title"
	, track."Name"
FROM chinook_sqlite."Artist" AS artist
INNER JOIN chinook_mariadb."Album" AS album
	ON artist."ArtistId" = album."ArtistId"
INNER JOIN chinook_postgresql_9_6."Track" AS track
	ON album."AlbumId" = track."AlbumId"
;






DROP EXTENSION IF EXISTS file_fdw CASCADE;
-- Create the file extension
CREATE EXTENSION file_fdw;






-- One does need a server, but afterwards every csv file is avilable
CREATE SERVER chinook_csv_srv
	FOREIGN DATA WRAPPER file_fdw
;






DROP SCHEMA IF EXISTS chinook_csv CASCADE;
-- Create a schema where the CSV table is stored
CREATE SCHEMA chinook_csv;






-- Creating a foreign table based on a csv file
-- Options are the same as in COPY
CREATE FOREIGN TABLE chinook_csv."Genre" (
	"GenreId" integer,
	"Name" text
) SERVER chinook_csv_srv
OPTIONS (
	FILENAME '/var/sqlite/Genre.csv',
	FORMAT 'csv',
	HEADER 'true'
)
;






-- Select some data
SELECT *
FROM chinook_csv."Genre"
;






-- Join SQLite, two PostgreSQL servers, and a CSV tables
SELECT artist."Name"
	, album."Title"
	, track."Name"
	, genre."Name"
FROM chinook_sqlite."Artist" AS artist
INNER JOIN chinook_mariadb."Album" AS album
	ON artist."ArtistId" = album."ArtistId"
INNER JOIN chinook_postgresql_9_6."Track" AS track
	ON album."AlbumId" = track."AlbumId"
INNER JOIN chinook_csv."Genre" AS genre
	ON track."GenreId" = genre."GenreId"
;






DROP MATERIALIZED VIEW IF EXISTS mv_album_artist;
-- Creates an materialized view on foreign tables
CREATE MATERIALIZED VIEW mv_album_artist AS
WITH album AS
	(
		SELECT "ArtistId"
			, array_agg("Title") AS album_titles
		FROM chinook_mariadb."Album"
		GROUP BY "ArtistId"
	)
SELECT artist."Name" AS artist
	, album.album_titles
	, SUM(ARRAY_LENGTH(album_titles, 1))
FROM chinook_sqlite."Artist" AS artist
LEFT OUTER JOIN album
	ON artist."ArtistId" = album."ArtistId"
GROUP BY artist."Name"
	, album.album_titles
;






-- Select Data from the materialzed view
SELECT *
FROM mv_album_artist
WHERE upper(artist) LIKE 'A%'
ORDER BY artist
;






-- Multicorn Examples
DROP EXTENSION IF EXISTS multicorn CASCADE;
-- Create the multicorn extension
CREATE EXTENSION multicorn;






-- Create the server, which is simply a placeholder
CREATE SERVER rss_srv foreign data wrapper multicorn OPTIONS
(
    wrapper 'multicorn.rssfdw.RssFdw'
)
;






DROP FOREIGN TABLE IF EXISTS rss_music_news;
-- Create a foreign table based on an RSS feed
CREATE FOREIGN TABLE rss_music_news (
	title CHARACTER VARYING,
	link CHARACTER VARYING,
	description CHARACTER VARYING,
	"pubDate" TIMESTAMPTZ,
	guid CHARACTER VARYING
) server rss_srv OPTIONS (
	url 'http://www.music-news.com/rss/UK/news?includeCover=false'
)
;






-- Select some data
SELECT *
FROM rss_music_news
;






-- Link the previous RSS feed to existing data
SELECT a."Name"
	, r.title
	, r.description
FROM rss_music_news AS r
	INNER JOIN chinook_sqlite."Artist" AS a
		ON r.title ilike '%' || a."Name" || '%'
;






DROP FOREIGN TABLE IF EXISTS rss_rolling_stone;
-- Create a foreign table based on an RSS feed
CREATE FOREIGN TABLE rss_rolling_stone (
	title CHARACTER VARYING,	
	link CHARACTER VARYING,
	"content:encoded" CHARACTER VARYING,
	"pubDate" TIMESTAMPTZ,
	guid CHARACTER VARYING
) server rss_srv OPTIONS (
	url 'http://www.rollingstone.com/music/rss'
)
;






SELECT *
FROM rss_rolling_stone
;






-- Link the previous RSS feed to existing data
SELECT *
FROM chinook_sqlite."Artist" AS a
	INNER JOIN rss_rolling_stone AS r
		ON r.title ilike '%' || a."Name" || '%'
;






-- Create a foreign table based on an RSS feed
CREATE FOREIGN TABLE rss_mi2nbandnews (
	title CHARACTER VARYING,	
	link CHARACTER VARYING,
	description CHARACTER VARYING,
	"pubDate" TIMESTAMPTZ,
	guid CHARACTER VARYING
) server rss_srv OPTIONS (
	url 'http://feeds.feedburner.com/mi2nbandnews'
)
;






SELECT * FROM rss_mi2nbandnews;






-- Create a foreign table based on an RSS feed
CREATE FOREIGN TABLE rss_mi2neventnews (
	title CHARACTER VARYING,	
	link CHARACTER VARYING,
	description CHARACTER VARYING,
	"pubDate" TIMESTAMPTZ,
	guid CHARACTER VARYING
) server rss_srv OPTIONS (
	url 'http://feeds.feedburner.com/mi2nmusicevents'
)
;






SELECT * FROM rss_mi2nbandnews;






DROP MATERIALIZED VIEW IF EXISTS mv_rss_music_newslists CASCADE;
-- This materialized view will contain the results of all RSS music feeds
CREATE MATERIALIZED VIEW mv_rss_music_newslists AS
SELECT current_timestamp AS refreshed
	, ROW_NUMBER()OVER() AS rn
	, 'Rolling Stone' AS source
	, 'http://www.rollingstone.com/music/rss' AS url
	, r.title
	, r."content:encoded" AS content
	, TRUE AS encoded
	, r.link
	, r."pubDate" AS published
FROM rss_rolling_stone AS r
UNION
SELECT current_timestamp AS refreshed
	, ROW_NUMBER()OVER() AS rn
	, 'Music-News' AS source
	, 'http://www.music-news.com/rss/UK/news?includeCover=false' AS url
	, r.title
	, r.description AS content
	, FALSE AS encoded
	, r.link
	, r."pubDate" AS publihed
FROM rss_music_news AS r
UNION
SELECT current_timestamp AS refreshed
	, ROW_NUMBER()OVER() AS rn
	, 'Music Industry News Network: Band News' AS source
	, 'http://feeds.feedburner.com/mi2nbandnews' AS url
	, r.title
	, r.description AS content
	, FALSE AS encoded
	, r.link
	, r."pubDate" AS publihed
FROM rss_mi2nbandnews AS r
UNION
SELECT current_timestamp AS refreshed
	, ROW_NUMBER()OVER() AS rn
	, 'Music Industry News Network: Event News' AS source
	, 'http://feeds.feedburner.com/mi2nmusicevents' AS url
	, r.title
	, r.description AS content
	, FALSE AS encoded
	, r.link
	, r."pubDate" AS publihed
FROM rss_mi2neventnews AS r
;






-- The unique index will help to refresh the materilized view
CREATE UNIQUE INDEX udx_mv_rss_music_newslists_source_rn
	ON mv_rss_music_newslists USING btree
	(source, rn)
;






-- The two materialized views are joined and return data
-- from a lot of different sources, but this time all queried
-- inside PostgreSQL
SELECT *
FROM mv_rss_music_newslists AS r
	INNER JOIN mv_album_artist AS a
		ON r.title ilike '%' || a.artist || '%'	
		OR r.content ilike '%' || a.artist || '%'	
;






-- Installing the pg_cron extension to schedule jobs in PostgreSQL
-- https://github.com/citusdata/pg_cron
DROP EXTENSION IF EXISTS pg_cron;
CREATE EXTENSION pg_cron;






DROP TABLE IF EXISTS cron.log;
-- The table is used for logging calls
CREATE TABLE cron.log (
	log_id bigserial NOT NULL,
	time_stamp_begin timestamp WITH time ZONE NOT NULL,
	time_stamp_end timestamp WITH time ZONE NOT NULL,
	executed text NOT NULL,
	CONSTRAINT log_pk PRIMARY KEY (log_id) 
)
;






DROP PROCEDURE IF EXISTS refresh_every_minute();
-- The procedure will be used to log it's calls into the recently created log table
CREATE OR REPLACE PROCEDURE refresh_every_minute() AS $$
DECLARE
	ts_start timestamp WITH time ZONE DEFAULT current_timestamp;
BEGIN
	-- Refresh the materialized view concurrently to keep it available
	REFRESH MATERIALIZED VIEW CONCURRENTLY mv_rss_music_newslists;

	-- Write a log entry into the log table
	INSERT INTO cron.log (executed, time_stamp_begin, time_stamp_end)
	VALUES (
		'REFRESH MATERIALIZED VIEW CONCURRENTLY mv_rss_music_newslists',
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
VALUES ('* * * * *', 'CALL refresh_every_minute()', '', 5434, 'chinook', 'postgres')
;







SELECT * FROM cron.job;






-- Select the log written by the procedure
SELECT * FROM cron.log;






-- The refresh is running transparent in the background
SELECT *
FROM mv_rss_music_newslists
;





-- Remove the scheduled job
SELECT cron.unschedule(1);






-- Create a foreign table based on an RSS feed
CREATE FOREIGN TABLE rss_postgresql_events (
	title text,
	link text,
	description text,
	"pubDate" TIMESTAMPTZ,
	guid text
) server rss_srv OPTIONS (
	url 'https://www.postgresql.org/events.rss'
)
;






-- PostgreSQL Conferences RSS feed
SELECT title
	, "pubDate"::DATE AS "Conference Start Date"
	, strip_tags(description)
FROM rss_postgresql_events
WHERE "pubDate"::DATE > NOW()::DATE
ORDER BY "pubDate" ASC
;






-- Don't use this function in production,
-- it might not return what you expect ;-)
CREATE OR REPLACE FUNCTION strip_tags(TEXT) RETURNS TEXT AS
$$
	WITH t1 AS
		(
    		SELECT regexp_replace($1, '<[^>]*>', '', 'g') AS res
    	)
    SELECT regexp_replace(res, E'[\\n\\r]+', ' ', 'g' )
    FROM t1
   ;
$$ LANGUAGE SQL
;

