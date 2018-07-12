DROP EXTENSION IF EXISTS sqlite_fdw CASCADE;
-- Create the SQLite foreign data wrapper extension in the current database
CREATE EXTENSION sqlite_fdw;






-- Create the mapping to the foreign SQLite file
CREATE SERVER sqlite_server
	FOREIGN DATA WRAPPER sqlite_fdw
	OPTIONS (database '/var/sqlite/Chinook_Sqlite.sqlite')
;






-- Create the SQLite foreign table, column definitions have to match
CREATE FOREIGN TABLE sqlite_artist(
	"ArtistId" integer,
	"Name" character varying(120)
)
SERVER sqlite_server
OPTIONS(
	table 'Artist'
);






--  Select some data
SELECT * FROM sqlite_artist;





-- Join SQLite with PostgreSQL 10
SELECT artist."Name"
	, album."Title"
FROM sqlite_artist AS artist
INNER JOIN "Album" AS album
	ON artist."ArtistId" = album."ArtistId"
;





-- Select data from a different PostgreSQL database
-- Should not work!
SELECT * FROM chinook.public."Track";
SELECT * FROM "Track";





DROP EXTENSION IF EXISTS postgres_fdw CASCADE;
-- Create the PostgreSQL extension to link other PostgreSQL databases
CREATE EXTENSION postgres_fdw;





-- Create a connection to the other database PostgreSQL on the same server (9.6)
CREATE SERVER pg_localhost_chinook
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (host '127.0.0.1', port '5432', dbname 'chinook')
;





-- Create a user mapping
CREATE USER MAPPING FOR stefanie
        SERVER pg_localhost_chinook
        OPTIONS (user 'stefanie', password 'password')
;





DROP SCHEMA IF EXISTS pg96 CASCADE; 
CREATE SCHEMA pg96;




 
-- Link foreign tables into the current database and schema
IMPORT FOREIGN SCHEMA public LIMIT TO("Track")
FROM SERVER pg_localhost_chinook
INTO pg96
;





-- Try to select some data
SELECT * FROM "Track";





-- Join SQLite and PostgreSQL tables
SELECT artist."Name"
	, album."Title"
	, track."Name"
FROM sqlite_artist AS artist
INNER JOIN "Album" AS album
	ON artist."ArtistId" = album."ArtistId"
INNER JOIN pg96."Track" AS track
	ON album."AlbumId" = track."AlbumId"
;






DROP EXTENSI,,,,ON IF EXISTS file_fdw CASCADE;
-- Create the fi
CREATE EXTENSION file_fdw;





-- One does need a server, but afterwards every csv file is avilable
CREATE SERVER chinook_csv
	FOREIGN DATA WRAPPER file_fdw
;





-- Creating a foreign table based on a csv file
-- Options are the same as in COPY
CREATE FOREIGN TABLE csv_genre (
	"GenreId" integer,
	"Name" text
) SERVER chinook_csv
OPTIONS (
	filename '/var/sqlite/Genre.csv',
	format 'csv',
	HEADER 'true'
);





-- Select some data
SELECT * FROM csv_genre;





-- Join SQLite, two PostgreSQL servers, and a CSV tables
SELECT artist."Name"
	, album."Title"
	, track."Name"
	, genre."Name"
FROM sqlite_artist AS artist
INNER JOIN "Album" AS album
	ON artist."ArtistId" = album."ArtistId"
INNER JOIN pg96."Track" AS track
	ON album."AlbumId" = track."AlbumId"
INNER JOIN csv_genre AS genre
	ON track."GenreId" = genre."GenreId"
;







DROP MATERIALIZED VIEW IF EXISTS mv_album_artist;
-- Creates an materialized view on foreign tables
CREATE MATERIALIZED VIEW mv_album_artist AS
WITH album AS
	(
		SELECT "ArtistId"
			, array_agg("Title") AS album_titles
		FROM "Album"
		GROUP BY "ArtistId"
	)
SELECT artist."Name" AS artist
	, album.album_titles
	, SUM(ARRAY_LENGTH(album_titles, 1))
FROM sqlite_artist AS artist
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
CREATE SERVER rss_srv foreign data wrapper multicorn options (
    wrapper 'multicorn.rssfdw.RssFdw'
)
;





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





-- Select some data from an RSS Feed from the web
SELECT * FROM rss_mi2nbandnews;





-- Link the previous RSS feed to existing data
SELECT *
FROM rss_mi2nbandnews AS r
INNER JOIN sqlite_artist AS a
	ON r.description ilike '%' || a."Name" || '%'
;





-- Create a foreign table based on an RSS feed
CREATE FOREIGN TABLE rss_postgresql_events (
	title CHARACTER VARYING,	
	link CHARACTER VARYING,
	description CHARACTER VARYING,
	"pubDate" TIMESTAMPTZ,
	guid CHARACTER VARYING
) server rss_srv OPTIONS (
	url 'https://www.postgresql.org/events.rss'
)
;





-- Entend the query of the RSS feed
SELECT title
	, "pubDate"::DATE AS "Conference Start Date"
	, description
FROM rss_postgresql_events
WH
ERE "pubDate"::DATE > NOW()::DATE
ORDER BY "pubDate" ASC
;





