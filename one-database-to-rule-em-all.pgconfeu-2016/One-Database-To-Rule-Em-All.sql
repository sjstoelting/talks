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





DROP EXTENSION IF EXISTS mysql_fdw CASCADE;
-- Create the foreign data wrapper extension in the current database
CREATE EXTENSION mysql_fdw;





-- Create the mapping to the foreign MariaDB server
CREATE SERVER mariadb_server
	FOREIGN DATA WRAPPER mysql_fdw
	OPTIONS (host '127.0.0.1', port '3306')
;





-- Create a user mapping with user and password of the foreign table
-- PostgreSQL gives you options to connect this user with its own users
CREATE USER MAPPING FOR PUBLIC SERVER mariadb_server
OPTIONS (username 'stefanie', password 'secret')
;





-- Create the MariaDB foreign table, column definitions have to match
CREATE FOREIGN TABLE mysql_album(
	"AlbumId" integer,
	"Title" character varying(160),
	"ArtistId" integer
)
SERVER mariadb_server
OPTIONS(
	dbname 'Chinook',
	table_name 'Album'
);





--  Select some data
SELECT  * FROM mysql_album;





-- Join SQLite with MariaDB
SELECT artist."Name"
	, album."Title"
FROM sqlite_artist AS artist
INNER JOIN mysql_album AS album
	ON artist."ArtistId" = album."ArtistId"
;





-- Select data from a different PostgreSQL database
-- Should not work!
SELECT * FROM chinook.public."Track";
SELECT * FROM "Track";




 
DROP EXTENSION IF EXISTS postgres_fdw CASCADE;
-- Create the PostgreSQL extension to link other PostgreSQL databases
CREATE EXTENSION postgres_fdw;





-- Create a connection to the other database on the same server
CREATE SERVER pg_localhost_chinook
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (host '127.0.0.1', port '5432', dbname 'chinook')
;





-- Create a user mapping
CREATE USER MAPPING FOR stefanie
        SERVER pg_localhost_chinook
        OPTIONS (user 'stefanie', password 'password')
;





-- Link foreign tables into the current database and schema
IMPORT FOREIGN SCHEMA public LIMIT TO("Track")
FROM SERVER pg_localhost_chinook
INTO public
;





-- Try to select some data
SELECT * FROM "Track";





-- Join SQLite, MariaDB, and PostgreSQL tables
SELECT artist."Name"
	, album."Title"
	, track."Name"
FROM sqlite_artist AS artist
INNER JOIN mysql_album AS album
	ON artist."ArtistId" = album."ArtistId"
INNER JOIN "Track" AS track
	ON album."AlbumId" = track."AlbumId"
;






DROP EXTENSION IF EXISTS file_fdw CASCADE;
-- Create the file fdw
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
	filename '/var/tmp/Genre.csv',
	format 'csv',
	HEADER 'true'
);





-- Select some data
SELECT * FROM csv_genre;





-- Join SQLite, MariaDB, PostgreSQL, and CSV tables
SELECT artist."Name"
	, album."Title"
	, track."Name"
	, genre."Name"
FROM sqlite_artist AS artist
INNER JOIN mysql_album AS album
	ON artist."ArtistId" = album."ArtistId"
INNER JOIN "Track" AS track
	ON album."AlbumId" = track."AlbumId"
INNER JOIN csv_genre AS genre
	ON track."GenreId" = genre."GenreId"
;






-- Joining SQLite and MariaDB tables using PostgreSQL expressions
WITH album AS
	(
		SELECT "ArtistId"
			, array_agg("Title") AS album_titles
		FROM mysql_album
		GROUP BY "ArtistId"
	)
SELECT artist."Name" AS artist
	, album.album_titles
FROM sqlite_artist AS artist
INNER JOIN album
	ON artist."ArtistId" = album."ArtistId"
;





DROP MATERIALIZED VIEW IF EXISTS mv_album_artist;
-- Creates an materialized view on foreign tables
CREATE MATERIALIZED VIEW mv_album_artist AS
WITH album AS
	(
		SELECT "ArtistId"
			, array_agg("Title") AS album_titles
		FROM mysql_album
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






-- Select the mv data
SELECT *
FROM mv_album_artist
WHERE upper(artist) LIKE 'A%'
ORDER BY artist
;






/* MariaDB control statement
-- SELECT the amount of albums from the MariaDB table from MariaDB, not with a foreign data wrapper
SELECT count( * ) AS AlbumCount
FROM `Album`
;
*/





-- Insert data calculated from foreign tables using PostgreSQL features into another foreign table
INSERT INTO mysql_album("AlbumId", "ArtistId", "Title")
WITH album AS
	(
		-- Generate a new album id
		SELECT MAX(album."AlbumId") + 1 AS new_album_id
		FROM mysql_album AS album
	)
SELECT album.new_album_id
	, artist."ArtistId"
	, 'Back in Black'
FROM sqlite_artist AS artist, album
WHERE artist."Name" = 'AC/DC'
GROUP BY album.new_album_id
	, artist."ArtistId"
;






-- Select data from the materialized view
SELECT *
FROM mv_album_artist
WHERE artist = 'AC/DC'
ORDER BY artist
;






-- Refresh the mv to see the recently added data
REFRESH MATERIALIZED VIEW mv_album_artist;






-- We can even delete data from foreign tables
DELETE FROM mysql_album
WHERE "Title" = 'Back in Black'
	AND "ArtistId" = 1
;






/* MariaDB control statement
-- SELECT the amount of albums from the MariaDB table from MariaDB, not with a foreign data wrapper
SELECT count( * ) AS AlbumCount
FROM `Album`
;*/






-- Using PostgreSQL JSON with data from MariaDB and SQLite
-- Step 1: Albums with tracks as JSON
WITH albums AS
	(
		SELECT a."ArtistId" AS artist_id
			, a."Title" AS album_title
			, array_agg(t."Name") AS album_tracks
		FROM mysql_album AS a
			INNER JOIN "Track" AS t
				ON a."AlbumId" = t."AlbumId"
		GROUP BY a."ArtistId"
			, a."Title"
	)
SELECT row_to_json(albums) AS album_tracks
FROM albums
;





-- Albums including tracks with aritsts with some JSON magic
WITH albums AS
	(
		SELECT a."ArtistId" AS artist_id
			, a."Title" AS album_title
			, array_agg(t."Name") AS album_tracks
		FROM mysql_album AS a
			INNER JOIN "Track" AS t
				ON a."AlbumId" = t."AlbumId"
		GROUP BY a."ArtistId"
			, a."Title"
	)
, js_albums AS
	(
		SELECT row_to_json(albums) AS album_tracks
		FROM albums
	)
SELECT a."Name" AS artist
	, jsonb_pretty(al.album_tracks::jsonb) AS albums_tracks
FROM sqlite_artist AS a
INNER JOIN js_albums AS al
	ON a."ArtistId" = (al.album_tracks->>'artist_id')::int
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





-- Query the RSS feed
SELECT *
FROM rss_postgresql_events
;





-- Entend the query of the RSS feed
SELECT title
	, "pubDate"::DATE AS "Conference Start Date"
	, description
FROM rss_postgresql_events
WHERE "pubDate"::DATE > NOW()::DATE
ORDER BY "pubDate" ASC
;

