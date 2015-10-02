-- Create the foreign data wrapper extension in the current database
CREATE EXTENSION mysql_fdw;



-- Create the mapping to the foreign MariaDB server
CREATE SERVER mariadb_server
	FOREIGN DATA WRAPPER mysql_fdw
	OPTIONS (host '127.0.0.1', port '3306');



-- Create a user mapping with user and password of the foreign table
-- PostgreSQL gives you options to connect this user with its own users
CREATE USER MAPPING FOR PUBLIC SERVER mariadb_server
OPTIONS (username 'pg_test', password 'secret');



DROP FOREIGN TABLE mysql_album;
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



-- Create the SQLite foreign data wrapper extension in the current database
CREATE EXTENSION sqlite_fdw;



-- Dropping the SQLite server connections, just to show, that it does work
DROP SERVER sqlite_server CASCADE;



-- Create the mapping to the foreign SQLite file
CREATE SERVER sqlite_server
	FOREIGN DATA WRAPPER sqlite_fdw
	OPTIONS (database '/var/sqlite/Chinook_Sqlite.sqlite');



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




-- Join PostgreSQL, MariaDB and SQLite tables
SELECT *
FROM sqlite_artist AS artist
INNER JOIN mysql_album AS album
	ON artist."ArtistId" = album."ArtistId"
INNER JOIN "Track" AS track
	ON album."AlbumId" = track."AlbumId"
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



DROP MATERIALIZED VIEW mv_album_artist;
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



-- Creates a unique index on a mv
CREATE UNIQUE INDEX mv_album_artist__artist ON mv_album_artist(artist);



-- Select the mv data
SELECT *
FROM mv_album_artist
WHERE artist = 'AC/DC'
;



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
-- WHERE artist = 'AC/DC'
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
SELECT count( * ) AS AlbumCount
FROM `Album`
;
*/

