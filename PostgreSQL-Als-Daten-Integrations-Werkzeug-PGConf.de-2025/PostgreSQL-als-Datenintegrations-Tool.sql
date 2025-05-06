/**
 * Das Skript ist Teil des Vortrags PostgreSQL Als Daten Integrationswerkzeug
 * Gegeben auf der German PostgreSQL Conference 2025
 * 
 * Die folgenden Erweiterungen werden benötigt:
 * - sqlite_fdw:   https://github.com/pgspider/sqlite_fdw/
 * - postgres_fdw: https://www.postgresql.org/docs/current/postgres-fdw.html
 * - file_fdw:     https://www.postgresql.org/docs/current/file-fdw.html
 * - multicorn2:   https://github.com/pgsql-io/multicorn2
 * - pg_cron:      https://github.com/citusdata/pg_cron
 *
 * Author:  Stefanie Janine Stölting, mail@stefanie-stoelting.de
 * License: Creative Commons Attribution 4.0 International
 *          http://creativecommons.org/licenses/by/4.0/
 */

DROP EXTENSION IF EXISTS sqlite_fdw CASCADE;
-- Den SQLite foreign data wrapper als extension in der aktuellen
-- Datenbank anlegen
CREATE EXTENSION sqlite_fdw;






-- Ein Mapping auf die SQLite Datei als Server anlegen
-- Wichtig:
-- Der Benutzer postgres muss Lese- und Schreibrechte
-- nicht nur auf die Datei, sondern auch auf den Ordner haben
CREATE SERVER sqlite_server
	FOREIGN DATA WRAPPER sqlite_fdw
	OPTIONS (database '/var/sqlite/Chinook_Sqlite.sqlite')
;





-- Ein Schema für SQLite Tabellen anlegen
CREATE SCHEMA IF NOT EXISTS sqlite;





-- Eine Foreign Table mit Verweis auf die SQLite Datenbank anlegen,
-- die Spaltendefinitionen müssen passen
CREATE FOREIGN TABLE sqlite.artist(
	"ArtistId" integer,
	"Name" text
)
SERVER sqlite_server
OPTIONS(
	table 'Artist'
);






--  Daten abfragen
SELECT * FROM sqlite.artist;





-- Join SQLite mit PostgreSQL 17
SELECT artist."Name" AS album_name
	, album.title
FROM sqlite.artist AS artist
INNER JOIN public.album
	ON artist."ArtistId" = album.artist_id 
;





DROP EXTENSION IF EXISTS postgres_fdw CASCADE;
-- Den PostgreSQL foreign data wrapper anlegen
CREATE EXTENSION postgres_fdw;





-- Eine Verbindung zu einer anderen PostgreSQL Instanz vevinden,
-- hier zu einer PostgreSQL 14
CREATE SERVER pg_localhost_chinook
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (host '127.0.0.1', port '5432', dbname 'chinook')
;





-- Ein Benutzer Mapping anlegen
CREATE USER MAPPING FOR stefanie
        SERVER pg_localhost_chinook
        OPTIONS (user 'stefanie', password 'password')
;





-- Ein Schema für foreign PostgreSQL Tabellen anlegen
CREATE SCHEMA IF NOT EXISTS pg14;




 
-- Link zu foreign tables in der aktuellen Datenbank anlegen
IMPORT FOREIGN SCHEMA public LIMIT TO("Track")
FROM SERVER pg_localhost_chinook
INTO pg14
;





-- Versuch, Daten anzuzeigen
SELECT * FROM pg14."Track";





-- Join SQLite und PostgreSQL Tabellen
SELECT artist."Name"
	, album.title
	, track."Name"
FROM sqlite.artist AS artist
INNER JOIN public.album
	ON artist."ArtistId" = album.artist_id
INNER JOIN pg14."Track" AS track
	ON album.album_id = track."AlbumId"
;






-- Daten in externer Tabelle aktualisieren
UPDATE pg14."Track" SET
	"Name" = 'Der Track Name ist geändert!'
WHERE "TrackId" = 6;







-- Daten anzeigen
SELECT *
FROM pg14."Track" SET
WHERE "TrackId" = 6;







-- Daten in externer Tabelle aktualisieren
UPDATE pg14."Track" SET
	"Name" = 'Put The Finger On You'
WHERE "TrackId" = 6;







-- Daten anzeigen
SELECT *
FROM pg14."Track" SET
WHERE "TrackId" = 6;







DROP EXTENSION IF EXISTS file_fdw CASCADE;
-- Die file extension anlegen
CREATE EXTENSION file_fdw;





-- Es braucht ein server Objekt, danach kann man alle CSV Dateien einbinden
-- (Wenn der user postgres Rechte auf den Pfad und die Datei hat)
CREATE SERVER chinook_csv
	FOREIGN DATA WRAPPER file_fdw
;





-- Ein Schema für CSV files anlegen
CREATE SCHEMA IF NOT EXISTS CSV;




-- Anlegen einer foreign Tabelle basierend auf einer CSV Datei
-- Die Optionen sind die gleichen wie im COPY Befehl
CREATE FOREIGN TABLE csv.genre (
	"GenreId" integer,
	"Name" text
) SERVER chinook_csv
OPTIONS (
	filename '/var/sqlite/Genre.csv',
	format 'csv',
	HEADER 'true'
);




-- Daten anzeigen
SELECT * FROM csv.genre;





-- Join SQLite, two PostgreSQL servers, and a CSV tables
SELECT artist."Name"
	, album.title
	, track."Name"
	, genre."Name"
FROM sqlite.artist AS artist
INNER JOIN public.album
	ON artist."ArtistId" = album.artist_id 
INNER JOIN pg14."Track" AS track
	ON album.album_id = track."AlbumId"
INNER JOIN csv.genre AS genre
	ON track."GenreId" = genre."GenreId"
;







DROP MATERIALIZED VIEW IF EXISTS mv_album_artist;
-- Creates an materialized view on foreign tables
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






-- Daten anzeigen
SELECT *
FROM mv_album_artist
WHERE upper(artist) LIKE 'A%'
ORDER BY artist
;







-- Multicorn Examples
DROP EXTENSION IF EXISTS multicorn CASCADE;
-- Die multicorn extension anlegen
CREATE EXTENSION multicorn;





-- Einen Server anlegen, der ist aber nur ein Platzhalter
CREATE SERVER rss_srv foreign data wrapper multicorn options (
    wrapper 'multicorn.rssfdw.RssFdw'
)
;





-- Ein Schema für multicorn Tabellen
CREATE SCHEMA IF NOT EXISTS multicorn;





-- Eine foreign Tabelle auf Basis eines RSS feeds anlegen
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





-- Daten aus dem RSS Feed direkt aus dem Web abfragen
SELECT *
FROM multicorn.rss_mi2nbandnews;





-- Die Daten aus dem RSS Feed mit einer Tabelle verbinden
SELECT a."Name" AS artist
	, r.*
FROM multicorn.rss_mi2nbandnews AS r
INNER JOIN sqlite.artist AS a
	ON r.description ilike '%' || a."Name" || '%'
;





DROP MATERIALIZED VIEW IF EXISTS multicorn.rss_mi2nbandnews_mv;
-- Einen Materialized View anlegen
CREATE MATERIALIZED VIEW multicorn.rss_mi2nbandnews_mv AS
SELECT ROW_NUMBER() OVER() AS rn
	, r.title
	, r.link
	, r.description
	, r."pubDate" AS data_publication
	, r.guid
FROM multicorn.rss_mi2nbandnews AS r;




-- Einen Unique Index anlegen
CREATE UNIQUE INDEX rss_mi2nbandnews_mv_udx
	ON multicorn.rss_mi2nbandnews_mv
	USING btree
		(
			rn
		);




-- Daten aus dem Materialized View anzeigen
SELECT *
FROM multicorn.rss_mi2nbandnews_mv
;





-- Die Daten aus dem RSS Feed mit einer Tabelle verbinden
SELECT a."Name" AS artist
	, r.*
FROM multicorn.rss_mi2nbandnews_mv AS r
INNER JOIN sqlite.artist AS a
	ON r.description ilike '%' || a."Name" || '%'
;





-- Die cron Extension anlegen um zu aktualisierungen zu Terminieren
DROP EXTENSION IF EXISTS pg_cron;
CREATE EXTENSION pg_cron;




DROP TABLE IF EXISTS cron.log;
-- Die Tabelle wird benutzt, um cron Aufrufe zu loggen
CREATE TABLE cron.log (
	log_id bigserial NOT NULL,
	time_stamp_begin timestamp WITH time ZONE NOT NULL,
	time_stamp_end timestamp WITH time ZONE NOT NULL,
	executed text NOT NULL,
	CONSTRAINT log_pk PRIMARY KEY (log_id) 
)
;






DROP PROCEDURE IF EXISTS refresh_every_minute();
-- Die Prozedur wird den Materialized View aktualisieren und das loggen
CREATE OR REPLACE PROCEDURE refresh_every_minute() AS $$
DECLARE
	ts_start timestamp WITH time ZONE DEFAULT current_timestamp;
BEGIN
	-- Den Materialized View aktualisieren
	REFRESH MATERIALIZED VIEW CONCURRENTLY multicorn.rss_mi2nbandnews_mv;

	-- Einen Log Eintrag speichern
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
-- Einen Cron Job in PostgreSQL anlegen um den Materialized View
-- jede Minute automatisch zu aktulisieren
INSERT INTO cron.job (schedule, command, nodename, nodeport, database, username)
VALUES ('* * * * *', 'CALL refresh_every_minute()', '', 5435, 'chinook', 'postgres')
;






-- Das Cron Job anzeigen
SELECT *
FROM cron.job;




--
SELECT *
FROM cron.log;




-- Mit der Job ID wird der Job gestoppt
SELECT cron.unschedule(1);




-- Noch einen RSS Feed anlegen
CREATE FOREIGN TABLE multicorn.rss_postgresql_events (
	title text,
	link text,
	description text,
	"pubDate" TIMESTAMPTZ,
	guid text
) server rss_srv OPTIONS (
	url 'https://www.postgresql.org/events.rss'
)
;





-- Die kommenden PostgreSQL Events anzeigen
SELECT title
	, "pubDate"::DATE AS "Conference Start Date"
	, description
FROM multicorn.rss_postgresql_events
WHERE "pubDate"::DATE > NOW()::DATE
ORDER BY "pubDate" ASC
;





