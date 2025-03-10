-- Drop all objects
DROP EXTENSION IF EXISTS sqlite_fdw CASCADE;
DROP SCHEMA IF EXISTS chinook_sqlite CASCADE;
DROP EXTENSION IF EXISTS mysql_fdw CASCADE;
DROP SCHEMA IF EXISTS chinook_mariadb CASCADE;
DROP EXTENSION IF EXISTS postgres_fdw CASCADE;
DROP SCHEMA IF EXISTS chinook_postgresql_9_6 CASCADE; 
DROP EXTENSION IF EXISTS file_fdw CASCADE;
DROP SCHEMA IF EXISTS chinook_csv CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_album_artist;
DROP EXTENSION IF EXISTS multicorn CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_rss_music_newslists CASCADE;
DROP PROCEDURE IF EXISTS refresh_every_minute();
DROP TABLE IF EXISTS cron.log;
DROP EXTENSION IF EXISTS pg_cron;
