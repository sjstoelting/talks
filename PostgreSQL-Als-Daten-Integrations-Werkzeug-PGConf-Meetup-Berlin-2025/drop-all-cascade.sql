/**
 * Das Skript ist Teil des Vortrags PostgreSQL Als Daten Integrationswerkzeug
 * Gegeben auf der German PostgreSQL Conference 2025
 * 
 * Author:  Stefanie Janine Stölting, mail@stefanie-stoelting.de
 * License: Creative Commons Attribution 4.0 International
 *          http://creativecommons.org/licenses/by/4.0/
 */

-- Drop all objects
DROP EXTENSION IF EXISTS sqlite_fdw CASCADE;
DROP SCHEMA IF EXISTS sqlite CASCADE;
DROP EXTENSION IF EXISTS postgres_fdw CASCADE;
DROP SCHEMA IF EXISTS pg14 CASCADE; 
DROP EXTENSION IF EXISTS file_fdw CASCADE;
DROP SCHEMA IF EXISTS csv CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_album_artist;
DROP EXTENSION IF EXISTS multicorn CASCADE;
DROP SCHEMA IF EXISTS multicorn CASCADE;
DROP PROCEDURE IF EXISTS refresh_every_minute();
DROP TABLE IF EXISTS cron.log;
DROP EXTENSION IF EXISTS pg_cron;
DROP MATERIALIZED VIEW IF EXISTS multicorn.rss_mi2nbandnews_mv;
