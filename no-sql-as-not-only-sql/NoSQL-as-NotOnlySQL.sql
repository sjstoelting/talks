-- Step 1: Albums with tracks as JSON
WITH albums AS
	(
		SELECT a."ArtistId" AS artist_id
			, a."Title" AS album_title
			, array_agg(t."Name") AS album_tracks
		FROM "Album" AS a
			INNER JOIN "Track" AS t
				ON a."AlbumId" = t."AlbumId"
		GROUP BY a."ArtistId"
			, a."Title"
	)
SELECT row_to_json(albums) AS album_tracks
FROM albums
;






-- Step 2 Abums including tracks with aritsts
WITH albums AS
	(
		SELECT a."ArtistId" AS artist_id
			, a."Title" AS album_title
			, array_agg(t."Name") AS album_tracks
		FROM "Album" AS a
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
	, al.album_tracks AS albums_tracks
FROM sqlite_artist AS a
	INNER JOIN js_albums AS al
		ON a."ArtistId" = CAST(al.album_tracks->>'artist_id' AS INT)
;






-- DROP VIEW v_artist_data;
-- Step 3 Return one row for an artist with all albums as VIEW
CREATE OR REPLACE VIEW v_artist_data AS
WITH albums AS
	(
		SELECT a."ArtistId" AS artist_id
			, a."Title" AS album_title
			, array_agg(t."Name") AS album_tracks
		FROM "Album" AS a
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
, artist_albums AS
	(
		SELECT a."Name" AS artist
			, array_agg(al.album_tracks) AS albums_tracks
		FROM "Artist" AS a
			INNER JOIN js_albums AS al
				ON a."ArtistId" = CAST(al.album_tracks->>'artist_id' AS INT)
		GROUP BY a."Name"
	)
SELECT CAST(row_to_json(artist_albums) AS JSONB) AS artist_data
FROM artist_albums
;






-- Select data from the view
SELECT *
FROM v_artist_data
;






-- SELECT data from that VIEW, that does querying 
SELECT jsonb_pretty(artist_data) pretty_artistdata
FROM v_artist_data
WHERE artist_data->>'artist' IN ('Miles Davis', 'AC/DC')
;






-- SELECT some data that VIEW using JSON methods
SELECT jsonb_pretty(artist_data#>'{albums_tracks}') AS all_albums
	, jsonb_pretty(artist_data#>'{albums_tracks, 0}') AS tracks_0
	, artist_data#>'{albums_tracks, 0, album_title}' AS title
	, artist_data#>'{albums_tracks, 0, artist_id}' AS artist_id
	, artist_data->>'artist' AS artist
FROM v_artist_data
WHERE artist_data->'albums_tracks' @> '[{"album_title":"Miles Ahead"}]'
;






-- Array to records
SELECT jsonb_array_elements(artist_data#>'{albums_tracks}')->>'artist_id' AS artist_id
	, artist_data->>'artist' AS artist  
	, jsonb_array_elements(artist_data#>'{albums_tracks}')->>'album_title' AS ablum_title
	, jsonb_array_elements(jsonb_array_elements(artist_data#>'{albums_tracks}')#>'{album_tracks}') AS song_titles
FROM v_artist_data
WHERE artist_data->'albums_tracks' @> '[{"artist_id":139}]'
ORDER BY 3, 4;








-- DROP FUNCTION trigger_v_artist_data_insert() CASCADE;
-- Create a function, which will be used for INSERT on the view v_artrist_data
CREATE OR REPLACE FUNCTION trigger_v_artist_data_insert()
	RETURNS trigger AS
$BODY$
	-- Data variables
	DECLARE rec			RECORD;
	-- Error variables
	DECLARE v_state		TEXT;
	DECLARE v_msg		TEXT;
	DECLARE v_detail	TEXT;
	DECLARE v_hint		TEXT;
	DECLARE v_context	TEXT;
BEGIN
	-- Update table Artist
	IF OLD.artist_data->>'artist' <> NEW.artist_data->>'artist' THEN
		UPDATE "Artist"
		SET "Name" = NEW.artist_data->>'artist'
		WHERE "ArtistId" = artist_data#>'{albums_tracks, 0, artist_id}';
	END IF;

	-- Update table Album in a foreach

	-- Update table Track in a foreach

	RETURN NEW;

	EXCEPTION WHEN unique_violation THEN
		RAISE NOTICE 'Sorry, but the user "%" does already exist, please use a different name', _Username;
		RETURN 0;

	WHEN others THEN
		GET STACKED DIAGNOSTICS
			v_state = RETURNED_SQLSTATE,
			v_msg = MESSAGE_TEXT,
			v_detail = PG_EXCEPTION_DETAIL,
			v_hint = PG_EXCEPTION_HINT,
			v_context = PG_EXCEPTION_CONTEXT;

		RAISE NOTICE '%', v_msg;
		RETURN OLD;
END;
$BODY$
	LANGUAGE plpgsql;






-- The trigger will be fired instead of an INSERT statemen to save data
CREATE TRIGGER v_artist_data_instead INSTEAD OF INSERT
	ON v_artist_data
	FOR EACH ROW
	EXECUTE PROCEDURE trigger_v_artist_data_insert()
;
