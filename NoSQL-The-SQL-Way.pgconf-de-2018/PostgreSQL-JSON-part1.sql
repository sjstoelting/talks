-- Lets start the easy way: With a function call to get album data
SELECT json_build_object (
		'album_id', "AlbumId",
		'track_id, ', "TrackId",
		'track_name', "Name"
	)
FROM "Track"
;







-- Adding array_agg to build JSON arrays
WITH tracks AS
	(
		SELECT "AlbumId" AS album_id
			, json_build_object (
				'track_id, ', "TrackId",
				'track_name', "Name"
			)
		FROM "Track"
	)
SELECT album_id
	, json_build_object ('tracks', array_agg (tracks)) AS tracks
FROM tracks
GROUP BY album_id
;








-- Step 1: Tracks as JSON with the album identifier using row_to_json
WITH tracks AS
	(
		SELECT "AlbumId" AS album_id
			, "TrackId" AS track_id
			, "Name" AS track_name
		FROM "Track"
	)
SELECT row_to_json (tracks) AS tracks
FROM tracks
;









-- Step 2 Abums including tracks with aritst identifier, :: as CAST operatorS
WITH tracks AS
	(
		SELECT "AlbumId" AS album_id
			, "TrackId" AS track_id
			, "Name" AS track_name
		FROM "Track"
	)
, json_tracks AS
	(
		SELECT row_to_json(tracks) AS tracks
		FROM tracks
	)
, albums AS
	(
		SELECT a."ArtistId" AS artist_id
			, a."AlbumId" AS album_id
			, a."Title" AS album_title
			, array_agg(t) AS album_tracks
		FROM "Album" AS a
			INNER JOIN json_tracks AS t
				ON a."AlbumId" = (t.tracks->>'album_id')::int
		GROUP BY a."ArtistId"
			, a."AlbumId"
			, a."Title"
	)
SELECT artist_id
	, array_agg(row_to_json(albums)) AS album
FROM albums
GROUP BY artist_id
;







-- DROP VIEW IF EXISTS v_json_artist_data CASCADE;
-- Step 3 Return one row for an artist with all albums as VIEW
CREATE OR REPLACE VIEW v_json_artist_data AS
WITH tracks AS
	(
		SELECT "AlbumId" AS album_id
			, "TrackId" AS track_id
			, "Name" AS track_name
			, "MediaTypeId" AS media_type_id
			, "Milliseconds" As milliseconds
			, "UnitPrice" AS unit_price
		FROM "Track"
	)
, json_tracks AS
	(
		SELECT row_to_json(tracks) AS tracks
		FROM tracks
	)
, albums AS
	(
		SELECT a."ArtistId" AS artist_id
			, a."AlbumId" AS album_id
			, a."Title" AS album_title
			, array_agg(t.tracks) AS album_tracks
		FROM "Album" AS a
			INNER JOIN json_tracks AS t
				ON a."AlbumId" = (t.tracks->>'album_id')::int
		GROUP BY a."ArtistId"
			, a."AlbumId"
			, a."Title"
	)
, json_albums AS
	(
		SELECT artist_id
			, array_agg(row_to_json(albums)) AS album
		FROM albums
		GROUP BY artist_id
	)
, artists AS
	(
		SELECT a."ArtistId" AS artist_id
			, a."Name" AS artist
			, jsa.album AS albums
		FROM "Artist" AS a
			INNER JOIN json_albums AS jsa
				ON a."ArtistId" = jsa.artist_id
	)
SELECT (row_to_json(artists))::jsonb AS artist_data
FROM artists
;








-- Select data from the view
SELECT *
FROM v_json_artist_data
;








-- SELECT data from that VIEW with jsonb_pretty
SELECT jsonb_pretty(artist_data)
FROM v_json_artist_data
WHERE artist_data->>'artist' IN ('Miles Davis', 'AC/DC')
;








-- SELECT some data from that VIEW using JSON methods:
-- #> JSON object at path
-- @> does JSON contain at top level
SELECT 	artist_data->>'artist' AS artist
	, artist_data#>'{albums, 1, album_title}' AS album_title
	, jsonb_pretty(artist_data#>'{albums, 1, album_tracks}') AS album_tracks
FROM v_json_artist_data
WHERE artist_data->'albums' @> '[{"album_title":"Miles Ahead"}]'
;








-- Array to records: jsonb_array_elements expands JSON array to a set of values
SELECT artist_data->>'artist_id' AS artist_id
	, artist_data->>'artist' AS artist
	, jsonb_array_elements(artist_data#>'{albums}')->>'album_title' AS album_title
	, jsonb_array_elements(jsonb_array_elements(artist_data#>'{albums}')#>'{album_tracks}')->>'track_name' AS song_titles
	, jsonb_array_elements(jsonb_array_elements(artist_data#>'{albums}')#>'{album_tracks}')->>'track_id' AS song_id
FROM v_json_artist_data
WHERE artist_data->>'artist' = 'Metallica'
ORDER BY album_title
	, song_id
;








-- Convert albums to a recordset
SELECT *
FROM jsonb_to_recordset(
	(
		SELECT (artist_data->>'albums')::jsonb
		FROM v_json_artist_data
		WHERE (artist_data->>'artist_id')::int = 50
	)
) AS x(album_id int, artist_id int, album_title text, album_tracks jsonb)
;








-- Convert the tracks to a recordset with jsonb_to_recordset
SELECT album_id
	, track_id
	, track_name
	, media_type_id
	, milliseconds
	, unit_price
FROM jsonb_to_recordset(
	(
		-- JSON data fot the field output
		SELECT artist_data#>'{albums, 1, album_tracks}'
		FROM v_json_artist_data
		WHERE (artist_data->>'artist_id')::int = 50
	)
-- Field list with data types
) AS x(album_id int, track_id int, track_name text, media_type_id int, milliseconds int, unit_price float)
;








-- DROP FUNCTION IF EXISTS trigger_v_json_artist_data_update() CASCADE;
-- Create a function, which will be used for UPDATE on the view v_artrist_data
CREATE OR REPLACE FUNCTION trigger_v_json_artist_data_update()
	RETURNS trigger AS
$BODY$
	-- Data variables
	DECLARE rec			RECORD;
BEGIN
	-- Update table Artist
	IF (OLD.artist_data->>'artist')::varchar(120) <> (NEW.artist_data->>'artist')::varchar(120) THEN
		UPDATE "Artist"
		SET "Name" = (NEW.artist_data->>'artist')::varchar(120)
		WHERE "ArtistId" = (OLD.artist_data->>'artist_id')::int;
	END IF;

	-- Update table Album with an UPSERT

	-- Update table Track with an UPSERT

	RETURN NEW;
END;
$BODY$
	LANGUAGE plpgsql;




	



-- The trigger will be fired instead of an UPDATE statement to save data
CREATE TRIGGER v_json_artist_data_instead_update INSTEAD OF UPDATE
	ON v_json_artist_data
	FOR EACH ROW
	EXECUTE PROCEDURE trigger_v_json_artist_data_update()
;







-- Manipulate data with jsonb_set
SELECT artist_data->>'artist_id' AS artist_id
	, artist_data->>'artist' AS artist
	, jsonb_set(artist_data, '{artist}', '"Whatever we want, it is just text"'::jsonb)->>'artist' AS new_artist
FROM v_json_artist_data
WHERE (artist_data->>'artist_id')::int = 50
;








-- Update a JSONB column with a jsonb_set result
UPDATE v_json_artist_data
SET artist_data= jsonb_set(artist_data, '{artist}', '"NEW Metallica"'::jsonb)
WHERE (artist_data->>'artist_id')::int = 50
;








-- View the changes done by the UPDATE statement
SELECT artist_data->>'artist_id' AS artist_id
	, artist_data->>'artist' AS artist
FROM v_json_artist_data
WHERE (artist_data->>'artist_id')::int = 50
;








-- View the changes in in the table instead of the JSONB view
-- The result should be the same, only the column name differ
SELECT *
FROM "Artist"
WHERE "ArtistId" = 50
;








-- Manipulate data with the concatenating / overwrite operator
SELECT artist_data->>'artist_id' AS artist_id
	, artist_data->>'artist' AS artist
	, jsonb_set(artist_data, '{artist}', '"Whatever we want, it is just text"'::jsonb)->>'artist' AS new_artist
	, artist_data || '{"artist":"Metallica"}'::jsonb->>'artist' AS correct_name
FROM v_json_artist_data
WHERE (artist_data->>'artist_id')::int = 50
;








-- Revert the name change of Metallica with in a different way: With the replace operator
UPDATE v_json_artist_data
SET artist_data = artist_data || '{"artist":"Metallica"}'::jsonb
WHERE (artist_data->>'artist_id')::int = 50
;








-- View the changes done by the UPDATE statement
SELECT artist_data->>'artist_id' AS artist_id
	, artist_data->>'artist' AS artist
FROM v_json_artist_data
WHERE (artist_data->>'artist_id')::int = 50
;








-- Remove some data with the - operator
SELECT jsonb_pretty(artist_data) AS complete
	, jsonb_pretty(artist_data - 'albums') AS minus_albums
	, jsonb_pretty(artist_data) = jsonb_pretty(artist_data - 'albums') AS is_different
FROM v_json_artist_data
WHERE artist_data->>'artist' IN ('Miles Davis', 'AC/DC')
;
