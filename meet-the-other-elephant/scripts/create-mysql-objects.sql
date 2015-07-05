CREATE EXTENSION mysql_fdw;

CREATE SERVER mariadb_server
FOREIGN DATA WRAPPER mysql_fdw
OPTIONS (address '127.0.0.1', port '3306');

CREATE USER MAPPING FOR PUBLIC SERVER mariadb_server
OPTIONS (username 'pg_test', password 'secret');

CREATE FOREIGN TABLE fdw_mariadb_users (
    users_id BIGINT,
    username VARCHAR(100),
    firstname VARCHAR(100),
    lastname VARCHAR(100), 
    pwhash VARCHAR(255)
)
SERVER mariadb_server
OPTIONS (
    dbname 'meet_the_other_elephant',
    table_name 'users'
);

CREATE FOREIGN TABLE fdw_mariadb_addresses (
    address_id bigint,
    users_id bigint,
    address_type character varying(100),
    postcode character varying(100),
    city character varying(100),
    street character varying(100),
    country_id integer
)
SERVER mariadb_server
OPTIONS (
    dbname 'meet_the_other_elephant',
    table_name 'addresses'
);

-----------------------------------------------------------

SELECT * FROM fdw_mariadb_users;

-----------------------------------------------------------

SELECT row_to_json(fdw_mariadb_users) AS json_result
FROM fdw_mariadb_users;

-----------------------------------------------------------

SELECT row_to_json(row(users_id, firstname || ' ' || lastname)) AS json_result
FROM fdw_mariadb_users;

-----------------------------------------------------------

SELECT row_to_json(t1) AS json_result
FROM (
    SELECT users_id
        , firstname || ' ' || lastname AS username
    FROM fdw_mariadb_users
) AS t1;

-----------------------------------------------------------

WITH t1 AS (
    SELECT users_id
        , firstname || ' ' || lastname AS username
    FROM fdw_mariadb_users
)
SELECT row_to_json(t1) AS json_result
FROM t1;

-----------------------------------------------------------

WITH t1 AS (
    SELECT users_id
        , firstname || ' ' || lastname AS username
    FROM fdw_mariadb_users
)
SELECT array_to_json(array_agg(row_to_json(t1))) AS json_result
FROM t1;

-----------------------------------------------------------

SELECT u.users_id
    , u.firstname || ' ' || u.lastname AS username
    , bson_get_text(d.bson_data, 'species') AS species_json
    , bson_get_text(d.bson_data, 'name') AS species_json
    , json_data->'species' AS pet_name_json
    , json_data->>'name' AS pet_name_text
FROM fdw_mariadb_users AS u
LEFT OUTER JOIN users_data as d
    ON u.users_id = d.users_id;

-----------------------------------------------------------

WITH t1 AS (
    SELECT u.users_id
        , u.firstname || ' ' || u.lastname AS username
        , bson_get_text(d.bson_data, 'species') AS species_json
        , d.json_data->'name' AS pet_name
    FROM fdw_mariadb_users AS u
    LEFT OUTER JOIN users_data as d
        ON u.users_id = d.users_id
)
SELECT row_to_json(t1) AS json_result
FROM t1;

-----------------------------------------------------------

WITH t1 AS (
    SELECT u.users_id
        , u.firstname || ' ' || u.lastname AS username
        , d.synonyms
        , bson_get_text(d.bson_data, 'species') AS species_json
        , d.json_data->'name' AS pet_name
    FROM fdw_mariadb_users AS u
    LEFT OUTER JOIN users_data as d
        ON u.users_id = d.users_id
)
SELECT array_to_json(array_agg(row_to_json(t1))) AS json_result
FROM t1;

-----------------------------------------------------------

WITH t1 AS (
    SELECT u.users_id
        , u.firstname || ' ' || u.lastname AS username
        , bson_get_text(d.bson_data, 'species') AS species_bson
        , d.json_data->'species' AS species_json
        , d.json_data->'name' AS pet_name
    FROM fdw_mariadb_users AS u
    LEFT OUTER JOIN users_data as d
        ON u.users_id = d.users_id
) ,
t2 AS (
    SELECT count(users_id) AS count_of_users,
        count(species_bson) AS count_of_bson_species,
        count(species_json) AS count_of_json_pecies
    FROM t1
)
SELECT t1.*, t2.*
FROM t1, t2;

-----------------------------------------------------------

SELECT * 
FROM jsonb_each_text(
    (
        SELECT json_data 
        FROM users_data 
        WHERE users_id = 1
    )
);

-----------------------------------------------------------

SELECT *
FROM jsonb_to_recordset(
    (
        SELECT json_data
        FROM users_data 
        WHERE users_id = 3
    )
) AS x(id int, "name" text, species text);

-----------------------------------------------------------


