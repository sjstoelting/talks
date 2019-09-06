/**
 * Author: mail@stefanie-stoelting.de
 * Licence: PostgreSQL Licence
 * 
 * Talk at PGDay Austria 2019/09/06
 * PostgreSQL JSON Features - Heute und in der Zukunft
 */

-- Create a table for JSON data with 1998 Amazon reviews
DROP TABLE IF EXISTS reviews CASCADE;

CREATE TABLE reviews(review_jsonb jsonb);








-- Import customer reviews from a file
COPY reviews
FROM '/var/tmp/customer_reviews_nested_1998.json'
;







-- There should be 589.859 records imported into the table
SELECT count(*)
FROM reviews
;








-- Lets have a look at the JSON structure
SELECT jsonb_pretty(review_jsonb)
	,  review_jsonb
FROM reviews
LIMIT 1
;








-- Select data with JSON
SELECT review_jsonb#>> '{product,title}' AS title
    , avg((review_jsonb#>> '{review,rating}')::int) AS average_rating
FROM reviews
WHERE review_jsonb@>'{"product": {"category": "Sheet Music & Scores"}}'
GROUP BY title
ORDER BY average_rating DESC
;

EXPLAIN ANALYSE SELECT review_jsonb#>> '{product,title}' AS title
    , avg((review_jsonb#>> '{review,rating}')::int) AS average_rating
FROM reviews
WHERE review_jsonb@>'{"product": {"category": "Sheet Music & Scores"}}'
GROUP BY title
ORDER BY average_rating DESC
;


-- Create a GIN index
DROP INDEX IF EXISTS review_review_jsonb;
CREATE INDEX review_review_jsonb ON reviews USING GIN (review_jsonb);


/*** History with several versions on the same hardware ***/
--PostgreSQL 9.6
--Planning time: 0.261 ms
--Execution time: 613.261 ms
--WITH GIN
--Planning time: 0.280 ms
--Execution time: 2.013 ms

--PostgreSQL 10
--Planning time: 0.515 ms
--Execution time: 323.349 ms
--WITH GIN
--Planning time: 0.365 ms
--Execution time: 1.227 ms


--PostgreSQL 11
--Planning Time: 0.329 ms
--Execution Time: 318.587 ms
--WITH GIN
--Planning Time: 0.335 ms
--Execution Time: 1.868 ms

--PostgreSQL 12
--Planning Time: 0.302 ms
--Execution Time: 266.029 ms
--WITH GIN
--Planning Time: 0.223 ms
--Execution Time: 1.253 ms









-- SELECT some statistics from the JSON data
SELECT review_jsonb#>>'{product,category}' AS category
	, avg((review_jsonb#>>'{review,rating}')::int) AS average_rating
	, count((review_jsonb#>>'{review,rating}')::int) AS count_rating
FROM reviews
GROUP BY category
;








DROP INDEX IF EXISTS reviews_product_category;
-- Create a B-Tree index on a JSON expression
CREATE INDEX reviews_product_category ON reviews ((review_jsonb#>>'{product,category}'));








-- Using jsonb path queries: Filter by existing path
SELECT count(*) AS count_of_books
FROM reviews
WHERE jsonb_path_exists(review_jsonb, '$.product.category')
;






-- Using jsonb path queries: 
SELECT jsonb_path_query(review_jsonb,'$.*') AS jsonb_path_result
FROM reviews
;






-- Using jsonb path queries: Getting filtered results from field values
SELECT jsonb_path_query(review_jsonb, '$[*] ? (@.product.category == "Computers & Internet")') AS jsonb_col
FROM reviews
;






-- Using jsonb path queries: Getting filtered results from field values
SELECT jsonb_path_query(review_jsonb, '$[*] ? (@.product.category == "Computers & Internet")') AS jsonb_col
FROM reviews;

WITH t1 AS
	(
		SELECT jsonb_path_query(review_jsonb, '$[*] ? (@.product.category == "Computers & Internet")') AS jsonb_col
		FROM reviews
	)
SELECT jsonb_col#>> '{product,category}' AS category
	, jsonb_col#>> '{product,subcategory}' AS subcategory
	, jsonb_col#>> '{product,title}' AS title
FROM t1
;






-- Using jsonb path queries: Get an array from the jsonb,
-- same as jsonb_path_query, but returns arrays
SELECT jsonb_path_query_array(review_jsonb, '$.*') AS jsonb_path_result
FROM reviews
;

SELECT jsonb_path_query_array(review_jsonb, '$.product.*') AS jsonb_path_result
FROM reviews
;

SELECT jsonb_path_query_array(review_jsonb, '$.review.*') AS jsonb_path_result
FROM reviews
;






-- Using jsonb path queries: Get the data from the first id
-- of the JSONB object
SELECT jsonb_path_query_first(review_jsonb, '$.*'#ä) AS jsonb_path_result
FROM reviews
;



