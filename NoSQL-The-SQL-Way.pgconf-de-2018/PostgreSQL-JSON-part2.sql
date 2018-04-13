-- DROP TABLE IF EXISTS reviews CASCADE;
﻿-- Create a table for JSON data with 1998 Amazon reviews

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








-- Create a GIN index
DROP INDEX IF EXISTS review_review_jsonb;
CREATE INDEX review_review_jsonb ON reviews USING GIN (review_jsonb);








-- SELECT some statistics from the JSON data
SELECT review_jsonb#>>'{product,category}' AS category
	, avg((review_jsonb#>>'{review,rating}')::int) AS average_rating
	, count((review_jsonb#>>'{review,rating}')::int) AS count_rating
FROM reviews
GROUP BY category
;








-- Create a B-Tree index on a JSON expression
DROP INDEX IF EXISTS reviews_product_category;
CREATE INDEX reviews_product_category ON reviews ((review_jsonb#>>'{product,category}'));


