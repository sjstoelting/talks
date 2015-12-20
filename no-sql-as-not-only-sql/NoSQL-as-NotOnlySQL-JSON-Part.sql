-- DROP TABLE reviews CASCADE;
﻿-- Create a table for JSON data with 1998 Amazon reviews

CREATE TABLE reviews(review_jsonb jsonb);






-- Import customer reviews from a file
COPY reviews FROM '/var/tmp/customer_reviews_nested_1998.json'






-- Maintenance the filled table
VACUUM ANALYZE reviews;






-- There should be 589.859 records imported into the table
SELECT count(*) FROM reviews;






-- Select data with JSON
SELECT
    review_jsonb#>> '{product,title}' AS title
    , avg((review_jsonb#>> '{review,rating}')::int) AS average_rating
FROM reviews
WHERE review_jsonb@>'{"product": {"category": "Sheet Music & Scores"}}'
GROUP BY 1 
ORDER BY 2 DESC
;






-- Create a GIN index
CREATE INDEX review_review_jsonb ON reviews USING GIN (review_jsonb);







-- SELECT some statistics from the JSON data
SELECT  review_jsonb#>>'{product,category}' AS category
	, avg((review_jsonb#>>'{review,rating}')::int) AS average_rating
	, count((review_jsonb#>>'{review,rating}')::int) AS count_rating
FROM reviews
GROUP BY 1
;






-- Create a B-Tree index on a JSON expression
CREATE INDEX reviews_product_category ON reviews ((review_jsonb#>>'{product,category}'));
