-- Create a base dataset and join all relevant tables
DROP TABLE IF EXISTS complete_joint_dataset;
CREATE TEMP TABLE complete_joint_dataset AS
SELECT
  rental.customer_id,
  inventory.film_id,
  film.title,
  category.name AS category_name,
  rental.rental_date
FROM dvd_rentals.rental
INNER JOIN dvd_rentals.inventory
  ON rental.inventory_id = inventory.inventory_id
INNER JOIN dvd_rentals.film
  ON inventory.film_id = film.film_id
INNER JOIN dvd_rentals.film_category
  ON film.film_id = film_category.film_id
INNER JOIN dvd_rentals.category
  ON film_category.category_id = category.category_id;
  

-- Calculate customer rental counts for each category
DROP TABLE IF EXISTS category_counts;
CREATE TEMP TABLE category_counts AS 
SELECT
  customer_id,
  category_name,
  COUNT(*) rental_count,
  MAX(rental_date) AS latest_rental_date
FROM complete_joint_dataset
GROUP BY 
  customer_id,
  category_name;


-- Aggregate all customer total films watched
DROP TABLE IF EXISTS total_counts;
CREATE TEMP TABLE total_counts AS
SELECT 
  customer_id,
  SUM(rental_count) AS total_film_count
FROM category_counts
GROUP BY 
  customer_id;


-- Identify the top 2 categories for each customer
DROP TABLE IF EXISTS top_categories;
CREATE TEMP TABLE top_categories AS 
WITH cte_rank AS (
  SELECT 
    customer_id,
    category_name,
    rental_count,
    DENSE_RANK() OVER (
      PARTITION BY customer_id
      ORDER BY 
        rental_count desc,
        latest_rental_date desc,
        category_name
    ) AS category_rank
  FROM category_counts
  )
SELECT * FROM cte_rank
WHERE category_rank <= 2;

-- Calculate each category’s aggregated average rental count
DROP TABLE IF EXISTS average_category_count;
CREATE TEMP TABLE average_category_count AS 
SELECT 
  category_name,
  FLOOR(AVG(rental_count)) AS category_average
FROM category_counts
GROUP BY category_name;

-- Calculate the percentile metric for each customer’s top category film count
DROP TABLE IF EXISTS top_category_percentile;
CREATE TEMP TABLE top_category_percentile AS 
WITH calculated_cte AS (
  SELECT
    top_categories.customer_id,
    top_categories.category_name AS top_category_name,
    category_counts.category_name,
    top_categories.rental_count,
    top_categories.category_rank,
    CEILING(
      100 * PERCENT_RANK() OVER (
        PARTITION BY category_counts.category_name
        ORDER BY category_counts.rental_count desc
      )
    ) AS percentile
  FROM category_counts
  LEFT JOIN top_categories
    ON category_counts.customer_id = top_categories.customer_id
)
SELECT 
  customer_id,
  category_name,
  rental_count,
  category_rank,
  CASE 
    WHEN percentile = 0 THEN 1
    ELSE percentile
  END
FROM calculated_cte
WHERE 
  category_rank = 1 
  AND top_category_name = category_name;

-- Generate our first top category insights table using all previously generated tables
DROP TABLE IF EXISTS first_category_insights;
CREATE TEMP TABLE first_category_insights AS 
SELECT 
  top_category_percentile.customer_id,
  top_category_percentile.category_name,
  top_category_percentile.rental_count,
  top_category_percentile.rental_count - average_category_count.category_average AS average_comparison,
  top_category_percentile.percentile
FROM top_category_percentile
LEFT JOIN average_category_count
  ON average_category_count.category_name = top_category_percentile.category_name;

-- Generate the 2nd category insights
DROP TABLE IF EXISTS second_category_insights;
CREATE TEMP TABLE second_category_insights AS
SELECT 
  top_categories.customer_id,
  top_categories.category_name,
  top_categories.rental_count,
  ROUND(100 * top_categories.rental_count::NUMERIC / total_counts.total_film_count) AS total_percentage
FROM top_categories
LEFT JOIN total_counts
  ON total_counts.customer_id = top_categories.customer_id
WHERE top_categories.category_rank = 2;

-- 5.1.2 Category Recommendations
-- Generate a summarised film count table with the category included, we will use this table to rank the films by popularity
DROP TABLE IF EXISTS film_counts;
CREATE TEMP TABLE film_counts AS
SELECT DISTINCT 
  film_id,
  title,
  category_name,
  COUNT(*) OVER (
    PARTITION BY film_id
  ) AS rental_count
FROM complete_joint_dataset;

-- Create a previously watched films for the top 2 categories to exclude for each customer
DROP TABLE IF EXISTS category_film_exclusions;
CREATE TEMP TABLE category_film_exclusions AS
SELECT DISTINCT
  customer_id,
  film_id
FROM complete_joint_dataset;

-- Finally perform an anti join from the relevant category films on the exclusions and use window functions to keep the top 3 from each category by popularity - be sure to split out the recommendations by category ranking
DROP TABLE IF EXISTS category_recommendations;
CREATE TEMP TABLE category_recommendations AS 
WITH ranked_films_cte AS (
  SELECT
    top_categories.customer_id,
    top_categories.category_name,
    top_categories.category_rank,
    film_counts.film_id,
    film_counts.title,
    film_counts.rental_count,
    DENSE_RANK() OVER (
      PARTITION BY 
        top_categories.customer_id,
        top_categories.category_rank
      ORDER BY 
        film_counts.rental_count desc,
        film_counts.title
    ) AS reco_rank
  FROM top_categories
  INNER JOIN film_counts
    ON top_categories.category_name = film_counts.category_name
  WHERE NOT EXISTS (
    SELECT 1
    FROM category_film_exclusions
    WHERE
      category_film_exclusions.customer_id = top_categories.customer_id 
      AND category_film_exclusions.film_id = film_counts.film_id
  )
)
SELECT * FROM ranked_films_cte 
WHERE reco_rank <= 3;

-- Create a new base dataset which has a focus on the actor instead of category
DROP TABLE IF EXISTS actor_joint_dataset;
CREATE TEMP TABLE actor_joint_dataset AS 
SELECT 
  rental.customer_id,
  rental.rental_id,
  rental.rental_date,
  film.film_id,
  film.title,
  actor.actor_id,
  actor.first_name,
  actor.last_name
FROM dvd_rentals.rental
INNER JOIN dvd_rentals.inventory
  ON rental.inventory_id = inventory.inventory_id
INNER JOIN dvd_rentals.film
  ON inventory.film_id = film.film_id
INNER JOIN dvd_rentals.film_actor
  ON film.film_id = film_actor.film_id
INNER JOIN dvd_rentals.actor
  ON film_actor.actor_id = actor.actor_id;

-- Identify the top actor and their respective rental count for each customer based off the ranked rental counts
DROP TABLE IF EXISTS top_actor_counts;
CREATE TEMP TABLE top_actor_counts AS 
WITH actor_counts AS (
  SELECT 
    customer_id,
    actor_id,
    first_name,
    last_name,
    COUNT(*) AS rental_count,
    MAX(rental_date) AS latest_rental_date
  FROM actor_joint_dataset
  GROUP BY 
    customer_id,
    actor_id,
    first_name,
    last_name
),
ranked_actor_cte AS (
  SELECT 
  actor_counts.*,
  DENSE_RANK() OVER (
    PARTITION BY customer_id
    ORDER BY rental_count desc, latest_rental_date desc, first_name, last_name 
  ) AS actor_rank
  FROM actor_counts
)
SELECT 
  customer_id,
  actor_id,
  first_name, 
  last_name,
  rental_count
FROM ranked_actor_cte 
WHERE actor_rank = 1;

-- Generate total actor rental counts to use for film popularity ranking in later steps
DROP TABLE IF EXISTS actor_film_counts;
CREATE TEMP TABLE actor_film_counts AS 
WITH film_counts AS (
SELECT 
  film_id,
  COUNT(DISTINCT rental_id) as rental_count
FROM actor_joint_dataset
GROUP BY film_id
)
SELECT DISTINCT
  actor_joint_dataset.film_id,
  actor_joint_dataset.actor_id,
  actor_joint_dataset.title,
  film_counts.rental_count
FROM actor_joint_dataset
LEFT JOIN film_counts
  ON actor_joint_dataset.film_id = film_counts.film_id;

-- Create an updated film exclusions table which includes the previously watched films like we had for the category recommendations - but this time we need to also add in the films which were previously recommended
DROP TABLE IF EXISTS actor_film_exclusions;
CREATE TEMP TABLE actor_film_exclusions AS 
(
  SELECT DISTINCT 
    customer_id,
    film_id
  FROM complete_joint_dataset
)
UNION
(
  SELECT DISTINCT
    customer_id, 
    film_id
  FROM category_recommendations
);

-- Apply the same ANTI JOIN technique and use a window function to identify the 3 valid film recommendations for our customers
DROP TABLE IF EXISTS actor_recommendations;
CREATE TEMP TABLE actor_recommendations AS 
WITH ranked_actor_films_cte AS (
  SELECT
    top_actor_counts.customer_id,
    top_actor_counts.first_name,
    top_actor_counts.last_name,
    top_actor_counts.rental_count,
    actor_film_counts.actor_id,
    actor_film_counts.film_id,
    actor_film_counts.title,
    DENSE_RANK() OVER (
      PARTITION BY 
        top_actor_counts.customer_id
      ORDER BY 
      top_actor_counts.rental_count, 
      actor_film_counts.title
    ) AS reco_rank
  FROM top_actor_counts
  INNER JOIN actor_film_counts
    ON top_actor_counts.actor_id = actor_film_counts.actor_id
  WHERE NOT EXISTS (
    SELECT 1 
    FROM actor_film_exclusions
    WHERE actor_film_exclusions.customer_id = top_actor_counts.customer_id AND 
    actor_film_exclusions.film_id = actor_film_counts.film_id
  )
)
SELECT * FROM ranked_actor_films_cte
WHERE reco_rank <= 3;



-- Customer Level Insights
SELECT *
FROM first_category_insights
LIMIT 10;

SELECT *
FROM second_category_insights
LIMIT 10;

SELECT *
FROM top_actor_counts
LIMIT 10;

-- Recommendations
SELECT *
FROM category_recommendations
WHERE customer_id = 1
ORDER BY category_rank, reco_rank;

SELECT *
FROM actor_recommendations
ORDER BY customer_id, reco_rank
LIMIT 15;

-- final data asset
DROP TABLE IF EXISTS final_data_asset;
CREATE TEMP TABLE final_data_asset AS 
WITH first_category AS (
  SELECT 
    customer_id,
    category_name,
    CONCAT('You''ve watched ', rental_count, ' ', category_name,
    ' films, that''s ' , average_comparison,
    ' more than the DVD Rental Co average and puts you in the top ',
    percentile, '% of ', category_name, ' Gurus!') AS insight
  FROM first_category_insights
),
second_category AS (
  SELECT 
    customer_id,
    category_name,
    CONCAT('You''ve watched ', rental_count, ' ',category_name,
    ', making up ', total_percentage, '% of your entire viewing history!') AS insight
  FROM second_category_insights
),
top_actor AS (
  SELECT 
    customer_id,
    CONCAT(INITCAP(first_name), ' ',INITCAP(last_name)) AS actor_name,
    CONCAT('You''ve watched ', rental_count, ' films featuring ', INITCAP(first_name), 
    ' ', INITCAP(last_name), '! Here are some other films ', INITCAP(first_name), ' stars in that might interest you!'
    ) AS insight
  FROM top_actor_counts
),
adjusted_title_case_category_recommendations AS (
  SELECT
    customer_id,
    INITCAP(title) AS title,
    category_rank,
    reco_rank
  FROM category_recommendations
),
wide_category_recommendations AS (
  SELECT 
    customer_id,
    MAX(CASE WHEN category_rank = 1 AND reco_rank = 1 THEN title END) AS cat_1_reco_1,
    MAX(CASE WHEN category_rank = 1 AND reco_rank = 2 THEN title END) AS cat_1_reco_2,
    MAX(CASE WHEN category_rank = 1 AND reco_rank = 3 THEN title END) AS cat_1_reco_3,
    MAX(CASE WHEN category_rank = 2 AND reco_rank = 1 THEN title END) AS cat_2_reco_1, 
    MAX(CASE WHEN category_rank = 2 AND reco_rank = 2 THEN title END) AS cat_2_reco_2, 
    MAX(CASE WHEN category_rank = 2 AND reco_rank = 3 THEN title END) AS cat_2_reco_3 
  FROM adjusted_title_case_category_recommendations
  GROUP BY customer_id
),
adjusted_title_case_actor_recommendations AS (
  SELECT 
    customer_id,
    INITCAP(title) AS title,
    reco_rank
  FROM actor_recommendations
),
wide_actor_recommendations AS (
  SELECT
    customer_id,
    MAX(CASE WHEN reco_rank = 1 THEN title END) AS actor_reco_1,
    MAX(CASE WHEN reco_rank = 2 THEN title END) AS actor_reco_2,
    MAX(CASE WHEN reco_rank = 3 THEN title END) AS actor_reco_3
  FROM adjusted_title_case_actor_recommendations
  GROUP BY customer_id
),
final_output AS (
  SELECT 
    t1.customer_id,
    t1.category_name AS cat_1,
    t4.cat_1_reco_1,
    t4.cat_1_reco_2,
    t4.cat_1_reco_3,
    t2.category_name AS cat_2,
    t4.cat_2_reco_1,
    t4.cat_2_reco_2,
    t4.cat_2_reco_3,
    t3.actor_name,
    t5.actor_reco_1,
    t5.actor_reco_2,
    t5.actor_reco_3,
    t1.insight AS cat_1_insight,
    t2.insight AS cat_2_insight,
    t3.insight AS actor_insight
  FROM first_category AS t1
  INNER JOIN second_category AS t2
    ON t1.customer_id = t2.customer_id
  INNER JOIN top_actor AS t3
    ON t1.customer_id = t3.customer_id
  INNER JOIN wide_category_recommendations AS t4 
    ON t1.customer_id = t4.customer_id
  INNER JOIN wide_actor_recommendations AS t5 
    ON t1.customer_id = t5.customer_id
)
SELECT * FROM final_output;

SELECT *
FROM final_data_asset

-- Easier Viewing 

WITH first_category AS (
  SELECT 
    customer_id,
    category_name,
    CONCAT('You''ve watched ', rental_count, ' ', category_name,
    ' films, that''s ' , average_comparison,
    ' more than the DVD Rental Co average and puts you in the top ',
    percentile, '% of ', category_name, ' Gurus!') AS insight
  FROM first_category_insights
),
second_category AS (
  SELECT 
    customer_id,
    category_name,
    CONCAT('You''ve watched ', rental_count, ' ',category_name,
    ', making up ', total_percentage, '% of your entire viewing history!') AS insight
  FROM second_category_insights
),
top_actor AS (
  SELECT 
    customer_id,
    CONCAT(INITCAP(first_name), ' ',INITCAP(last_name)) AS actor_name,
    CONCAT('You''ve watched ', rental_count, ' films featuring ', INITCAP(first_name), 
    ' ', INITCAP(last_name), '! Here are some other films ', INITCAP(first_name), ' stars in that might interest you!'
    ) AS insight
  FROM top_actor_counts
),
top_category_and_actor_only AS (
  SELECT
    t1.customer_id,
    t1.category_name AS cat_1,
    t2.category_name AS cat_2,
    t3.actor_name AS actor
  FROM first_category AS t1
  INNER JOIN second_category AS t2
    ON t1.customer_id = t2.customer_id
  INNER JOIN top_actor AS t3 
    ON t1.customer_id = t3.customer_id
),
all_insights_only AS (
  SELECT
    t1.customer_id,
    t1.insight AS insight_cat_1,
    t2.insight AS insight_cat_2,
    t3.insight AS insight_actor
  FROM first_category AS t1
  INNER JOIN second_category AS t2
    ON t1.customer_id = t2.customer_id
  INNER JOIN top_actor AS t3 
    ON t1.customer_id = t3.customer_id
),
long_form_recommendations AS (
  SELECT
    customer_id, 
    'category' AS reco_type,
    category_name AS reco_name,
    INITCAP(title) AS title,
    category_rank AS email_rank,
    reco_rank
  FROM category_recommendations
  UNION 
  SELECT 
    customer_id,
    'actor' AS reco_type,
    CONCAT(INITCAP(first_name), ' ', INITCAP(last_name)) AS reco_name,
    INITCAP(title) AS title,
    3 AS email_rank,
    reco_rank
  FROM actor_recommendations
)
SELECT * FROM long_form_recommendations
ORDER BY customer_id, email_rank, reco_rank
LIMIT 10
-- top_category_and_actor_only
-- all_insights_only
