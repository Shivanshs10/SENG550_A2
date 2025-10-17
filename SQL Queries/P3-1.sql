-- 1. Count the number of different cities each customer has lived in. (2 marks) --
SELECT
    customer_id,
    COUNT(DISTINCT city) AS number_of_different_cities
FROM
    dim_customers
GROUP BY
    customer_id
ORDER BY
    customer_id;