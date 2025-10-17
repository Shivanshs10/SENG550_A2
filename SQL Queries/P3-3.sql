-- 3. Total Price vs. Actual Amount Difference (4 marks) Write a query to calculate the sum of differences between listed product price (at the time of the order) and paid amount (price - amount). --
SELECT
    SUM(dp.price - fo.amount) AS total_price_vs_amount_difference
FROM
    fact_orders fo
JOIN
    dim_products dp ON fo.product_id = dp.product_id
    AND fo.order_date >= dp.start_date
    AND (dp.end_date IS NULL OR fo.order_date < dp.end_date);