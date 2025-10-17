-- 2. Total Sold Amount per City (4 marks) Write a query to calculate the total amount grouped by customer’s cities (Their city at the time of the order). --
SELECT
    dc.city AS customer_city_at_order,
    SUM(fo.amount) AS total_sold_amount
FROM
    fact_orders fo
JOIN
    dim_customers dc ON fo.customer_id = dc.customer_id
    AND fo.order_date >= dc.start_date
    AND (dc.end_date IS NULL OR fo.order_date < dc.end_date)
GROUP BY
    dc.city
ORDER BY
    total_sold_amount DESC;