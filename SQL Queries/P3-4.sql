-- 4. Write a query to return all orders for a specific customer (e.g., C1), including: order_id, order_date, customer_city (at the time of order), product_name, product_price, amount.(4 Marks) --
SELECT
    fo.order_id,
    fo.order_date,
    dc.city AS customer_city_at_order,
    dp.name AS product_name_at_order,
    dp.price AS product_price_at_order,
    fo.amount AS paid_amount
FROM
    fact_orders fo
JOIN
    dim_customers dc ON fo.customer_id = dc.customer_id
    AND fo.order_date >= dc.start_date
    AND (dc.end_date IS NULL OR fo.order_date < dc.end_date)
JOIN
    dim_products dp ON fo.product_id = dp.product_id
    AND fo.order_date >= dp.start_date
    AND (dp.end_date IS NULL OR fo.order_date < dp.end_date)
WHERE
    fo.customer_id = 'C1' -- Filter for C1 in this case
ORDER BY
    fo.order_date;