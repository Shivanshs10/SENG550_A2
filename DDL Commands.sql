DROP TABLE IF EXISTS dim_customers CASCADE;

DROP TABLE IF EXISTS dim_products CASCADE;

DROP TABLE IF EXISTS fact_orders CASCADE;

CREATE TABLE dim_customers (
    id SERIAL PRIMARY KEY,
    customer_id TEXT,
    name TEXT,
    email TEXT,
    city TEXT,
    start_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_date TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

CREATE TABLE dim_products (
    id SERIAL PRIMARY KEY,
    product_id TEXT,
    name TEXT,
    category TEXT,
    price NUMERIC,
    start_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_date TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

CREATE TABLE fact_orders (
    order_id TEXT PRIMARY KEY,
    product_id TEXT,
    customer_id TEXT,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    amount NUMERIC
);
