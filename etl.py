import psycopg2
from pymongo import MongoClient # pip install pymongo psycopg2-binary python-dotenv (required)
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

pg_conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME", "storedb2"),
    user=os.getenv("DB_USER", "postgres"),
    password=os.getenv("DB_PASSWORD", "jk2004"),
    host=os.getenv("DB_HOST", "localhost"),
    port=os.getenv("DB_PORT", 5432)
)
pg_cur = pg_conn.cursor()

mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["sales_db"]
orders_summary = mongo_db["orders_summary"]

# Clear old mongo data
orders_summary.delete_many({})
print("Cleared existing MongoDB data.")

sql = """
SELECT f.order_id,
       f.order_date,
       f.customer_id,
       c.name AS customer_name,
       c.city AS customer_city,
       f.product_id,
       p.name AS product_name,
       p.price AS product_price,
       f.amount
FROM fact_orders f
-- join to the customer version that was valid at order_date
LEFT JOIN dim_customers c
  ON f.customer_id = c.customer_id
  AND c.start_date <= f.order_date
  AND (c.end_date IS NULL OR c.end_date > f.order_date)
-- join to the product version that was valid at order_date
LEFT JOIN dim_products p
  ON f.product_id = p.product_id
  AND p.start_date <= f.order_date
  AND (p.end_date IS NULL OR p.end_date > f.order_date)
ORDER BY f.order_date;
"""

pg_cur.execute(sql)
rows = pg_cur.fetchall()
print(f"Extracted {len(rows)} rows from PostgreSQL.")

docs = []
for row in rows:
    order_id, order_date, customer_id, customer_name, customer_city, \
    product_id, product_name, product_price, amount = row

    if isinstance(order_date, datetime):
        order_date_iso = order_date.isoformat()
    else:
        order_date_iso = str(order_date)

    doc = {
        "order_id": order_id,
        "order_date": order_date_iso,
        "customer_id": customer_id,
        "customer_name": customer_name,
        "customer_city": customer_city,
        "product_id": product_id,
        "product_name": product_name,
        "product_price": float(product_price) if product_price is not None else None,
        "amount": float(amount) if amount is not None else None
    }
    docs.append(doc)

if docs:
    orders_summary.insert_many(docs)
print(f"Inserted {len(docs)} documents into MongoDB collection 'orders_summary'.")

pg_cur.close()
pg_conn.close()
mongo_client.close()
