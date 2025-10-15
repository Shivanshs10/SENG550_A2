import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME", "storedb2"),
    user=os.getenv("DB_USER", "postgres"),
    password=os.getenv("DB_PASSWORD", "jk2004"),
    host=os.getenv("DB_HOST", "localhost"),
    port=os.getenv("DB_PORT", 5432)
)
cur = conn.cursor()

def add_product(product_id, name, category, price):
    cur.execute(
        """INSERT INTO dim_products (product_id, name, category, price, start_date, is_current) VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, TRUE)""", (product_id, name, category, price)
        )
    conn.commit()
    print(f"Added product {product_id} - {name}")


def add_customer(customer_id, name, email, city):
    cur.execute(
        """INSERT INTO dim_customers (customer_id, name, email, city, start_date, is_current) VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, TRUE)""", (customer_id, name, email, city)
        )
    conn.commit()
    print(f"Added customer {customer_id} - {name}")


def update_customer_city(customer_id, new_city):
    # close old record
    cur.execute(
        """UPDATE dim_customers SET end_date = CURRENT_TIMESTAMP, is_current = FALSE WHERE customer_id = %s AND is_current = TRUE""", (customer_id,)
        )
    # copy static fields
    cur.execute(
        """SELECT name, email FROM dim_customers WHERE customer_id = %s ORDER BY id DESC LIMIT 1""", (customer_id,)
        )
    name, email = cur.fetchone()
    # insert new version
    cur.execute(
        """INSERT INTO dim_customers (customer_id, name, email, city, start_date, is_current) VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, TRUE)""", (customer_id, name, email, new_city)
        )
    conn.commit()
    print(f"Updated {customer_id}'s city → {new_city}")


def update_product_price(product_id, new_price):
    cur.execute(
        """UPDATE dim_products SET end_date = CURRENT_TIMESTAMP, is_current = FALSE WHERE product_id = %s AND is_current = TRUE""", (product_id,)
        )
    cur.execute(
        """SELECT name, category FROM dim_products WHERE product_id = %s ORDER BY id DESC LIMIT 1""", (product_id,)
        )
    name, category = cur.fetchone()
    cur.execute(
        """INSERT INTO dim_products (product_id, name, category, price, start_date, is_current) VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, TRUE)""", (product_id, name, category, new_price)
        )
    conn.commit()
    print(f"Updated {product_id}'s price → {new_price}")


def add_order(order_id, product_id, customer_id, amount):
    cur.execute(
        """INSERT INTO fact_orders (order_id, product_id, customer_id, order_date, amount) VALUES (%s, %s, %s, CURRENT_TIMESTAMP, %s)""", (order_id, product_id, customer_id, amount)
        )
    conn.commit()
    print(f"Added order {order_id}: {customer_id} bought {product_id} for {amount}")


# Executing the Operations

def run_operations():
    print("\n--- Executing Part 2 Operations ---")

    # 1-Add product P1 (Laptop, Electronics, $1000)
    add_product("P1", "Laptop", "Electronics", 1000)

    # 2-Add product P2 (Phone, Electronics, $500)
    add_product("P2", "Phone", "Electronics", 500)

    # 3–Add customer C1 (Alice, New York)
    add_customer("C1", "Alice", "alice@example.com", "New York")

    # 4-Add customer C2 (Bob, Boston)
    add_customer("C2", "Bob", "bob@example.com", "Boston")

    # 5-Add order O1: C1 buys P1 for $1000
    add_order("O1", "P1", "C1", 1000)

    # 6-Update C1 city to Chicago
    update_customer_city("C1", "Chicago")

    # 7-Update P1 price to 900
    update_product_price("P1", 900)

    # 8-Order O2: C1 buys P1 for 850
    add_order("O2", "P1", "C1", 850)

    # 9-Update C2 city to Calgary
    update_customer_city("C2", "Calgary")

    # 10-Order O3: C2 buys P2 for 500
    add_order("O3", "P2", "C2", 500)

    # 11-Order O4: C1 buys P1 for 900
    add_order("O4", "P1", "C1", 900)

    # 12-Update C1 city to San Francisco
    update_customer_city("C1", "San Francisco")

    # 13-Order O5: C1 buys P2 for 450
    add_order("O5", "P2", "C1", 450)

    # 14-Order O6: C2 buys P1 for 900
    add_order("O6", "P1", "C2", 900)

    print("\n--- All operations completed ---")

if __name__ == "__main__":
    run_operations()
    cur.close()
    conn.close()
