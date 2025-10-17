# SENG550_A2

## PART 1

The screenshots of the tables after they've been created:

![alt text](<Screenshots/Screenshot 2025-10-13 205615.png>)

![alt text](<Screenshots/Screenshot 2025-10-13 205624.png>)

## PART 2

Screenshot of the tables after running the Python code

![alt text](<Screenshots/Screenshot 2025-10-13 212846.png>)
![alt text](<Screenshots/Screenshot 2025-10-13 212857.png>)
![alt text](<Screenshots/Screenshot 2025-10-13 212908.png>)


## PART 3

The SQL Query Outputs are below, and a file of all the queries is attached under queries.sql:

![alt text](<Screenshots/Screenshot 2025-10-13 212937.png>)
![alt text](<Screenshots/Screenshot 2025-10-17 001438.png>)
![alt text](<Screenshots/Screenshot 2025-10-17 001456.png>)
![alt text](<Screenshots/Screenshot 2025-10-17 001517.png>)



## PART 4

The database has been set up, so no screenshot required

## PART 5

### Part A

Image of the content in the MongoDB database:

![alt text](<Screenshots/Screenshot 2025-10-14 212538.png>)

### Part B

Queries outputs:

![alt text](<Screenshots/Screenshot 2025-10-14 212306.png>)
![alt text](<Screenshots/Screenshot 2025-10-14 212321.png>)
![alt text](<Screenshots/Screenshot 2025-10-14 212340.png>)
![alt text](<Screenshots/Screenshot 2025-10-14 212415.png>)


## PART 8

Advantages:
2 advantages of adding a MongoDB pipeline to the data architecture are that it improves read performance for analysis queries because MongoDB can store denormalized and pre-aggregated documents. This  makes analytical queries faster as fewer joins are needed. Another advantage is that it is better for scalability for reads because MongoDB is designed to scale horizontally, so it can handle large volumes of read heavy workloads better than other databases can.


Disadvantages:
3 disadvantages of adding a MongoDB pipeline are that there is data redundancy and integrity issues because MongoDB stores denormalized data. This means there can be duplicated data which increases the risk of inconsistent data across documents. Another disadtantage is that it takes more effort because you need to have etl piplines, and have to handle data type conversions. Lastly, since data is not updated in real time, there can be data latency which is not helpful when you need the latest data.


## PART 9

### A snippet for incremental loading would look something like this:
- First, you would find the latest inserted order in MongoDB, and then you query it to fetch the new orders from PostgreSQL

(Find the latest inserted order_id in MongoDB)
latest_order = mongo_collection.find_one(sort=[("order_id", -1)])
last_order_id = latest_order["order_id"] if latest_order else 0

(Only fetch new orders from PostgreSQL)
query = f"""
SELECT 
    fo.order_id, fo.order_date, dc.customer_id, dc.name, dc.city,
    dp.product_id, dp.name, dp.price, fo.amount
FROM fact_orders fo
JOIN dim_customers dc ON fo.customer_id = dc.customer_id
JOIN dim_products dp ON fo.product_id = dp.product_id
WHERE fo.order_id > {last_order_id}
ORDER BY fo.order_id;
"""
pg_cursor.execute(query)
new_rows = pg_cursor.fetchall()

### For the pipeline to run periodically, you could use this in your command line:

- */5 * * * * /usr/bin/python3 /path/to/etl.py

### If order is inserted more than once

If an order is inserted more than once then you will get duplicated data and confusion in queries.

### Incremental loading vs Fully reloading

An advantage of incremental loading is that only new data is inserted, and it is faster and requires less resource usage. However, it is more complex to implement. On the other hand, fully reloading the data is simpler to implement, but it is slower and takes up more resources

