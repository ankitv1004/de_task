
# Script to run all DDL of source tables
#***************************************************#

#Install pyodbc module if it is not installed by running below command
#pip install pyodbc

#below code is to import pyodbc and specify server credentials of source database

import pyodbc
SERVER = 'tcp:ankitv1004.database.windows.net,1433;'
DATABASE = 'sourcedb'
USERNAME = 'CloudSA3952ed7e@ankitv1004'
PASSWORD = 'onetwo@12'

connectionString = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}'

#create connections to source DB hosted on SQL Server

conn = pyodbc.connect(connectionString)

cursor = conn.cursor()

insert1="""INSERT INTO customers (customer_id, name, email, country)
VALUES
    (1, 'Alice Johnson', 'alice.johnson@example.com', 'United States'),
    (2, 'Bob Smith', 'bob.smith@example.com', 'Canada'),
    (3, 'Charlie Brown', 'charlie.brown@example.com', 'United Kingdom'),
    (4, 'David Williams', 'david.williams@example.com', 'Australia'),
    (5, 'Eva Green', 'eva.green@example.com', 'Germany'),
    (6, 'Frank Miller', 'frank.miller@example.com', 'France'),
    (7, 'Grace Lee', 'grace.lee@example.com', 'South Korea'),
    (8, 'Hannah Kim', 'hannah.kim@example.com', 'South Korea'),
    (9, 'Isaac Newton', 'isaac.newton@example.com', 'United Kingdom'),
    (10, 'Julia Roberts', 'julia.roberts@example.com', 'United States');
    """
cursor.execute(
    insert1
)


insert2= """
INSERT INTO orders (order_id, customer_id, order_date, total_amount, status)
VALUES
    (1, 1, '2024-01-15', 150.75, 'Completed'),
    (2, 2, '2024-02-10', 250.00, 'Pending'),
    (3, 3, '2024-02-25', 120.00, 'Shipped'),
    (4, 4, '2024-03-05', 300.50, 'Completed'),
    (5, 5, '2024-03-15', 500.00, 'Cancelled'),
    (6, 1, '2024-03-20', 75.25, 'Pending'),
    (7, 6, '2024-03-25', 80.00, 'Completed'),
    (8, 7, '2024-04-01', 45.00, 'Shipped'),
    (9, 8, '2024-04-10', 600.00, 'Pending'),
    (10, 9, '2024-04-15', 90.00, 'Completed');

"""
cursor.execute(
    insert2
)

insert3=""" 
INSERT INTO categories (category_id, category_name)
VALUES
    (1, 'Electronics'),
    (2, 'Books'),
    (3, 'Clothing'),
    (4, 'Home Appliances'),
    (5, 'Toys'),
    (6, 'Sports Equipment'),
    (7, 'Beauty Products'),
    (8, 'Automotive'),
    (9, 'Garden Supplies'),
    (10, 'Grocery');
"""
cursor.execute(
    insert3
)

insert4="""
INSERT INTO products (product_id, product_name, category_id)
VALUES
    (1, 'Smartphone', 1),          -- Electronics
    (2, 'Laptop', 1),              -- Electronics
    (3, 'Science Fiction Novel', 2), -- Books
    (4, 'T-shirt', 3),             -- Clothing
    (5, 'Microwave Oven', 4),      -- Home Appliances
    (6, 'Action Figure', 5),       -- Toys
    (7, 'Basketball', 6),          -- Sports Equipment
    (8, 'Lipstick', 7),            -- Beauty Products
    (9, 'Car Tire', 8),            -- Automotive
    (10, 'Garden Shovel', 9);      -- Garden Supplies
"""
cursor.execute(
    insert4
)


insert5="""
INSERT INTO order_items (item_id, order_id, product_id, quantity, price)
VALUES
    (1, 1, 1, 2, 150.75),  -- Order 1, 2 Smartphones, $150.75 each
    (2, 1, 3, 1, 12.50),   -- Order 1, 1 Science Fiction Novel, $12.50 each
    (3, 2, 2, 1, 250.00),  -- Order 2, 1 Laptop, $250.00 each
    (4, 3, 4, 3, 15.00),   -- Order 3, 3 T-shirts, $15.00 each
    (5, 4, 5, 1, 300.50),  -- Order 4, 1 Microwave Oven, $300.50 each
    (6, 5, 6, 5, 10.00),   -- Order 5, 5 Action Figures, $10.00 each
    (7, 6, 7, 2, 40.00),   -- Order 6, 2 Basketballs, $40.00 each
    (8, 7, 8, 3, 25.00),   -- Order 7, 3 Lipsticks, $25.00 each
    (9, 8, 9, 1, 60.00),   -- Order 8, 1 Car Tire, $60.00 each
    (10, 9, 10, 2, 30.00); -- Order 9, 2 Garden Shovels, $30.00 each

"""
cursor.execute(
    insert5
)


insert6="""
INSERT INTO reviews (review_id, product_id, customer_id, rating, review_date)
VALUES
    (1, 1, 1, 5, '2024-04-20'),  -- Customer 1 reviews Smartphone with a rating of 5
    (2, 2, 2, 4, '2024-04-22'),  -- Customer 2 reviews Laptop with a rating of 4
    (3, 3, 3, 3, '2024-04-25'),  -- Customer 3 reviews Science Fiction Novel with a rating of 3
    (4, 4, 4, 5, '2024-05-01'),  -- Customer 4 reviews T-shirt with a rating of 5
    (5, 5, 5, 2, '2024-05-03'),  -- Customer 5 reviews Microwave Oven with a rating of 2
    (6, 6, 6, 4, '2024-05-05'),  -- Customer 6 reviews Action Figure with a rating of 4
    (7, 7, 7, 5, '2024-05-07'),  -- Customer 7 reviews Basketball with a rating of 5
    (8, 8, 8, 3, '2024-05-10'),  -- Customer 8 reviews Lipstick with a rating of 3
    (9, 9, 9, 4, '2024-05-12'),  -- Customer 9 reviews Car Tire with a rating of 4
    (10, 10, 10, 5, '2024-05-15'); -- Customer 10 reviews Garden Shovel with a rating of 5
"""
cursor.execute(
    insert6
)

conn.commit()
