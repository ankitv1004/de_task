
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

# This is DDL to create table in source db

SQL_STATEMENT1 = """
CREATE TABLE customers (
customer_id INT PRIMARY KEY,
name VARCHAR(100) NOT NULL,
email VARCHAR(100) UNIQUE NOT NULL,
country VARCHAR(50) NOT NULL
)
"""

cursor = conn.cursor()

cursor.execute(
    SQL_STATEMENT1
)


SQL_STATEMENT2 = """
CREATE TABLE orders (
order_id INT PRIMARY KEY,
customer_id INT NOT NULL,
order_date DATE NOT NULL,
total_amount DECIMAL(10, 2) NOT NULL,
status VARCHAR(20) NOT NULL,
FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);
"""

cursor.execute(
    SQL_STATEMENT2
)


SQL_STATEMENT5 = """
CREATE TABLE categories (
category_id INT PRIMARY KEY,
category_name VARCHAR(100) NOT NULL
);
"""

cursor.execute(
    SQL_STATEMENT5
)

SQL_STATEMENT4 = """
CREATE TABLE products (
product_id INT PRIMARY KEY,
product_name VARCHAR(100) NOT NULL,
category_id INT NOT NULL,
FOREIGN KEY (category_id) REFERENCES categories(category_id)
);
"""

cursor.execute(
    SQL_STATEMENT4
)

SQL_STATEMENT3 = """
CREATE TABLE order_items (
item_id INT PRIMARY KEY,
order_id INT NOT NULL,
product_id INT NOT NULL,
quantity INT NOT NULL,
price DECIMAL(10, 2) NOT NULL,
FOREIGN KEY (order_id) REFERENCES orders(order_id),
FOREIGN KEY (product_id) REFERENCES products(product_id)
);
"""

cursor.execute(
    SQL_STATEMENT3
)

SQL_STATEMENT6 = """
CREATE TABLE reviews (
review_id INT PRIMARY KEY,
product_id INT NOT NULL,
customer_id INT NOT NULL,
rating INT CHECK (rating BETWEEN 1 AND 5),
review_date DATE NOT NULL,
FOREIGN KEY (product_id) REFERENCES products(product_id),
FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);
"""

cursor.execute(
    SQL_STATEMENT6
)

conn.commit()
#Check if all tables are created
cursor.execute(
    """ SELECT * FROM INFORMATION_SCHEMA.TABLES;"""
)
records = cursor.fetchall()
for r in records:
    print(r)