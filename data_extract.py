def data_extract(**kwargs):
    # create connection 
    #Install pyodbc module if it is not installed by running below command
    #pip install pyodbc

    #below code is to import pyodbc and specify server credentials of source database
    import pandas as pd
    import pyodbc
    import logging
    SERVER = 'tcp:ankitv1004.database.windows.net,1433;'
    DATABASE = 'sourcedb'
    USERNAME = 'CloudSA3952ed7e@ankitv1004'
    PASSWORD = 'onetwo@12'

    connectionString = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}'

    #create connections to source DB hosted on SQL Server

    conn = pyodbc.connect(connectionString)

    cursor = conn.cursor()
    
    query1="""select * from customers
        """
    cursor.execute(
        query1
    )
    # records = cursor.fetchall()
    # for r in records:
    #     print(r)
        
    # Fetch all rows from the executed query
    rows = cursor.fetchall()

    # Get column names from the cursor description
    columns = [column[0] for column in cursor.description]

    # Create DataFrame from rows with column names
    df_customers = pd.DataFrame.from_records(rows, columns=columns)

    query2="""select * from orders"""
    
    cursor.execute(
        query2
    )
       
    # Fetch all rows from the executed query
    rows = cursor.fetchall()

    # Get column names from the cursor description
    columns = [column[0] for column in cursor.description]

    # Create DataFrame from rows with column names
    df_orders = pd.DataFrame.from_records(rows, columns=columns)
    # print(df_orders)
    
    query3="""select * from order_items"""
    
    cursor.execute(
        query3
    )
       
    # Fetch all rows from the executed query
    rows = cursor.fetchall()

    # Get column names from the cursor description
    columns = [column[0] for column in cursor.description]

    # Create DataFrame from rows with column names
    df_order_items = pd.DataFrame.from_records(rows, columns=columns)
    
    
    query4="""select * from products"""
    
    cursor.execute(
        query4
    )
       
    # Fetch all rows from the executed query
    rows = cursor.fetchall()

    # Get column names from the cursor description
    columns = [column[0] for column in cursor.description]

    # Create DataFrame from rows with column names
    df_products = pd.DataFrame.from_records(rows, columns=columns)
    
    query5="""select * from categories"""
    
    cursor.execute(
        query5
    )
       
    # Fetch all rows from the executed query
    rows = cursor.fetchall()

    # Get column names from the cursor description
    columns = [column[0] for column in cursor.description]

    # Create DataFrame from rows with column names
    df_categories = pd.DataFrame.from_records(rows, columns=columns)
    
    query6="""select * from reviews"""
    
    cursor.execute(
        query6
    )
       
    # Fetch all rows from the executed query
    rows = cursor.fetchall()

    # Get column names from the cursor description
    columns = [column[0] for column in cursor.description]

    # Create DataFrame from rows with column names
    df_reviews = pd.DataFrame.from_records(rows, columns=columns)
    
    
    # df_orders['order_date'] = df_orders['order_date'].astype(str)
    # df_orders['total_amount'] = df_orders['total_amount'].astype(str)
    
    date_columns_orders = df_orders.select_dtypes(include=['datetime', 'datetimetz', 'object']).columns
    df_orders[date_columns_orders] = df_orders[date_columns_orders].astype(str)
    # print(df_orders.dtypes)
    
    date_columns_order_items = df_order_items.select_dtypes(include=['datetime', 'datetimetz', 'object']).columns
    df_order_items[date_columns_order_items] = df_order_items[date_columns_order_items].astype(str)
    
    date_columns_reviews = df_reviews.select_dtypes(include=['datetime', 'datetimetz', 'object']).columns
    df_reviews[date_columns_reviews] = df_reviews[date_columns_reviews].astype(str)
    

    df_customers = df_customers.to_dict()
    df_orders = df_orders.to_dict()
    df_order_items=df_order_items.to_dict()
    df_products=df_products.to_dict()
    df_reviews=df_reviews.to_dict()
    df_categories=df_categories.to_dict()
    
    ti = kwargs['ti']
    kwargs['ti'].xcom_push(key='df_customers', value=df_customers)
    kwargs['ti'].xcom_push(key='df_orders', value=df_orders)
    kwargs['ti'].xcom_push(key='df_order_items', value=df_order_items)
    kwargs['ti'].xcom_push(key='df_products', value=df_products)
    kwargs['ti'].xcom_push(key='df_categories', value=df_categories)
    kwargs['ti'].xcom_push(key='df_reviews', value=df_reviews)
    
    logger = logging.getLogger("airflow.task")
    task_instance = kwargs['ti']
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id
    run_id = task_instance.run_id

    # Log task information
    logger.info(f"Running task _Test: {task_id}")
    logger.info(f"DAG ID: {dag_id}")
    logger.info(f"Run ID: {run_id}")
#data_extract()
