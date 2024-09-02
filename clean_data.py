def clean_data(**kwargs):
    import pandas as pd
    import logging
    ti = kwargs['ti']
    df_customers=kwargs['ti'].xcom_pull(key='df_customers', task_ids='data_extract')
    df_orders=kwargs['ti'].xcom_pull(key='df_orders', task_ids='data_extract')
    df_order_items=kwargs['ti'].xcom_pull(key='df_order_items', task_ids='data_extract')
    df_products=kwargs['ti'].xcom_pull(key='df_products', task_ids='data_extract')
    df_categories=kwargs['ti'].xcom_pull(key='df_categories', task_ids='data_extract')
    df_reviews=kwargs['ti'].xcom_pull(key='df_reviews', task_ids='data_extract')
    
    df_customers = pd.DataFrame(df_customers)
    df_orders=pd.DataFrame(df_orders)
    df_order_items = pd.DataFrame(df_order_items)
    df_categories = pd.DataFrame(df_categories)
    df_products = pd.DataFrame(df_products)
    df_reviews = pd.DataFrame(df_reviews)

    
    df_customers = df_customers.drop_duplicates(subset=['customer_id'], keep=False)
    df_customers = df_customers.dropna(subset=['customer_id'], how='all')
    df_orders = df_orders.dropna(subset=['order_id'], how='all')
    df_products = df_products.dropna(subset=['product_id'], how='all')
    df_reviews = df_reviews.dropna(subset=['review_id'], how='all')
    df_categories = df_categories.dropna(subset=['category_id'], how='all')
    
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
