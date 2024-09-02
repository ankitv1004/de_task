def clean_data(**kwargs):
    import pandas as pd
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

    
    
    # print(df_customers,df_orders,df_order_items,df_products,df_categories,df_reviews)
    # print(df_customers.dtypes,df_orders.dtypes,df_order_items.dtypes,df_products.dtypes,df_categories.dtypes,df_reviews.dtypes)
    
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