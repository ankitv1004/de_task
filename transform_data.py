def transform_data(**kwargs):
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
    
    
    df_join1=pd.merge(df_customers,df_orders,on='customer_id',how='inner')
    df_join1['total_amount']=df_join1['total_amount'].astype(float)
    
    customer_summary = df_join1.groupby(['customer_id', 'name', 'email', 'country']).agg(
        total_amount_spent=('total_amount', 'sum'),
        total_orders=('order_id', 'count'),
        average_order_value=('total_amount', 'mean')
        ).reset_index()
    customer_summary

    total_products_ordered = df_order_items.groupby('order_id')['quantity'].sum().reset_index()
    total_products_ordered.columns = ['order_id', 'total_products_ordered']
    total_products_ordered
    
    total_products_ordered=pd.merge(df_orders[['order_id','customer_id']],total_products_ordered,on='order_id',how='inner')
    total_products_ordered=total_products_ordered.groupby('customer_id')['total_products_ordered'].sum().reset_index()
    total_products_ordered
    
    average_rating = df_reviews.groupby('customer_id')['rating'].mean().reset_index()
    average_rating.columns = ['customer_id', 'average_rating']
    average_rating
    
    summary=pd.merge(customer_summary,total_products_ordered,on='customer_id',how='inner')
    summary=pd.merge(summary,average_rating,on='customer_id',how='inner')
    print(summary)
    
    date_columns_reviews = summary.select_dtypes(include=['datetime', 'datetimetz', 'object']).columns
    summary[date_columns_reviews] = summary[date_columns_reviews].astype(str)
    summary=summary.to_dict()
    ti = kwargs['ti']
    kwargs['ti'].xcom_push(key='summary', value=summary)
    
    
