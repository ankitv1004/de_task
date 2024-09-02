def insert_data(**kwargs):
    from pymongo import MongoClient
    import pandas as pd

    # MongoDB Atlas connection string with srv
    connection_string = "mongodb+srv://ankitchail:Jm8K9C54gxSWxqso@cluster0.u41jx.mongodb.net/"

    # Connect to MongoDB Atlas
    client = MongoClient(connection_string)

    # Access the database and collection
    db = client['target_db'] 
    summary_cn = db['summary']  
    
    #Insights collections
    customers_with_avg_order100_cn=db['customers_with_avg_order100']
    top_5_customers_by_customer_ratings_cn=db['top_5_customers_by_customer_ratings']
    top_5_customers_by_total_orders_cn=db['top_5_customers_by_total_orders']
    top_5_customers_by_amountspent_cn=db['top_5_customers_by_amountspent']
    
    #pull summary data from previous task
    ti = kwargs['ti']
    summary=kwargs['ti'].xcom_pull(key='summary', task_ids='transform_data')
    
    summary = pd.DataFrame(summary)
    
    #Generate and insert Insights
    #1
    customers_with_avg_order100=summary[summary['average_order_value']>=100][['customer_id','name']]
    customers_with_avg_order100=customers_with_avg_order100.to_dict('records')
    customers_with_avg_order100_cn.insert_many(customers_with_avg_order100)
    
    #2
    top_5_customers_by_customer_ratings = summary.nlargest(5, 'average_rating')[['customer_id','name']]
    top_5_customers_by_customer_ratings=top_5_customers_by_customer_ratings.to_dict('records')
    top_5_customers_by_customer_ratings_cn.insert_many(top_5_customers_by_customer_ratings)
    
    #3
    top_5_customers_by_total_orders = summary.nlargest(5, 'total_orders')[['customer_id','name']]
    top_5_customers_by_total_orders=top_5_customers_by_total_orders.to_dict('records')
    top_5_customers_by_total_orders_cn.insert_many(top_5_customers_by_total_orders)
    
    #4
    top_5_customers_by_amountspent = summary.nlargest(5, 'total_amount_spent')[['customer_id','name']]
    top_5_customers_by_amountspent=top_5_customers_by_amountspent.to_dict('records')
    top_5_customers_by_amountspent_cn.insert_many(top_5_customers_by_amountspent)
    
    
    #insert summary
    
    summary=summary.to_dict('records')
    summary_cn.insert_many(summary)

    print("Data inserted successfully.")