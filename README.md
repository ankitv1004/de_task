# de_task
Scripts for ETL

This project extracts data from Azure SQL db hosted on SQL Server on Azure cloud, 
cleans the data,
transforma the data 
and loads the data to Mongo db cloud.

The decription of included scripts is as follows:
1. source_table_DDL- It has DDLs for creating tables tables on source db.
2. insert_data- This file contains insert scripts to insert data in source tables.
3. data_extract- This file contains scripts to extract data from source db.
4. clean_data- This file contains scripts to clean the extracted data.
5. transform_data- This file contains scripts to transform data.
6. insert_data- This file insert final summary data and insight data in target db.
7. etl_dag- This is the dag file for airflow.

   Requirements in airflow environment-
   1. pyodbc module
   2. pymongo
   3. pandas
  
   Note- Sometimes Sql server does not get connected. If it happens rerunning script resolves it.
