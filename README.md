# de_task
**Scripts for ETL and data pipiline**

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
  

**Steps to run**
1. Copy all the scripts in dag folder
2. Pip Install pymongo, pyodbc,pyodbc sql server driver, pandas
3. Trigger etl_dag from airflow web UI.

Note

a). Sometimes Sql server does not get connected. If it happens, rerunning script resolves it. 
b).**Data is already inserted in Source db. Running the pipeline extracts , tansforms and loads the data in Mongodb.**
c). To check data in source db, use connection code from data_extract script.
   To check data in target db, use connection code from insert_data script.
   
 - Setup instructions for the SQL and NoSQL databases.- No need for setting up, connection to cloud hosted SQL and NoSQL db is 
    established in scripts
 - Instructions on how to run the pipeline.- Manual trigger to run instantly. Once active, pipeline is scheduled to run every 3 hrs.
 - Explanations of the data structures used and their time complexities (Big O notations).
   Pandas df and dictionary are used for transformation and insertion in Mongodb.
