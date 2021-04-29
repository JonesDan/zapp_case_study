from google.cloud import bigquery,bigquery_datatransfer
from google.oauth2 import service_account
import datetime
import functions as f
import os

# get credentials using the client_secrets.json which contains my service account details
credentials = service_account.Credentials.from_service_account_file(
    'client_secrets.json', scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
client = bigquery.Client(credentials=credentials, project=credentials.project_id,)
client_transfer = bigquery_datatransfer.DataTransferServiceClient(credentials=credentials)


# sql query to be scheduled every 15 minutes
# first CTE take data from the product table and splits out warehouse and product_id from sku
# the lag function is used to get the previous stock quantity so I can generate a restock flag

sql = """
with product_stream as (
SELECT 
sku,
split(sku,'-')[OFFSET(0)] as warehouse,
split(sku,'-')[OFFSET(1)] as product_id,
inventory_quantity,
update_time,
lag(inventory_quantity) OVER (PARTITION BY sku ORDER BY update_time) as prev_quantity,
lag(update_time) OVER (PARTITION BY sku ORDER BY update_time) as prev_update_time
 FROM `zapp-case-study.bronze_warehouse.warehouse_product_task2`)
 select 
sku,
warehouse,
product_id,
inventory_quantity,
update_time,
prev_quantity,
prev_update_time,
case when inventory_quantity = 0 then True else False end as out_of_stock_flag, 
case when prev_quantity is null then 0 else inventory_quantity - prev_quantity end as product_delta,
case when inventory_quantity - prev_quantity > 0 then True else False end as restock_flag
from product_stream
"""

# This table will live in the silver Dataset
dataset = 'silver_warehouse'

# Function to create Dataset (if dataset exists this wont do anything)
f.create_dataset(client,dataset)

# Define silver schema table name
table_id = 'warehouse_product_overview_task2'

# Function tp set up scheduled query
f.scheduled_query(client_transfer,dataset,table_id,sql,schedule="every 15 minutes",name='15 min update of warehouse inventory')