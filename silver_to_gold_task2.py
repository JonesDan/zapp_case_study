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

# sql query to create a view in the gold dataset

sql = """
SELECT 
sku,
warehouse,
product_id,
inventory_quantity,
update_time,
prev_quantity,
prev_update_time,
out_of_stock_flag, 
product_delta,
restock_flag
 FROM `zapp-case-study.silver_warehouse.warehouse_product_task2` wpo
"""

# This view will live in the gold Dataset
dataset = 'gold_warehouse'

# Function to create Dataset (if dataset exists this wont do anything)
f.create_dataset(client,dataset)

# Define silver schema view name
view_id = 'warehouse_product_task2'

# Function to create view (if view exists this wont do anything)
f.create_view(client,dataset,view_id,sql)