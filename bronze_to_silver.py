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

# sql query to be scheduled once a month
# the sql joins the warehouse and product tables on the warehouse_id
# to get the warehouse id from the product table i had to take it from the sku using the split function

sql = """
SELECT 
wo.warehouse_name,
wo.warehouse_id,
wo.country,
split(wp.sku,'-')[OFFSET(1)] as product_id,
wp.inventory_quantity,
date_trunc(wo.update_time,MONTH) as reporting_month
 FROM `zapp-case-study.bronze_warehouse.warehouse_overview` wo
 join `zapp-case-study.bronze_warehouse.warehouse_product` wp on wo.warehouse_id = split(wp.sku,'-')[OFFSET(0)]
"""

# This table will live in the silver Dataset
dataset = 'silver_warehouse'
# Function to create Dataset (if dataset exists this wont do anything)
f.create_dataset(client,dataset)
# Define silver schema table name
table_id = 'warehouse_product_overview'
# Function tp set up scheduled query
f.scheduled_query(client_transfer,dataset,table_id,sql,schedule="1 of month 00:00",name='Monthly Report')