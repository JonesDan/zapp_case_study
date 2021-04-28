from google.cloud import bigquery,bigquery_datatransfer
from google.oauth2 import service_account
import datetime
import functions as f
import os

credentials = service_account.Credentials.from_service_account_file(
    'client_secrets.json', scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
client = bigquery.Client(credentials=credentials, project=credentials.project_id,)
client_transfer = bigquery_datatransfer.DataTransferServiceClient(credentials=credentials)

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

dataset = 'silver_warehouse'
f.create_dataset(client,dataset)

table_id = 'warehouse_product_overview'

f.scheduled_query(client_transfer,dataset,table_id,sql,schedule="1 of month 00:00",name='Monthly Report')