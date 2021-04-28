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
warehouse_name,
warehouse_id,
country,
product_id,
inventory_quantity,
reporting_month
 FROM `zapp-case-study.silver_warehouse.warehouse_product_overview` wpo
"""

dataset = 'gold_warehouse'
f.create_dataset(client,dataset)

view_id = 'warehouse_product_overview'

f.create_view(client,dataset,view_id,sql)