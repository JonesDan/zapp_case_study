from google.cloud import bigquery,bigquery_datatransfer
from google.oauth2 import service_account
from datetime import datetime
import functions as f

# get credentials using the client_secrets.json which contains my service account details
credentials = service_account.Credentials.from_service_account_file(
    'client_secrets.json', scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )

client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

# The initial tables will live in the bronze Dataset
dataset = 'bronze_warehouse'

# Define table name and schema
table_id_product = 'warehouse_product_task2'
product_schema = [
bigquery.SchemaField("sku", bigquery.enums.SqlTypeNames.STRING),
bigquery.SchemaField("inventory_quantity", bigquery.enums.SqlTypeNames.INTEGER),
bigquery.SchemaField("update_time", bigquery.enums.SqlTypeNames.TIMESTAMP),
]

# Function to create Dataset (if dataset exists this wont do anything)
f.create_dataset(client,dataset)
# Function to create Table (if Table exists this wont do anything)
f.create_table(client,dataset,table_id_product,product_schema)