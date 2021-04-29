import httplib2
import os
from apiclient import discovery
from google.oauth2 import service_account
import functions as f
from google.cloud import bigquery
import pandas as pd

# get credentials using the client_secrets.json which contains my service account details
scopes = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/cloud-platform"]
secret_file = os.path.join(os.getcwd(), 'client_secrets.json')

# get credentials using the client_secrets.json which contains my service account details
# I'm using service to pull data from google sheets and the client to uplaod the dataframe
credentials = service_account.Credentials.from_service_account_file(secret_file, scopes=scopes)
service = discovery.build('sheets', 'v4', credentials=credentials)
client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

# ID for google sheets
gsheet_product = '1qNM_au_sVAbN_U4VNzs1007urb5kv2ojFgQGcFehpPs'

# This table will live in the bronze Dataset
dataset = 'bronze_warehouse'

# Define table name
table_id_product = 'warehouse_product_task2'

# upload the data 3 times to resemble 5 minute updates
# function to pull data from google sheets
df_product = f.pull_gs_data(service,spreadsheet_id=gsheet_product)
for x in range(3):
    # function to upload dataframe into Query. This will append the dataframe to the bronze table
    f.upload_dataframe(client,dataset,table_id_product,df_product,truncate=False)

    # Change inventory quantity so that its different for next upload
    df_product['inventory_quantity'] = df_product['inventory_quantity'].apply(lambda x: x - 4 if x - 4 > 0 else x + 10)
    # Change update time to that its 5 minutes into the future
    df_product['update_time'] = df_product['update_time']+pd.Timedelta(minutes=5)