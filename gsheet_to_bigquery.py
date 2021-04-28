import httplib2
import os
from apiclient import discovery
from google.oauth2 import service_account
import functions as f
from google.cloud import bigquery


scopes = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/cloud-platform"]
secret_file = os.path.join(os.getcwd(), 'client_secrets.json')

credentials = service_account.Credentials.from_service_account_file(secret_file, scopes=scopes)
service = discovery.build('sheets', 'v4', credentials=credentials)
client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

gsheet_warehouse_overview = '1IVj1yahZm_TbnD0RD8slEoNChhQnd7k25pSRqu9wGoA'
gsheet_product = '1qNM_au_sVAbN_U4VNzs1007urb5kv2ojFgQGcFehpPs'

dataset = 'bronze_warehouse'

table_id_warehouse = 'warehouse_overview'
table_id_product = 'warehouse_product'

df_warehouse_overview = f.pull_gs_data(service,spreadsheet_id=gsheet_warehouse_overview)
df_product = f.pull_gs_data(service,spreadsheet_id=gsheet_product)

f.upload_dataframe(client,dataset,table_id_warehouse,df_warehouse_overview,truncate=True)
f.upload_dataframe(client,dataset,table_id_product,df_product,truncate=True)

