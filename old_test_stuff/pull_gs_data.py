import httplib2
import os

from apiclient import discovery
from google.oauth2 import service_account

from google.cloud import bigquery

import pandas as pd

import gspread

class pull_gs_data:

    def __init__(self):
        scopes = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/spreadsheets"]
        secret_file = os.path.join(os.getcwd(), 'client_secrets.json')

        credentials = service_account.Credentials.from_service_account_file(secret_file, scopes=scopes)
        self.gc = gspread.authorize(credentials)
        # self.gc = gspread.service_account(filename=secret_file)
        self.service = discovery.build('sheets', 'v4', credentials=credentials)
    
    def pull_data(self,spreadsheet_id):
        sheet = self.service.spreadsheets()
        result = sheet.values().get(spreadsheetId=spreadsheet_id,range='Sheet1!A:C').execute()
        values = result.get('values', [])
        df = pd.DataFrame(columns=values[0],data=values[1:])

        #convert inventory quantity to int
        if 'inventory_quantity' in df.columns:
            df['inventory_quantity'] = df['inventory_quantity'].astype('int')
        return(df)
    
    def upload_data(self,spreadsheet_id,sheet,df):
        sh = self.gc.open_by_key(spreadsheet_id)
        worksheet = sh.worksheet(sheet)

        worksheet.update([df.columns.values.tolist()] + df.values.tolist())



