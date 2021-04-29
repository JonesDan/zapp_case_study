
from google.cloud import bigquery,bigquery_datatransfer

import httplib2
import os
from apiclient import discovery
from google.oauth2 import service_account

import pandas as pd
import gspread

import datetime

def create_dataset(client,dataset_id):

    # TODO(developer): Set dataset_id to the ID of the dataset to create.
    dataset_id = f"{client.project}.{dataset_id}"

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)

    # TODO(developer): Specify the geographic location where the dataset should reside.
    dataset.location = "EU"

    # Send the dataset to the API for creation, with an explicit timeout.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.
    try:
        dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
        print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
    except Exception as e:
        print(e)
        pass

def create_table(client,dataset_id,table_id,schema):

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = f"{client.project}.{dataset_id}.{table_id}"

    table = bigquery.Table(table_id, schema=schema)
    try:
        table = client.create_table(table)  # Make an API request.
        print(
            "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )
    except Exception as e:
        print(e)
        pass

def pull_gs_data(service,spreadsheet_id):
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=spreadsheet_id,range='Sheet1!A:C').execute()
    values = result.get('values', [])
    df = pd.DataFrame(columns=values[0],data=values[1:])
    df['update_time'] = datetime.datetime.now()

    #convert inventory quantity to int
    if 'inventory_quantity' in df.columns:
        df['inventory_quantity'] = df['inventory_quantity'].astype('int')

    return(df)

def upload_dataframe(client,dataset_id,table_id,df,truncate):

    table_id = f"{client.project}.{dataset_id}.{table_id}"

    # add the update_time schema in the definition to make sure the column is defined as a timestamp
    
    job_config = bigquery.LoadJobConfig(
        # Optionally, set the write disposition. BigQuery appends loaded rows
        # to an existing table by default, but with WRITE_TRUNCATE write
        # disposition it replaces the table with the loaded data.
        write_disposition= "WRITE_TRUNCATE" if truncate else "WRITE_APPEND",
        schema=[
            # Specify the type of columns whose type cannot be auto-detected. For
            # example the "title" column uses pandas dtype "object", so its
            # data type is ambiguous.
            bigquery.SchemaField("update_time", bigquery.enums.SqlTypeNames.TIMESTAMP),],
    )

    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

def scheduled_query(transfer_client,dataset_id,table_id,query_string,schedule,name):

    # The project where the query job runs is the same as the project
    # containing the destination dataset.

    parent = transfer_client.common_project_path('zapp-case-study')

    transfer_config = bigquery_datatransfer.TransferConfig(
        destination_dataset_id=dataset_id,
        display_name=name,
        data_source_id="scheduled_query",
        params={
            "query": query_string,
            "destination_table_name_template":table_id,
            "write_disposition": "WRITE_APPEND",
            "partitioning_field": "",
        },
        schedule=schedule)

    transfer_config = transfer_client.create_transfer_config(
        bigquery_datatransfer.CreateTransferConfigRequest(
            parent=parent,
            transfer_config=transfer_config,
        )
    )

    print("Created scheduled query '{}'".format(transfer_config.name))

def create_view(client,dataset_id,view_id,query_string):

    view_id = f"zapp-case-study.{dataset_id}.{view_id}"
    view = bigquery.Table(view_id)

    # The source table in this example is created from a CSV file in Google
    # Cloud Storage located at
    # `gs://cloud-samples-data/bigquery/us-states/us-states.csv`. It contains
    # 50 US states, while the view returns only those states with names
    # starting with the letter 'W'.
    view.view_query = query_string

    # Make an API request to create the view.
    view = client.create_table(view)
    print(f"Created {view.table_type}: {str(view.reference)}")