from google.cloud import bigquery,bigquery_datatransfer
from google.oauth2 import service_account
from datetime import datetime

class build_datasets:

    def __init__(self):

        credentials = service_account.Credentials.from_service_account_file(
    'client_secrets.json', scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )

        self.client = bigquery.Client(credentials=credentials, project=credentials.project_id,)


    def create_dataset(self,dataset_id):

        # TODO(developer): Set dataset_id to the ID of the dataset to create.
        dataset_id = f"{self.client.project}.{dataset_id}"

        # Construct a full Dataset object to send to the API.
        dataset = bigquery.Dataset(dataset_id)

        # TODO(developer): Specify the geographic location where the dataset should reside.
        dataset.location = "EU"

        # Send the dataset to the API for creation, with an explicit timeout.
        # Raises google.api_core.exceptions.Conflict if the Dataset already
        # exists within the project.
        try:
            dataset = self.client.create_dataset(dataset, timeout=30)  # Make an API request.
            print("Created dataset {}.{}".format(self.client.project, dataset.dataset_id))
        except Exception as e:
            print(e)
            pass
    
    def create_table(self,dataset_id,table_id,schema):

        # TODO(developer): Set table_id to the ID of the table to create.
        table_id = f"{self.client.project}.{dataset_id}.{table_id}"

        table = bigquery.Table(table_id, schema=schema)
        try:
            table = self.client.create_table(table)  # Make an API request.
            print(
                "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
            )
        except Exception as e:
            print(e)
            pass
    
    def truncate_table(self,dataset_id,table_id):

        table_id = f"{self.client.project}.{dataset_id}.{table_id}"

        # Construct a BigQuery client object.

        # TODO(developer): Set table_id to the ID of table to append to.
        # table_id = "your-project.your_dataset.your_table"

        errors = self.client.insert_rows_json(
            table_id, data, row_ids=[None] * len(data)
        )  # Make an API request.
        if errors == []:
            print("New rows have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))
    
    def upload_dataframe(self,dataset_id,table_id,df,truncate):

        table_id = f"{self.client.project}.{dataset_id}.{table_id}"

        if 'update_time' not in df.columns:
            df['update_time'] = datetime.datetime.strftime(datetime.now(),'%Y-%m-%dT%H:%M:%S')

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

        job = self.client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )  # Make an API request.
        job.result()  # Wait for the job to complete.

        table = self.client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
    
    def stream_data(self,dataset_id,table_id,df):

        table_id = f"{self.client.project}.{dataset_id}.{table_id}"

        # Construct a BigQuery client object.

        # TODO(developer): Set table_id to the ID of table to append to.
        # table_id = "your-project.your_dataset.your_table"

        data = df.to_json('records')
        errors = self.client.insert_rows_json(
            table_id, data, row_ids=[None] * len(data)
        )  # Make an API request.
        if errors == []:
            print("New rows have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))
    
    def create_table_from_query(self,dataset_id,table_id,sql):
        # TODO(developer): Set table_id to the ID of the destination table.
        # table_id = "your-project.your_dataset.your_table_name"

        table_id = f"{self.client.project}.{dataset_id}.{table_id}"

        job_config = bigquery.QueryJobConfig(destination=table_id)

        # Start the query, passing in the extra configuration.
        query_job = self.client.query(sql, job_config=job_config)  # Make an API request.
        query_job.result()  # Wait for the job to complete.

        print("Query results loaded to the table {}".format(table_id))
    
    def scheduled_query(self):

        transfer_client = bigquery_datatransfer.DataTransferServiceClient()

        # The project where the query job runs is the same as the project
        # containing the destination dataset.
        project_id = "your-project-id"
        dataset_id = "your_dataset_id"

        # This service account will be used to execute the scheduled queries. Omit
        # this request parameter to run the query as the user with the credentials
        # associated with this client.
        service_account_name = "abcdef-test-sa@abcdef-test.iam.gserviceaccount.com"

        # Use standard SQL syntax for the query.
        query_string = """
        SELECT
        CURRENT_TIMESTAMP() as current_time,
        @run_time as intended_run_time,
        @run_date as intended_run_date,
        17 as some_integer
        """

        parent = transfer_client.common_project_path(project_id)

        transfer_config = bigquery_datatransfer.TransferConfig(
            destination_dataset_id=dataset_id,
            display_name="Your Scheduled Query Name",
            data_source_id="scheduled_query",
            params={
                "query": query_string,
                "destination_table_name_template": "your_table_{run_date}",
                "write_disposition": "WRITE_TRUNCATE",
                "partitioning_field": "",
            },
            schedule="every 24 hours",
        )

        transfer_config = transfer_client.create_transfer_config(
            bigquery_datatransfer.CreateTransferConfigRequest(
                parent=parent,
                transfer_config=transfer_config,
                service_account_name=service_account_name,
            )
        )

        print("Created scheduled query '{}'".format(transfer_config.name))
