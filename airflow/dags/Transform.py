import os
import pandas as pd
from pathlib import path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from airflow.operators.python import PythonOperator


# Environment variables exposed in airflow by Terraform
PROJECT_ID = os.environ.get('PROJECT_ID')
BUCKET = os.environ.get('GCP_GSC_BUCKET')
BQ_STAGING_DATASET = os.environ.get("BIGQUERY_DATASET_1")
BQ_WAREHOUSE_DATASET = os.environ.get("BIGQUERY_DATASET_2")
GCP_DATA_SOURCE_PATH = 'sbti_de_data_lake_sbti-438203/parquet_files'


# Define paths to each of the CSV files on the Airflow directory

deals_csv_file = 'data/deals.csv'
accounts_csv_file ='data/sales_accounts.csv'
contacts_csv_file = 'data/contacts.csv'


# Define paths to each of the files in GCS
deals_parquet_file = f'{GCP_DATA_SOURCE_PATH}/deals.parquet'
accounts_parquet_file = f'{GCP_DATA_SOURCE_PATH}/sales_accounts.parquet'
contacts_parquet_file = f'{GCP_DATA_SOURCE_PATH}/contacts.parquet'

# Transformation functions
def parse_csv_file(filepath: str, **kwargs) -> tuple:
    """
    Read extracted CSV file and return both the DataFrame and the file name.
    """
    file_name = path(filepath).name
    df = pd.read_csv(filepath)
    return df, file_name

def handle_missing_values_and_duplicates(**kwargs) -> pd.DataFrame:
    """
    Handle missing values and duplicate rows in the DataFrame based on column data types.

    Args:
        kwargs: Task instance context to pull XComs.

    Returns:
        DataFrame: The processed DataFrame with missing values handled and duplicates removed.
    """
    ti = kwargs['ti']
    parsed_data = ti.xcom_pull(task_ids=f'parse_{file_name}_csv')

    df, file_name = parsed_data


    # Iterate through each column to fill missing values based on data type
    for column in df.columns:
        if df[column].dtype == 'object':
            # For object (string) columns, fill missing values with 'NA'
            df[column] = df[column].fillna('NA')

        elif pd.api.types.is_numeric_dtype(df[column]):
            # For numeric columns, fill missing values with 0
            df[column] = df[column].fillna(0)

        elif pd.api.types.is_bool_dtype(df[column]):   
            # For boolean columns, fill missing values with False
            df[column] = df[column].fillna(False) 

        elif pd.api.types.is_datetime64_any_dtype(df[column]):
            # For datetime columns, fill missing values with a specific date: 1970-01-01 00:00:00 UTC
            df[column] = df[column].fillna(pd.Timestamp('1970-01-01 00:00:00 UTC'))

    # Remove duplicate rows
    df = df.drop_duplicates()

    return df

def change_schema_and_save(df: pd.DataFrame, schema_mapping: dict) -> pd.DataFrame:
    """
    Change the schema of the DataFrame based on the provided mapping.

    Args:
        df (DataFrame): The DataFrame to transform.
        schema_mapping (dict): The schema mapping to apply.

    Returns:
        DataFrame: The transformed DataFrame.
    """

    for column, dtype in schema_mapping.items():
        if dtype == 'datetime':
            df[column] = pd.to_datetime(df[column], errors='coerce')  # Convert to datetime
        else:
            # Check if the dtype is an integer type
            if pd.api.types.is_integer_dtype(dtype):

                # Fill NaN values with 0 before converting
                df[column] = df[column].fillna(0)

            # Cast columns to the correct type
            df[column] = df[column].astype(dtype, errors='ignore')
            
    return df

def convert_csv_to_parquet_and_upload(directory: str, gcs_bucket_name: str):
    """
    Convert CSV files in the local directory to Parquet format, change thier schema and upload to Google Cloud Storage.

    Args:
        directory (str): The local directory containing CSV files.
        gcs_bucket_name (str): The name of the GCS bucket to upload to.
        schema_mapping_by_file (dict): A mapping of filenames to their respective schema transformations.
    """
    from google.cloud import storage
    from google.oauth2 import service_account
    # Declare function GCP credentials
    credentials = service_account.Credentials.from_service_account_file(
        ".google/sbti-service-cred.json"
    )

    # Initialize the GCS client
    client = storage.Client(credentials=credentials)
    bucket = client.bucket(gcs_bucket_name)

    # Create a subdirectory for Parquet files if it doesn't exist
    parquet_directory = os.path.join(directory, 'parquet')
    os.makedirs(parquet_directory, exist_ok=True)

    # Loop through all CSV files in the local directory
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            csv_file_path = os.path.join(directory, filename)
            parquet_file_path = os.path.join(parquet_directory, filename.replace('.csv', '.parquet'))

            # Determine which schema to apply based on the filename
            if filename in schema_mapping_by_file:
                schema_mapping = schema_mapping_by_file[filename]

                # Read the CSV file into a DataFrame
                df = parse_csv_file(directory, filename)

                # Apply the schema transformations
                df = change_schema_and_save(df, schema_mapping)

                # Handle missing values and duplicates
                df = handle_missing_values_and_duplicates(df)

                # Convert DataFrame to Parquet format and save it locally
                df.to_parquet(parquet_file_path, index=False)

                # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
                storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
                storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
                # End of Workaround

                # Upload the Parquet file to GCS
                blob = bucket.blob(f"parquet_files/{os.path.basename(parquet_file_path)}")
                blob.upload_from_filename(parquet_file_path)

                print(f"Uploaded {parquet_file_path} to gs://{gcs_bucket_name}/parquet_files/{os.path.basename(parquet_file_path)}")

# Schema mapping: field names and types for Parquet schema formatting
schema_mapping = {
     'contacts.csv': {
         'id': 'int',
         'first_name': 'str',
         'last_name': 'str',
         'display_name': 'str',
         'avatar': 'str',  # Assuming this can be a URL or string
         'job_title': 'str',
         'city': 'str',
         'state': 'str',
         'zipcode': 'str',  # Zip codes can have leading zeroes, best to store as 'str'ings
         'country': 'str',
         'email': 'str',  # Email as string
         'emails': 'object',  # Nested structure, would need to parse this for BigQuery as JSON
         'time_zone': 'str',
         'work_number': 'str',  # Storing phone numbers as 'str'ings
         'mobile_number': 'str',
         'address': 'str',
         'last_seen': 'datetime',  # datetime format
         'lead_score': 'float',
         'last_contacted': 'datetime',
         'open_deals_amount': 'float',
         'won_deals_amount': 'float',
         'last_contacted_sales_activity_mode': 'str',
         'created_at': 'datetime',  # datetime format
         'updated_at': 'datetime',
         'keyword': 'str',
         'medium': 'str',
         'last_contacted_mode': 'str',
         'recent_note': 'str',
         'won_deals_count': 'int',
         'last_contacted_via_sales_activity': 'str',
         'completed_sales_sequences': 'int',
         'active_sales_sequences': 'int',
         'web_form_ids': 'str',  # Assuming this is a comma-separated string
         'open_deals_count': 'int',
         'last_assigned_at': 'datetime',
         'facebook': 'str',  # URLs assumed to be strings
         'twitter': 'str',
         'linkedin': 'str',
         'is_deleted': bool,  # boolean flag
         'team_user_ids': 'object',  # JSON or 'list' of IDs
         'external_id': 'str',
         'work_email': 'str',
         'subscription_status': 'int',  # Assuming it's an enum or flag
         'subscription_types': 'str',  # Assuming it's a string of types
         'unsubscription_reason': 'str',
         'other_unsubscription_reason': 'str',
         'customer_fit': 'int',
         'record_type_id': 'int',
         'whatsapp_subscription_status': 'int',
         'sms_subscription_status': 'int',
         'last_seen_chat': 'datetime',
         'first_seen_chat': 'datetime',
         'locale': 'str',
         'total_sessions': 'int',
         'system_tags': 'object',  # JSON or 'list'
         'first_campaign': 'str',
         'first_medium': 'str',
         'first_source': 'str',
         'last_campaign': 'str',
         'last_medium': 'str',
         'last_source': 'str',
         'latest_campaign': 'str',
         'latest_medium': 'str',
         'latest_source': 'str',
         'mcr_id': 'int',
         'description': 'str',
         'phone_numbers': 'str',  # JSON or 'list'
         'tags': 'str',  # JSON or 'list'
         'links_conversations': 'str',  # Assuming URLs as 'str'ings
         'links_timeline_feeds': 'str',
         'links_document_associations': 'str',
         'links_notes': 'str',
         'links_tasks': 'str',
         'links_appointments': 'str',
         'links_reminders': 'str',
         'links_duplicates': 'str',
         'links_connections': 'str',
         'custom_field_cf_lead_source': 'str',
         'custom_field_cf_last_contacted': 'datetime'  # 'datetime' field
     },
    
    
     'deals.csv': {
         "id": 'str',
         "name": 'str',
         "amount": 'float',
         "base_currency_amount": 'float',
         "expected_close": 'datetime',  # Using pandas 'datetime' format
         "closed_date": 'datetime',
         "stage_updated_time": 'datetime',
         "probability": 'float',
         "updated_at": 'datetime',
         "created_at": 'datetime',
         "deal_pipeline_id": 'str',
         "deal_stage_id": 'str',
         "age": 'int',
         "recent_note": 'str',
         "completed_sales_sequences": 'str',
         "active_sales_sequences": 'str',
         "web_form_id": 'str',
         "upcoming_activities_time": 'datetime',
         "last_assigned_at": 'datetime',
         "last_contacted_sales_activity_mode": 'str',
         "last_contacted_via_sales_activity": 'str',
         "expected_deal_value": 'float',
         "is_deleted": bool,
         "team_user_ids": 'object',  # Assuming this field is a 'list'
         "avatar": 'str',
         "forecast_category": 'str',
         "deal_prediction": 'float',
         "deal_prediction_last_updated_at": 'datetime',
         "record_type_id": 'str',
         "freddy_forecast_metrics": 'str',
         "last_deal_prediction": 'str',
         "has_products": bool,
         "products": 'object',
         "deal_price_adjustments": 'str',
         "rotten_days": 'int',
         "tags": 'object',
         "custom_field_cf_deal_stage": 'str',
         "custom_field_cf_account_id": 'int',
         "custom_field_cf_deal_id": 'int',
         "custom_field_cf_deal_size": 'float',
         "links_conversations": 'str',
         "links_document_associations": 'str',
         "links_notes": 'str',
         "links_tasks": 'str',
         "links_appointments": 'str',
         "fc_widget_collaboration_convo_token": 'str',
         "fc_widget_collaboration_auth_token": 'str',
         "fc_widget_collaboration_encoded_jwt_token": 'str'
     },
    
    
    
    'sales_accounts.csv': {
        "id": 'int',
        "name": 'str',
        "address": 'str',
        "city": 'str',
        "state": 'str',
        "zipcode": 'str',
        "country": 'str',
        "number_of_employees": 'int',
        "annual_revenue": 'float',
        "website": 'str',
        "owner_id": 'int',
        "phone": 'str',
        "open_deals_amount": 'float',
        "open_deals_count": 'int',
        "won_deals_amount": 'float',
        "won_deals_count": 'int',
        "last_contacted": 'datetime',  # Can be parsed to 'datetime'
        "last_contacted_mode": 'str',
        "facebook": 'str',
        "twitter": 'str',
        "linkedin": 'str',
        "created_at": 'datetime',  # Can be parsed to 'datetime'
        "updated_at": 'datetime',  # Can be parsed to 'datetime'
        "avatar": 'str',
        "parent_sales_account_id": 'int',
        "recent_note": 'str',
        "last_contacted_via_sales_activity": 'str',
        "last_contacted_sales_activity_mode": 'datetime',
        "completed_sales_sequences": 'object',
        "active_sales_sequences": 'object',
        "last_assigned_at": 'datetime',  # Can be parsed to 'datetime'
        "is_deleted": bool,
        "team_user_ids": 'object',
        "record_type_id": 'int',
        "web_form_ids": 'object',
        "description": 'str',
        "note": 'str',
        "health_score": 'float',
        "account_tier": 'str',
        "renewal_date": 'datetime',  # Can be parsed to 'datetime'
        "domains": 'object',
        "tags": 'object',
        "links_conversations": 'str',
        "links_document_associations": 'str',
        "links_notes": 'str',
        "links_tasks": 'str',
        "links_appointments": 'str',
        "custom_field_cf_industry": 'str',
        "custom_field_cf_account_value": 'float',
        "custom_field_cf_region": 'str',
        "custom_field_cf_account_id": 'int'
    }
}



default_args = {
    'owner': 'SBTi',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 8, 10, 00),
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id='sbti_transformation_dag',
    default_args=default_args,
    schedule= '',
    catchup=False) as dag:

    """
    #### PARSE CSV FILES
    """
    parse_deals_csv = PythonOperator(
        task_id='parse_deals_csv',
        python_callable = parse_csv_file(deals_csv_file),
        op_kwargs={'filepath': 'data/deals.csv'},
        provide_context=True,
        dag=dag
    )

    parse_contacts_csv = PythonOperator(
        task_id='parse_contacts_csv',
        python_callable = parse_csv_file(contacts_csv_file),
        op_kwargs={'filepath':'data/contacts.csv'},
        provide_context=True,
        dag=dag
    )

    parse_accounts_csv = PythonOperator(
        task_id='parse_sales_accounts_csv',
        python_callable = parse_csv_file(accounts_csv_file),
        op_kwargs={'filepath':'data/sales_accounts.csv'},
        provide_context=True,
        dag=dag
    )


    """
    #### DEAL WITH MISSING VALUES AND DUPLICATES
    """
    deals_clean_task = PythonOperator(
        task_id='deals_missing_and_duplicates',
        python_callable = handle_missing_values_and_duplicates(),
        provided_context=True,
        dag=dag
    )

    contacts_clean_task = PythonOperator(
        task_id='contacts_missing_and_duplicates',
        python_callable = handle_missing_values_and_duplicates(),
        provided_context=True,
        dag=dag
    )

    accounts_clean_task = PythonOperator(
        task_id='accounts_missing_and_duplicates',
        python_callable = handle_missing_values_and_duplicates(),
        provided_context=True,
        dag=dag
    )


    """
    #### CHANGE SCHEMA TO FIT BQ
    """
    deals_schema_change = PythonOperator(
        task_id='deals_schema_change',
        python_callable= change_schema_and_save(),
        provided_context=True,
        dag=dag
    )

    contacts_schema_change = PythonOperator(
        task_id='contacts_schema_change',
        python_callable= change_schema_and_save(),
        provided_context=True,
        dag=dag
    )

    accounts_schema_change = PythonOperator(
        task_id='accounts_schema_change',
        python_callable= change_schema_and_save(),
        provided_context=True,
        dag=dag
    )





    """
    #### CONVERT TO PARQUET AND UPLOAD TO GCS
    """
    deals_parquet_upload = PythonOperator()

    contacts_parquet_upload = PythonOperator()

    accounts_parquet_upload = PythonOperator()

    """
    #### CREATE 3 EMPTY TABLES IN THE STAGING LAYER
    """

    # Task to create a BigQuery table in the staging dataset for the deals data
    create_deals_staging_table = BigQueryCreateEmptyTableOperator(
        task_id='create_deals_staging_table',
        dataset_id=f'{PROJECT_ID}.{BQ_STAGING_DATASET}',
        table_id='deals_staging',
        schema=[{"name": "id", "type": "INTEGER", "mode": "REQUIRED"}, 
                {"name": "name", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "amount", "type": "FLOAT", "mode": "NULLABLE"}, 
                {"name": "base_currency_amount", "type": "FLOAT", "mode": "NULLABLE"}, 
                {"name": "expected_close", "type": "DATE", "mode": "NULLABLE"}, 
                {"name": "closed_date", "type": "DATE", "mode": "NULLABLE"}, 
                {"name": "stage_updated_time", "type": "TIMESTAMP", "mode": "NULLABLE"}, 
                {"name": "probability", "type": "INTEGER", "mode": "NULLABLE"}, 
                {"name": "updated_at", "type": "TIMESTAMP", "mode": "NULLABLE"}, 
                {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"}, 
                {"name": "deal_pipeline_id", "type": "INTEGER", "mode": "NULLABLE"}, 
                {"name": "deal_stage_id", "type": "INTEGER", "mode": "NULLABLE"}, 
                {"name": "age", "type": "INTEGER", "mode": "NULLABLE"}, 
                {"name": "recent_note", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "completed_sales_sequences", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "active_sales_sequences", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "web_form_id", "type": "INTEGER", "mode": "NULLABLE"}, 
                {"name": "upcoming_activities_time", "type": "TIMESTAMP", "mode": "NULLABLE"}, 
                {"name": "last_assigned_at", "type": "TIMESTAMP", "mode": "NULLABLE"}, 
                {"name": "last_contacted_sales_activity_mode", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "last_contacted_via_sales_activity", "type": "TIMESTAMP", "mode": "NULLABLE"}, 
                {"name": "expected_deal_value", "type": "FLOAT", "mode": "NULLABLE"}, 
                {"name": "is_deleted", "type": "BOOLEAN", "mode": "NULLABLE"}, 
                {"name": "team_user_ids", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "avatar", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "forecast_category", "type": "INTEGER", "mode": "NULLABLE"}, 
                {"name": "deal_prediction", "type": "INTEGER", "mode": "NULLABLE"}, 
                {"name": "deal_prediction_last_updated_at", "type": "TIMESTAMP", "mode": "NULLABLE"}, 
                {"name": "record_type_id", "type": "INTEGER", "mode": "NULLABLE"}, 
                {"name": "freddy_forecast_metrics", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "last_deal_prediction", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "has_products", "type": "BOOLEAN", "mode": "NULLABLE"}, 
                {"name": "products", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "deal_price_adjustments", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "rotten_days", "type": "INTEGER", "mode": "NULLABLE"}, 
                {"name": "tags", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "custom_field_cf_deal_stage", "type": "FLOAT", "mode": "NULLABLE"}, 
                {"name": "custom_field_cf_account_id", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "custom_field_cf_deal_id", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "custom_field_cf_deal_size", "type": "INTEGER", "mode": "NULLABLE"}, 
                {"name": "links_conversations", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "links_document_associations", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "links_notes", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "links_tasks", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "links_appointments", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "fc_widget_collaboration_convo_token", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "fc_widget_collaboration_auth_token", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "fc_widget_collaboration_encoded_jwt_token", "type": "STRING", "mode": "NULLABLE"}],
    )

    # Task to create a BigQuery table in the staging dataset for the accounts data
    create_accounts_staging_table = BigQueryCreateEmptyTableOperator(
        task_id='create_accounts_staging_table',
        dataset_id=f'{PROJECT_ID}.{BQ_STAGING_DATASET}',
        table_id='accounts_staging',
        schema=[{"name": "id", "type": "INTEGER", "mode": "REQUIRED"}, 
                {"name": "name", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "address", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "city", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "state", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "zipcode", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "country", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "number_of_employees", "type": "INTEGER", "mode": "NULLABLE"}, 
                {"name": "annual_revenue", "type": "FLOAT", "mode": "NULLABLE"}, 
                {"name": "website", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "owner_id", "type": "INTEGER", "mode": "NULLABLE"}, 
                {"name": "phone", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "open_deals_amount", "type": "FLOAT", "mode": "NULLABLE"}, 
                {"name": "open_deals_count", "type": "INTEGER", "mode": "NULLABLE"}, 
                {"name": "won_deals_amount", "type": "FLOAT", "mode": "NULLABLE"}, 
                {"name": "won_deals_count", "type": "INTEGER", "mode": "NULLABLE"}, 
                {"name": "last_contacted", "type": "TIMESTAMP", "mode": "NULLABLE"}, 
                {"name": "last_contacted_mode", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "facebook", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "twitter", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "linkedin", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"}, 
                {"name": "updated_at", "type": "TIMESTAMP", "mode": "NULLABLE"}, 
                {"name": "avatar", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "parent_sales_account_id", "type": "INTEGER", "mode": "NULLABLE"}, 
                {"name": "recent_note", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "last_contacted_via_sales_activity", "type": "TIMESTAMP", "mode": "NULLABLE"}, 
                {"name": "last_contacted_sales_activity_mode", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "completed_sales_sequences", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "active_sales_sequences", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "last_assigned_at", "type": "TIMESTAMP", "mode": "NULLABLE"}, 
                {"name": "is_deleted", "type": "BOOLEAN", "mode": "NULLABLE"}, 
                {"name": "team_user_ids", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "record_type_id", "type": "INTEGER", "mode": "NULLABLE"}, 
                {"name": "web_form_ids", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "description", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "note", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "health_score", "type": "INTEGER", "mode": "NULLABLE"}, 
                {"name": "account_tier", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "renewal_date", "type": "DATE", "mode": "NULLABLE"}, 
                {"name": "domains", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "tags", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "links_conversations", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "links_document_associations", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "links_notes", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "links_tasks", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "links_appointments", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "custom_field_cf_industry", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "custom_field_cf_account_value", "type": "FLOAT", "mode": "NULLABLE"}, 
                {"name": "custom_field_cf_region", "type": "STRING", "mode": "NULLABLE"},
                {"name": "custom_field_cf_account_id", "type": "INTEGER", "mode": "NULLABLE"}],
    )   

    # Task to create a BigQuery table in the staging dataset for the contacts data
    create_contacts_staging_table = BigQueryCreateEmptyTableOperator(
        task_id='create_contacts_staging_table',
        dataset_id=f'{PROJECT_ID}.{BQ_STAGING_DATASET}',    
        table_id='contacts_staging',
        schema= [{"name": "id", "type": "INTEGER"},
                 {"name": "first_name", "type": "STRING"},
                 {"name": "last_name", "type": "STRING"},
                 {"name": "display_name", "type": "STRING"},
                 {"name": "avatar", "type": "STRING"},
                 {"name": "job_title", "type": "STRING"},
                 {"name": "city", "type": "STRING"},
                 {"name": "state", "type": "STRING"},
                 {"name": "zipcode", "type": "STRING"},
                 {"name": "country", "type": "STRING"},
                 {"name": "email", "type": "STRING"},
                 {"name": "emails", "type": "STRING"},
                 {"name": "time_zone", "type": "STRING"},
                 {"name": "work_number", "type": "STRING"},
                 {"name": "mobile_number", "type": "STRING"},
                 {"name": "address", "type": "STRING"},
                 {"name": "last_seen", "type": "TIMESTAMP"},
                 {"name": "lead_score", "type": "INTEGER"},
                 {"name": "last_contacted", "type": "TIMESTAMP"},
                 {"name": "open_deals_amount", "type": "FLOAT"},
                 {"name": "won_deals_amount", "type": "FLOAT"},
                 {"name": "last_contacted_sales_activity_mode", "type": "STRING"},
                 {"name": "created_at", "type": "TIMESTAMP"},
                 {"name": "updated_at", "type": "TIMESTAMP"},
                 {"name": "keyword", "type": "STRING"},
                 {"name": "medium", "type": "STRING"},
                 {"name": "last_contacted_mode", "type": "STRING"},
                 {"name": "recent_note", "type": "STRING"},
                 {"name": "won_deals_count", "type": "INTEGER"},
                 {"name": "last_contacted_via_sales_activity", "type": "STRING"},
                 {"name": "completed_sales_sequences", "type": "STRING"},
                 {"name": "active_sales_sequences", "type": "STRING"},
                 {"name": "web_form_ids", "type": "STRING"},
                 {"name": "open_deals_count", "type": "INTEGER"},
                 {"name": "last_assigned_at", "type": "TIMESTAMP"},
                 {"name": "facebook", "type": "STRING"},
                 {"name": "twitter", "type": "STRING"},
                 {"name": "linkedin", "type": "STRING"},
                 {"name": "is_deleted", "type": "BOOLEAN"},
                 {"name": "team_user_ids", "type": "STRING"},
                 {"name": "external_id", "type": "INTEGER"},
                 {"name": "work_email", "type": "STRING"},
                 {"name": "subscription_status", "type": "INTEGER"},
                 {"name": "subscription_types", "type": "STRING"},
                 {"name": "unsubscription_reason", "type": "STRING"},
                 {"name": "other_unsubscription_reason", "type": "STRING"},
                 {"name": "customer_fit", "type": "INTEGER"},
                 {"name": "record_type_id", "type": "INTEGER"},
                 {"name": "whatsapp_subscription_status", "type": "INTEGER"},
                 {"name": "sms_subscription_status", "type": "INTEGER"},
                 {"name": "last_seen_chat", "type": "TIMESTAMP"},
                 {"name": "first_seen_chat", "type": "TIMESTAMP"},
                 {"name": "locale", "type": "STRING"},
                 {"name": "total_sessions", "type": "INTEGER"},
                 {"name": "system_tags", "type": "STRING"},
                 {"name": "first_campaign", "type": "STRING"},
                 {"name": "first_medium", "type": "STRING"},
                 {"name": "first_source", "type": "STRING"},
                 {"name": "last_campaign", "type": "STRING"},
                 {"name": "last_medium", "type": "STRING"},
                 {"name": "last_source", "type": "STRING"},
                 {"name": "latest_campaign", "type": "STRING"},
                 {"name": "latest_medium", "type": "STRING"},
                 {"name": "latest_source", "type": "STRING"},
                 {"name": "mcr_id", "type": "STRING"},
                 {"name": "description", "type": "STRING"},
                 {"name": "phone_numbers", "type": "STRING"},
                 {"name": "tags", "type": "STRING"},
                 {"name": "links_conversations", "type": "STRING"},
                 {"name": "links_timeline_feeds", "type": "STRING"},
                 {"name": "links_document_associations", "type": "STRING"},
                 {"name": "links_notes", "type": "STRING"},
                 {"name": "links_tasks", "type": "STRING"},
                 {"name": "links_appointments", "type": "STRING"},
                 {"name": "links_reminders", "type": "STRING"},
                 {"name": "links_duplicates", "type": "STRING"},
                 {"name": "links_connections", "type": "STRING"},
                 {"name": "custom_field_cf_lead_source", "type": "STRING"},
                 {"name": "custom_field_cf_last_contacted", "type": "STRING"}],
    )

    
    
    """
    #### TRANSFER RAW DATA FILES FROM GCS TO THE STAGING LAYER OF THE BIGQUERY DATABASE FOR TRANSFORMATION 
    """

    # Tasks to transfer the data to BigQuery
    transfer_deals_to_bigquery = GCSToBigQueryOperator(
        task_id= 'transfer_deals_to_bigquery',
        bucket=BUCKET,
        source_objects=[deals_parquet_file],
        destination_project_dataset_table=f'{PROJECT_ID}.{BQ_STAGING_DATASET}.deals_staging',
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
    )

    transfer_contacts_to_bigquery = GCSToBigQueryOperator(
        task_id= 'transfer_contacts_to_bigquery',
        bucket=BUCKET,
        source_objects=[contacts_parquet_file],
        destination_project_dataset_table=f'{PROJECT_ID}.{BQ_STAGING_DATASET}.contacts_staging',
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
    )

    transfer_sales_to_bigquery = GCSToBigQueryOperator(
        task_id= 'transfer_accounts_to_bigquery',
        bucket=BUCKET,
        source_objects=[accounts_parquet_file],
        destination_project_dataset_table=f'{PROJECT_ID}.{BQ_STAGING_DATASET}.accounts_staging',
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
    )



    # Daily Aggregations per account
    daily_aggregations_per_account = BigQueryExecuteQueryOperator(
        task_id= 'daily_aggregations_per_account',
        sql= """

            WITH deal_aggregates AS (
                SELECT
                    a.id AS account_id,
                    DATE(d.created_at) AS deal_date,
                    COUNT(d.id) AS total_deals,
                    SUM(d.won_deals_amount) AS total_won_amount,
                    SUM(d.open_deals_amount) AS total_open_amount
                FROM
                    `sbti-438203.de_dataset_staging.deals` d
                JOIN
                    `sbti-438203.de_dataset_staging.accounts` a ON d.account_id = a.id
                GROUP BY
                    account_id, deal_date
            ),        
            contact_aggregates AS (
                SELECT
                    a.id AS account_id,
                    DATE(c.created_at) AS contact_date,
                    COUNT(c.id) AS total_contacts
                FROM
                    `sbti-438203.de_dataset_staging.contacts` c
                JOIN
                    `sbti-438203.de_dataset_staging.accounts` a ON c.account_id = a.id
                GROUP BY
                    account_id, contact_date
            ),        
            daily_aggregates AS (
                SELECT
                    da.account_id,
                    da.deal_date,
                    da.total_deals,
                    da.total_won_amount,
                    da.total_open_amount,
                    COALESCE(ca.total_contacts, 0) AS total_contacts
                FROM
                    deal_aggregates da
                LEFT JOIN
                    contact_aggregates ca ON da.account_id = ca.account_id AND da.deal_date = ca.contact_date
            ),        
            weekly_aggregates AS (
                SELECT
                    account_id,
                    DATE_TRUNC(deal_date, WEEK(MONDAY)) AS week_start,
                    SUM(total_deals) AS total_deals,
                    SUM(total_won_amount) AS total_won_amount,
                    SUM(total_open_amount) AS total_open_amount,
                    SUM(total_contacts) AS total_contacts
                FROM
                    daily_aggregates
                GROUP BY
                    account_id, week_start
            ),        
            monthly_aggregates AS (
                SELECT
                    account_id,
                    DATE_TRUNC(deal_date, MONTH) AS month_start,
                    SUM(total_deals) AS total_deals,
                    SUM(total_won_amount) AS total_won_amount,
                    SUM(total_open_amount) AS total_open_amount,
                    SUM(total_contacts) AS total_contacts
                FROM
                    daily_aggregates
                GROUP BY
                    account_id, month_start
            )
            SELECT
                'Daily' AS aggregation_period,
                account_id,
                deal_date AS period_start,
                total_deals,
                total_won_amount,
                total_open_amount,
                total_contacts
            FROM
                daily_aggregates

            UNION ALL

            SELECT
                'Weekly' AS aggregation_period,
                account_id,
                week_start AS period_start,
                total_deals,
                total_won_amount,
                total_open_amount,
                total_contacts
            FROM
                weekly_aggregates

            UNION ALL

            SELECT
                'Monthly' AS aggregation_period,
                account_id,
                month_start AS period_start,
                total_deals,
                total_won_amount,
                total_open_amount,
                total_contacts
            FROM
                monthly_aggregates
            ORDER BY
                account_id, period_start, aggregation_period;                    
        
        """,
        use_legacy_sql=False,
        destination_dataset_table= f'{PROJECT_ID}.{BQ_STAGING_DATASET}.daily_aggregate',
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED")
    


    # List of contacts per account
    list_contacts_per_account = BigQueryExecuteQueryOperator(
        task_id= 'list_contacts_per_account',
        sql= """
            SELECT 
                a.id AS account_id,
                a.name AS account_name,
                c.id AS contact_id,
                c.first_name,
                c.last_name,
                c.email,
                c.mobile_number
            FROM 
                accounts AS a
            LEFT JOIN 
                contacts AS c ON a.id = c.account_id  -- Adjust based on how accounts are related to contacts
            ORDER BY 
                a.id, c.last_name;

        """,
        destination_dataset_table= f'{PROJECT_ID}.{BQ_STAGING_DATASET}.contacts_per_account',
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED"
      )









    begin = DummyOperator(task_id="begin")
    end = DummyOperator(task_id="end")

    # Define Task Dependencies
    begin >> [parse_deals_csv, parse_contacts_csv, parse_accounts_csv]

    parse_accounts_csv >> accounts_clean_task >> accounts_schema_change >> accounts_parquet_upload >> create_accounts_staging_table >> transfer_sales_to_bigquery

    parse_contacts_csv >> contacts_clean_task >> contacts_schema_change >> contacts_parquet_upload >> create_contacts_staging_table >> transfer_contacts_to_bigquery

    parse_deals_csv >> deals_clean_task >> deals_schema_change >> deals_parquet_upload >> create_deals_staging_table >> transfer_deals_to_bigquery

    [transfer_sales_to_bigquery, transfer_contacts_to_bigquery, transfer_deals_to_bigquery ] >> daily_aggregations_per_account >> list_contacts_per_account >> end


