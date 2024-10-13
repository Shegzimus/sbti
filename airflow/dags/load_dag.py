import os
import pandas as pd
import json
import sys
import requests

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import BigQueryToBigQueryOperator



# Environment variables exposed in airflow by Terraform
PROJECT_ID = os.environ.get('PROJECT_ID')
BUCKET = os.environ.get('GCP_GSC_BUCKET')
BQ_STAGING_DATASET = os.environ.get("BIGQUERY_DATASET_1")
BQ_WAREHOUSE_DATASET = os.environ.get("BIGQUERY_DATASET_2")


# DECLARE FUNCTIONS NEEDED TO UPDATE THE API
def get_api_key():
    api_key_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../secrets/api_key.txt'))
    
    with open(api_key_path, 'r') as file:
        return file.read().strip()
    

def make_api_request(url: str):
    """
    Makes a GET request to the provided URL using an API key for authorization.

    This function retrieves the API key from a local file, constructs the 
    appropriate headers for the request, and sends a GET request to the specified 
    URL. If the request is successful (HTTP status code 200), the response is 
    returned. Otherwise, an exception is raised with the status code and error message.

    Args:
        url (str): The endpoint URL for the API request.

    Returns:
        requests.Response: The HTTP response object if the request is successful.

    Raises:
        Exception: If the API request fails, an exception is raised with the status 
                   code and the response text.
    """
    api_key = get_api_key()
    headers = {
        'Authorization': f'Token {api_key}',
        'Content-Type': 'application/json'
    }

    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response
    else:
        raise Exception(f"API request failed: {response.status_code}, {response.text}")


def update_deal(deal: dict, api_key: str) -> None:
    """Takes an updated deal dictionary as the only parameter and updates that deal in the Deals table using an API request."""

    # Get deal ID and form URL for request
    id = deal['id']
    url = f"https://freelance-763910376508891693.myfreshworks.com/crm/sales/api/deals/{id}"
    headers = {
        'Authorization': f'Token {api_key}',
        'Content-Type': 'application/json'
    }

    try:
        # Send PUT request to API
        response = requests.put(
            url=url,
            headers=headers,
            json=deal
        )
        response.raise_for_status()

    except requests.HTTPError as e:
        print(f"Error updating deal {id}: {e}")

    finally:
        return 


def make_update():
    # Fetch current deals
    url = 'https://freelance-763910376508891693.myfreshworks.com/crm/sales/api/deals/view/202000824304'
    response = make_api_request(url)
    current_deals =  response.json()['deals']

    select_deal = current_deals[0]

    select_deal['amount'] = 24456
    update_deal(select_deal, get_api_key())




default_args = {
    'owner': 'SBTi',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 8, 10, 00),
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id='sbti_load_dag',
    default_args=default_args,
    schedule= '',
    catchup=False) as dag:



    # Task to create a BigQuery table in the warehouse dataset for the deals data
    create_deals_load_table = BigQueryCreateEmptyTableOperator(
        task_id='create_deals_load_table',
        dataset_id=f'{PROJECT_ID}.{BQ_WAREHOUSE_DATASET}.deals',
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
                {"name": "custom_field_cf_deal_size", "type": "FLOAT", "mode": "NULLABLE"}, 
                {"name": "custom_field_cf_deal_stage", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "links_conversations", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "links_document_associations", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "links_notes", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "links_tasks", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "links_appointments", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "fc_widget_collaboration_convo_token", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "fc_widget_collaboration_auth_token", "type": "STRING", "mode": "NULLABLE"}, 
                {"name": "fc_widget_collaboration_encoded_jwt_token", "type": "STRING", "mode": "NULLABLE"}],
    )

    # Task to create a BigQuery table in the warehouse dataset for the accounts data
    create_accounts_load_table = BigQueryCreateEmptyTableOperator(
        task_id='create_accounts_load_table',
        dataset_id=f'{PROJECT_ID}.{BQ_WAREHOUSE_DATASET}.accounts',
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
                {"name": "custom_field_cf_region", "type": "STRING", "mode": "NULLABLE"}],
    )   

    # Task to create a BigQuery table in the warehouse dataset for the contacts data
    create_contacts_load_table = BigQueryCreateEmptyTableOperator(
        task_id='create_contacts_load_table',
        dataset_id=f'{PROJECT_ID}.{BQ_WAREHOUSE_DATASET}.contacts',    
        table_id='contacts_staging',
        schema= [{"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
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
                 {"name": "external_id", "type": "STRING"},
                 {"name": "work_email", "type": "STRING"},
                 {"name": "subscription_status", "type": "STRING"},
                 {"name": "subscription_types", "type": "STRING"},
                 {"name": "unsubscription_reason", "type": "STRING"},
                 {"name": "other_unsubscription_reason", "type": "STRING"},
                 {"name": "customer_fit", "type": "STRING"},
                 {"name": "record_type_id", "type": "STRING"},
                 {"name": "whatsapp_subscription_status", "type": "STRING"},
                 {"name": "sms_subscription_status", "type": "STRING"},
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
                 {"name": "custom_field_cf_job_title", "type": "STRING"},
                 {"name": "custom_field_cf_lead_source", "type": "STRING"},
                 {"name": "custom_field_cf_last_contacted", "type": "STRING"}],
    )

    """
    #### PUSH TO WAREHOUSE
    """
    push_deals_to_DWH = BigQueryToBigQueryOperator(
        source_project_dataset_tables= f'{PROJECT_ID}.{BQ_STAGING_DATASET}.deals_staging',
        destination_project_dataset_table= f'{PROJECT_ID}.{BQ_WAREHOUSE_DATASET}.deals',
        write_disposition= 'WRITE_EMPTY',


    
    )

    push_contacts_to_DWH = BigQueryToBigQueryOperator(
        source_project_dataset_tables= f'{PROJECT_ID}.{BQ_STAGING_DATASET}.contacts_staging',
        destination_project_dataset_table= f'{PROJECT_ID}.{BQ_WAREHOUSE_DATASET}.contacts',
        write_disposition= 'WRITE_EMPTY',

    
    )

    push_accounts_to_DWH = BigQueryToBigQueryOperator(
        source_project_dataset_tables= f'{PROJECT_ID}.{BQ_STAGING_DATASET}.accounts_staging',
        destination_project_dataset_table= f'{PROJECT_ID}.{BQ_WAREHOUSE_DATASET}.accounts',
        write_disposition= 'WRITE_EMPTY' ,
    )


    """
    #### PUSH TO FRESHWORKS
    """
    update_API_task = PythonOperator(
        task_id='update_API_task',
        python_callable=make_update(),
        provide_context=True,
    )



    begin = DummyOperator(task_id="begin")
    end = DummyOperator(task_id="end")

    """
    #### CHAIN TASKS
    """  

    begin >> [create_deals_load_table, create_accounts_load_table, create_contacts_load_table] 
    
    create_deals_load_table >> push_deals_to_DWH
    create_accounts_load_table >> push_accounts_to_DWH
    create_contacts_load_table >> push_contacts_to_DWH

    [push_deals_to_DWH, push_accounts_to_DWH, push_contacts_to_DWH] >> update_API_task >> end


