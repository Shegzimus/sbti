import os
import pandas as pd
import json
import sys
import requests

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator



# Helper function to read the API key securely from a file
def get_api_key(container_path: str, key_file_name: str):
    
    """
    Retrieves the API key from a local file.

    This function constructs the absolute path to the 'api_key.txt' file, 
    located two directories above the current file in a 'secrets' folder. 
    It opens the file, reads the API key, removes any leading or trailing 
    whitespace, and returns it as a string.

    Returns:
        str: The API key from the 'api_key.txt' file.

    Raises:
        FileNotFoundError: If the 'api_key.txt' file does not exist.
        IOError: If there is an issue reading the file.
    """
    containter_path = '/opt/airflow/secrets'   # Change to Local Directory while testing
    key_file_name = 'api_key.txt'
    api_key_path = os.path.abspath(os.path.join(f'{containter_path}', f'{key_file_name}'))

    with open(api_key_path, 'r') as file:
        return file.read().strip()


# Helper function to make API requests using the securely accessed API key
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


# Function to test API from the deals table
def deals_test():
    url = 'https://freelance-763910376508891693.myfreshworks.com/crm/sales/api/deals/view/202000824304'
    response = make_api_request(url)
    
    print(response.status_code)


# Function to test fetching contacts
def contacts_test():
    url = 'https://freelance-763910376508891693.myfreshworks.com/crm/sales/api/contacts/view/202000824289'
    response = make_api_request(url)
    
    print(response.status_code)


# Function to test fetching accounts
def accounts_test():
    url = 'https://freelance-763910376508891693.myfreshworks.com/crm/sales/api/sales_accounts/view/202000824318'
    response = make_api_request(url)
    
    print(response.status_code)



# Function to loop through page entities and extract data from the API
def extract_data(table: str, table_id: int):
    """
    Extracts paginated data from the Freshworks CRM API for the specified table and saves it as a CSV file.

    This function sends API requests to fetch data from a specified table within Freshworks CRM.
    It handles paginated data by making multiple API calls to retrieve all available entries, 
    combines the data, normalizes it into a DataFrame, and saves it as a CSV file in the 'data' directory. 
    If no entries exist or an error occurs, the function exits with an appropriate message.

    Args:
        table (str): The name of the table to fetch data from (e.g., 'deals', 'contacts').
        table_id (str or int): The unique ID of the table view in Freshworks CRM.

    Raises:
        Exception: If the API request fails at any point, an error message is printed, and the function exits.
    
    Procedure:
        1. Fetch metadata to determine the total number of pages and entries.
        2. Exit if no entries or pages are found.
        3. Iterate through all pages, make API calls, and aggregate the data.
        4. Normalize the data into a Pandas DataFrame and save it as a CSV file.
        5. Exit if no data is found for export.
    """   
    url = f'https://freelance-763910376508891693.myfreshworks.com/crm/sales/api/{table}/view/{table_id}'
    
    response = make_api_request(url)
    data = response.json()


    # ACCESS TOTAL NUMBER OF PAGES FROM THE RESPONSE METADATA
    total_pages = data['meta']['total_pages']
    total_entries = data['meta']['total']

    print(f"Fetching metadata from the {table} table...")
    print(f"Total Pages: {json.dumps(total_pages, indent=3)}")
    print(f"Total Entries: {json.dumps(total_entries, indent=3)}")
    print("Extracting data...")

    # EXIT THE FUNCTION IF NO ENTRIES EXIST
    if not total_entries or total_entries == '0':
        print("No entries found.")
        sys.exit()
    elif not total_pages or total_pages == '0':
        print("No pages to process.")  
        sys.exit() 


    # MAKE LOOPED API CALLS TO FILL ALL PAGES AND CONCATENATE THE DATA
    all_data = []
    current_page = 1

    while current_page <= int(total_pages):
        url = f'https://freelance-763910376508891693.myfreshworks.com/crm/sales/api/{table}/view/{table_id}?page={current_page}'
        response = make_api_request(url)

        # Side quest: If the API call returns 200 for the deals table, update the deal with ID 203
        if table == 'deals':
            current_deals =  response.json()['deals']
            change_deal = None
            for deal in current_deals:
                if deal['custom_field']['cf_deal_id'] == 203:
                    # change_deal = deal
                    deal['amount'] = 24456
                    break

            change_deal['amount'] = 24456
            # update_deal(change_deal, "api_key")        

    
        if response.status_code == 200:
            data = response.json()
            all_data.extend(data[f'{table}'])  # Combine data from all pages
            current_page += 1
        else:
            print(f"Failed to fetch deals for page {current_page}: {response.status_code}, {response.text}")
            break


    # EXTRACT THE DATA FROM THE JSON RESPONSE, CREATE A DATAFRAME AND SAVE AS CSV
    if all_data:
        df = pd.json_normalize(all_data)

        # Replace '.' with '_' in column names to facilitate BigQuery compatibility
        df.columns = df.columns.str.replace('.', '_', regex=False)
        
        # Save the DataFrame as a CSV file
        df.to_csv(f'data/{table}.csv', index=False)   # This 'Data' folder was already created by the Dockerfile 
    else:
        print("No data found to export.")
        sys.exit()


# WRAPPER FUNCTIONS TO EXTRACT SPECIFIC TABLES (deals, contacts, accounts). Very refactorable.

def extract_deals():
    extract_data('deals', '202000824304')

def extract_contacts():
    extract_data('contacts', '202000824289')

def extract_accounts():
    extract_data('sales_accounts', '202000824318')


# Environment variables for the extraction DAG
PROJECT_ID = os.environ.get('PROJECT_ID')
BUCKET = os.environ.get('GCP_GSC_BUCKET')

default_args = {
    'owner': 'SBTi',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 12, 10, 00),
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id='sbti_extraction_dag',
    default_args=default_args,
    schedule='@daily',
    catchup=False
    ) as dag:


# Tasks to test the API for availability
    test_deals_api_task = PythonOperator(
        task_id='test_deals_api',
        python_callable=deals_test,
        dag=dag
    )

    test_accounts_api_task = PythonOperator(
        task_id='test_accounts_api',
        python_callable=accounts_test,
        dag=dag
    )

    test_contacts_api_task = PythonOperator(
        task_id='test_contacts_api',
        python_callable=contacts_test,
        dag=dag
    )

# Tasks to extract/download tables
    extract_deals_task = PythonOperator(
        task_id='extract_deals',
        python_callable=extract_deals,
        dag=dag
    )

    extract_accounts_task = PythonOperator(
        task_id='extract_accounts',
        python_callable=extract_accounts,
        dag=dag
    )

    extract_contacts_task = PythonOperator(
        task_id='extract_contacts',
        python_callable=extract_contacts,
        dag=dag
    )




    begin = DummyOperator(task_id="begin")
    end = DummyOperator(task_id="end")

    # Task dependencies including begin and end
    begin >> [test_deals_api_task, test_accounts_api_task, test_contacts_api_task]

    test_deals_api_task >> extract_deals_task
    test_accounts_api_task >> extract_accounts_task
    test_contacts_api_task >> extract_contacts_task

    [extract_deals_task, extract_accounts_task, extract_contacts_task] >> end