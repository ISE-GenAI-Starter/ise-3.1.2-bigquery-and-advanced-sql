import streamlit as st

from google.cloud import bigquery
from google.oauth2 import service_account

# Set up credentials
credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
client = bigquery.Client(credentials=credentials)

# Project ID
# TODO: Replace this with your actual project ID!
project_id = "<YOUR PROJECT ID>"

def create_dataset(dataset_name):
  # Construct a full Dataset object to send to the API.
  dataset = bigquery.Dataset(dataset_name)
  
  # Specify the geographic location where the dataset should reside.
  dataset.location = "US"
  
  # Send the dataset to the API for creation, with an explicit timeout.
  # Raises google.api_core.exceptions.Conflict if the Dataset already
  # exists within the project.
  
  dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
  

def create_table(table_name):
  pass
  print(f"Created Table: {table_name}")

if __name__ == "__main__":
  if project_id is "<YOUR PROJECT ID>":
    raise AssertionError("You need to provide your project ID in the code!")

  dataset_name = f'{project_id}.test_dataset'
  table_name = f'{dataset_name}.test_table'
  
  try:
    create_dataset(dataset_name)
    print(f"Created a dataset called {dataset_name}")
  except google.api_core.exceptions.Conflict:
    print(f"A dataset called {dataset_name} already exists. Continuing...")
    
  try: 
    create_table(table_name)
    print(f"Created a table called {table_name}")
  except google.api_core.exceptions.Conflict:
    print(f"A table called {table_name} already exists. Continuing...")

  print("Done!")
    
  
