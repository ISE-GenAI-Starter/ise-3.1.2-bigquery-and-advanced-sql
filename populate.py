import os

from google.api_core.exceptions import Conflict
from google.cloud import bigquery

# Load environment variables from `.env` file
from dotenv import load_dotenv

load_dotenv()

# Set up credentials
client = bigquery.Client()

# Project ID
project_id = os.environ.get("PROJECT_ID")

if project_id is None:
    print("Did you forget to create a .env file with a PROJECT_ID?")
    exit(1)


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
    schema = [
        bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("animal_type", "STRING", mode="REQUIRED"),
    ]

    table = bigquery.Table(table_name, schema=schema)
    table = client.create_table(table)
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )


def populate_table(table_name):
    client.query(
        f"""
INSERT INTO {table_name} (name, animal_type)
VALUES ('rufus', 'dog'),
('jupiter', 'cat'),
('whiskers', 'cat'),
('max', 'dog'),
('buddy', 'dog'),
('io', 'dog'),
('mittens', 'cat'),
('shadow', 'cat'),
('lucky', 'rabbit'),
('pepper', 'hamster'),
('snowball', 'hamster'),
('sweetie', 'guinea pig'),
('kiwi', 'bird'),
('coco', 'parrot'),
('finny', 'fish'),
('bubbles', 'fish')
  """
    )


if __name__ == "__main__":
    dataset_name = f"{project_id}.test_dataset"
    table_name = f"{dataset_name}.test_table"

    try:
        create_dataset(dataset_name)
        print(f"Created a dataset called {dataset_name}")
    except Conflict:
        print(f"A dataset called {dataset_name} already exists. Continuing...")

    try:
        create_table(table_name)
        print(f"Created a table called {table_name}")
    except Conflict:
        print(f"A table called {table_name} already exists. Continuing...")

    populate_table(table_name)
    print(f"Populated {table_name} with data.")

    print("Done!")
