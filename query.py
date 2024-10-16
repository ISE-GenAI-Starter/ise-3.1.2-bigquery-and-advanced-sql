import os

from google.cloud import bigquery

# Load environment variables from `.env` file
from dotenv import load_dotenv

load_dotenv()

# Set up credentials
project_id = os.environ.get("PROJECT_ID")

if project_id is None:
    print("Did you forget to create a .env file with a PROJECT_ID?")
    exit(1)

# Create API client
client = bigquery.Client()

# Table name
table_name = f"{project_id}.test_dataset.test_table"


# Perform query
def run_query(query):
    query_job = client.query(query)
    # Convert to hashable list format, which allows the caching to work
    rows = [dict(row) for row in query_job.result()]
    return rows


rows = run_query(f"SELECT name, animal_type FROM `{table_name}` LIMIT 10 ")

print("Some animals:")
for row in rows:
    print(row["name"] + " " + row["animal_type"])
