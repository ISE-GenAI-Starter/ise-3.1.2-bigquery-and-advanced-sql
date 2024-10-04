import streamlit as st

from google.cloud import bigquery
from google.oauth2 import service_account

# Create API client
credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
client = bigquery.Client(credentials=credentials)

# Perform query
# Uses st.cache_data to reduce number of duplicate calls to BigQuery
@st.cache_data(ttl=600)
def run_query(query):
    query_job = client.query(query)
    # Convert to hashable list format, which allows the caching to work
    rows = [dict(row) for row in query_job.result()]
    return rows

rows = run_query("SELECT word FROM `bigquery-public-data.samples.shakespeare` LIMIT 10")

st.write("Some shakespearian words:")
for row in rows:
    st.write(row['word'])
