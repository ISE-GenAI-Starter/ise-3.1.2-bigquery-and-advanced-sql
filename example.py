from google.cloud import bigquery

# Create API client
client = bigquery.Client()


# Perform query
def run_query(query):
    query_job = client.query(query)
    # Convert to hashable list format, which allows the caching to work
    rows = [dict(row) for row in query_job.result()]
    return rows


rows = run_query("SELECT word FROM `bigquery-public-data.samples.shakespeare` LIMIT 10")

print("Some shakespearian words:")
for row in rows:
    print(row["word"])
