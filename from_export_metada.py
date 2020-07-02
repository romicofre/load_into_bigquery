import time

from google.cloud import bigquery

from utils import memory_usage

start_time = time.time()
print('Init load export document of users in bigquery')
client = bigquery.Client()
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND",)  # or WRITE_TRUNCATE
job_config.source_format = bigquery.SourceFormat.DATASTORE_BACKUP  # file is an .export_metadata document
uri = "gs://firebase_firestore/all_users.export_metadata"
table_id = 'dataset.table'
load_job = client.load_table_from_uri(
    uri,
    table_id,
    job_config=job_config,
)  # API request
print("Starting job {}".format(load_job.job_id))
load_job.result()  # Waits for table load to complete.
print("Job finished.")
print('Duration: {} seconds - Memory Usage: {}'.format(time.time() - start_time, memory_usage()))



