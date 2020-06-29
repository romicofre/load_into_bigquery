import time

import pandas as pd
from google.cloud import bigquery
from google.cloud import firestore
from google.cloud import logging

from utils import memory_usage


def from_firebase():
    """
    Returns:
        New data in users table
        Successful message
    """
    start_time = time.time()

    print('Init extract users from Firestore')
    db = firestore.Client()
    users = list(db.collection(u'users').stream())

    print('Convert users collection to pandas dataframe')
    users_dict = list(map(lambda x: x.to_dict(), users))
    df = pd.DataFrame(users_dict)
    df = df.drop(['column1', 'column2'], axis=1)
    # df = df.where(pd.notnull(df), None)  # some transform
    print('Duration: {} seconds - Memory Usage: {}'.format(time.time() - start_time, memory_usage()))

    print('Attempt to load users to BigQuery')
    client = bigquery.Client()
    table_id = 'bq_dataset.pandas_table'
    # Since string columns use the "object" dtype, pass in a (partial) schema
    # to ensure the correct BigQuery data type.
    df = df[['column3', 'column4']]
    job_config = bigquery.LoadJobConfig(schema=[
        bigquery.SchemaField("column4", "STRING"),
        bigquery.SchemaField("column3", "STRING")
    ])
    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )
    # Wait for the load job to complete.
    job.result()
    print('Duration: {} seconds - Memory Usage: {}'.format(time.time() - start_time, memory_usage()))
    return 'OK'


print(from_firebase())
