import csv
import datetime
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.bigquery import LoadJobConfig
from google.cloud.bigquery import SchemaField

import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/adrien/Documents/Projects/2018_googleCloudPlatform/noodlytics/service-account.json"

clientBQ = bigquery.Client()#.from_service_account_json('service_account.json')
clientS = storage.Client()#.from_service_account_json('service_account.json')
#Friend Schema EVENTS
SCHEMA = [
    SchemaField('Time', 'FLOAT', mode='required'),
    SchemaField('Type', 'INTEGER', mode='required'),
    SchemaField('Username', 'STRING', mode='required'),
    SchemaField('Payload', 'STRING', mode='nullable'),
    SchemaField('X', 'INTEGER', mode='required'),
    SchemaField('Y', 'INTEGER', mode='required'),
    SchemaField('Z', 'INTEGER', mode='required'),
]


DATE = "20"+(datetime.today() - datetime.timedelta(1)).strftime("%y%m%d")
print(DATE)
#DATASET
FRIEND_DATASET = 'Friends_Beta'
BUCKET_NAME = 'game_friend_bucket'

# List Files through bucket
def list_blobs(bucket_name):
    """Lists all the blobs in the bucket."""
    bucket = clientS.get_bucket(bucket_name)
    blobs = bucket.list_blobs()
    for blob in blobs:
        print(blob.name)

# def upload_yesterday_blobs(bucket_name):
#     upload_blobs(bucket_name, DATE)

def upload_blobs(bucket_name,date):
    bucket = clientS.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix = DATE + '/')
    for blob in blobs:
        print(blob.name)
        upload_blob(blob)

def upload_blob(blob):
    table_id = blob.name.split('/')[1][:-4]+"_events"
    table_ref = clientBQ.dataset(FRIEND_DATASET).table(table_id)
    load_config = LoadJobConfig()
    load_config.skip_leading_rows = 3
    load_config.schema = SCHEMA
    load_config.create_disposition = 'CREATE_IF_NEEDED'
    filePath = "gs://"+BUCKET_NAME+'/'+blob.name
    print(filePath)
    print(table_id)
    job = clientBQ.load_table_from_uri(filePath, table_ref, job_config=load_config) # API request)
    job.result()  # Waits for job to complete
    print('Loaded {} rows into {}:{}.'.format(
        job.output_rows, FRIEND_DATASET, table_id))

def load_data_from_gcs(dataset_id, table_id, source):
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    job = bigquery_client.load_table_from_uri(source, table_ref)

    job.result()  # Waits for job to complete

    print('Loaded {} rows into {}:{}.'.format(
        job.output_rows, dataset_id, table_id))

#DEBUG
#list_blobs(BUCKET_NAME)

#
#################### ROUTINE
upload_blobs(BUCKET_NAME)

