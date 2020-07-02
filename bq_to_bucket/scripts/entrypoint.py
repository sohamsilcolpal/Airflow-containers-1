
import pandas as pd
import argparse
import json
import os
import base64
import random
import string
import google.cloud.bigquery as bq
from google.cloud import storage
from datetime import datetime
# import pydata_google_auth



def bq_auth_client():
    gcs_sa = json.loads(os.environ['BQ_KEY'])
    with open('bq-sa.json', 'w') as json_file:
        json.dump(gcs_sa, json_file)

    return bq.client.Client.from_service_account_json('bq-sa.json')

def gcs_auth_client():
    gcs_sa = json.loads(os.environ['GCS_KEY'])
    with open('gcs-sa.json', 'w') as json_file:
        json.dump(gcs_sa, json_file)

    return storage.client.Client.from_service_account_json('gcs-sa.json')



parser = argparse.ArgumentParser(description='')

parser.add_argument('--query', metavar='N', type=str, nargs='+',
                    help='query string')     

parser.add_argument("--query_bucket_name", type=str, required=False,
                    help="GCS Bucket to read sql from.")

parser.add_argument("--query_path", type=str, required=False,
                    help="GCS folder in bucket to read sql from.")

parser.add_argument("--query_filename", type=str, required=False,
                    help="Filename to read sql statement.")

parser.add_argument("--output_bucket_name", type=str, required=True,
                    help="GCS Bucket to write to.")

parser.add_argument("--output_path", type=str, required=True,
                    help="GCS folder in bucket to write file to.")

parser.add_argument("--output_filename", type=str, required=True,
                    help="Filename to write file to excluding extension.")

parser.add_argument("--output_type", type=str, required=False, default='csv',
                    help="csv or pickle.")

args = parser.parse_args()


# For Local Use
# SCOPES = [
#     'https://www.googleapis.com/auth/cloud-platform',
#     'https://www.googleapis.com/auth/drive',
#     "https://www.googleapis.com/auth/bigquery"
# ]

# credentials = pydata_google_auth.get_user_credentials(SCOPES)

# bq_cli = bq.Client(
#     credentials=credentials,
#     project='cp-gaa-visualization-dev',
# )
# storage_cli = storage.Client(
#     credentials=credentials,
#     project='cp-gaa-visualization-dev',
# )

bq_cli = bq_auth_client()
storage_cli = gcs_auth_client()

if args.query:
    query = " ".join(args.query)
    
else:
    query_bucket = storage_cli.get_bucket(args.query_bucket_name)
    blob = query_bucket.get_blob(args.query_path+"/"+args.query_filename)
    blob.download_to_filename('./query.txt')
    query = ""
    with open('./query.txt') as file:
        query = query.join([line.replace("/n","") for line in file.readlines()])
        
        
print("QUERY : ",query)
results = bq_cli.query(query).to_dataframe()

if args.output_type=='pickle':
    filename = args.output_filename+".pkl"
    results.to_pickle(filename)
else:
    filename= args.output_filename+".csv"
    results.to_csv(filename,index=False)

bucket = storage_cli.get_bucket(args.output_bucket_name)
blob = bucket.blob(args.output_path+"/"+filename)
blob.upload_from_filename(filename)