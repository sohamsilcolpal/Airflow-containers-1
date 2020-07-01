import pandas as pd
import argparse
import os
import json
from google.oauth2 import service_account
from google.cloud import storage
import secrets
#tickler
def gcs_auth_client():
    gcs_sa = json.loads(os.environ['GCS_KEY'])
    with open('gcs-sa.json', 'w') as json_file:
        json.dump(gcs_sa, json_file)

    return storage.Client.from_service_account_json('gcs-sa.json')

parser = argparse.ArgumentParser(description='')

parser.add_argument("--input_bucket_name", type=str, required=True,
                    help="GCS Bucket to read from.")

parser.add_argument("--input_path", type=str, required=True,
                    help="GCS folder in bucket to read file from.")

parser.add_argument("--input_filename", type=str, required=True,
                    help="Filename to read csv from.")          

parser.add_argument("--output_bucket_name", type=str, required=True,
                    help="GCS Bucket to write to.")

parser.add_argument("--output_path", type=str, required=True,
                    help="GCS folder in bucket to write file to.")

parser.add_argument("--output_filename", type=str, required=True,
                    help="Filename to write file to.")

parser.add_argument("--index_col_name", type=str, required=True,
                    help="name to give to new column")

parser.add_argument("--unique_by_cols", type=str, required=False,
                    help="comma seperated subset of columns to create index. will result in index duplicates")

parser.add_argument("--hash_len", type=str, required=True,
                    help="length of hash int.")

args = parser.parse_args()


# key_path = "secret.json"
# credentials = service_account.Credentials.from_service_account_file(
#     key_path,
#     scopes=["https://www.googleapis.com/auth/cloud-platform"],
# )


# gcs_client = storage.Client(credentials=credentials,project = args.input_bucket_name)
gcs_client = gcs_auth_client()
# 
input_bucket = gcs_client.bucket(args.input_bucket_name)
output_bucket = gcs_client.bucket(args.output_bucket_name)

input_bucket.get_blob(args.input_path+"/"+args.input_filename).download_to_filename(args.input_filename)

if args.input_filename.endswith('csv'):
    input_data = pd.read_csv(args.input_filename)
    df_raw = pd.DataFrame(input_data)
elif args.input_filename.endswith('pkl'):
    df_raw = pd.read_pickle(args.input_filename)

if args.unique_by_cols:
    groupcols = args.unique_by_cols.split(",")
    temp = df_raw.drop_duplicates(groupcols)
    ids = [secrets.token_hex(int(args.hash_len)) for x in range(len(temp))]
    temp[args.index_col_name] = ids
    temp2 = temp[groupcols + [args.index_col_name]]
    df = df_raw.merge(temp2,on = groupcols)
else:
    ids = [secrets.token_hex(args.hash_len) for x in range(len(df_raw))]
    df_raw[args.index_col_name] = ids
    df = df_raw

if args.output_filename.endswith('pkl'):
    df.to_pickle(args.output_filename)
else:
    df.to_csv(args.output_filename,index=False)
blob = output_bucket.blob(args.output_path+"/"+args.output_filename)
blob.upload_from_filename(args.output_filename)









