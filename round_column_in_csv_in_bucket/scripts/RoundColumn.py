from google.cloud import storage
from io import BytesIO
import json
import argparse
import os
import pandas as pd
import ast


def gcs_auth_client():
    gcs_sa = json.loads(os.environ['GCS_KEY'])
    
    with open('gcs-sa.json', 'w') as json_file:
        json.dump(gcs_sa, json_file)
    return storage.Client.from_service_account_json('gcs-sa.json')
		
parser = argparse.ArgumentParser(description='')

parser.add_argument("--input_bucket_name", type=str, required=True,
                    help="GCS Bucket where the files liv.")

parser.add_argument("--input_path", type=str, required=True,
                    help="Path that containes the file to copy.")

parser.add_argument("--input_file_name", type=str, required=True,
                    help="File name to copy.")

parser.add_argument("--output_bucket_name", type=str, required=True,
                    help="GCS Bucket to write to.")

parser.add_argument("--output_path", type=str, required=True,
                    help="GCS folder in bucket to write file to.")

parser.add_argument("--output_file_name", type=str, required=True,
                    help="Filename to write file to.")

parser.add_argument("--column_to_round", type=str, required=True,
                    help="Column to round.")

parser.add_argument("--round_precision", type=str, required=True,
                    help="precision to round.")

args = parser.parse_args()

download_blob_file_name = args.input_path+'/'+args.input_file_name
download_storage_client = gcs_auth_client()
in_bucket = download_storage_client.bucket(args.input_bucket_name)
in_blob = in_bucket.blob(download_blob_file_name)
in_blob.download_to_filename(args.input_file_name)

input_data = pd.read_csv(args.input_file_name)
input_data_df = pd.DataFrame(input_data)

round_data_df = input_data_df.round({args.column_to_round: int(args.round_precision)})

round_data_df.to_csv(args.output_file_name,index=False)

upload_blob_file_name = args.output_path+'/'+args.output_file_name
upload_storage_client = gcs_auth_client()
out_bucket = upload_storage_client.bucket(args.output_bucket_name)
out_blob = out_bucket.blob(upload_blob_file_name)
out_blob.upload_from_filename(args.output_file_name)