from google.cloud import storage
import json
import argparse
import os
import pandas as pd
import requests
import ast
# import pydata_google_auth


# SCOPES = [
#     'https://www.googleapis.com/auth/cloud-platform',
#     'https://www.googleapis.com/auth/drive',
#     "https://www.googleapis.com/auth/bigquery",
# ]

# credentials = pydata_google_auth.get_user_credentials(SCOPES)
# storage = storage.Client(
#     credentials=credentials,
#     project='cp-prod-sociallistening',
# )

def gcs_auth_client():
    gcs_sa = json.loads(os.environ['GCS_KEY'])
    
    with open('gcs-sa.json', 'w') as json_file:
        json.dump(gcs_sa, json_file)
    return storage.Client.from_service_account_json('gcs-sa.json')
		
parser = argparse.ArgumentParser(description='')

parser.add_argument("--input_url", type=str, required=True,
                    help="API URL path.")

parser.add_argument("--input_access_key", type=str, required=True,
                    help="Path that containes the api access key.")

parser.add_argument("--input_access_secret_key", type=str, required=True,
                    help="Path that containes the api secret key.")

parser.add_argument("--output_bucket_name", type=str, required=True,
                    help="GCS Bucket to write to.")

parser.add_argument("--output_path", type=str, required=True,
                    help="GCS folder in bucket to write file to.")

parser.add_argument("--output_filename", type=str, required=True,
                    help="Filename to write to")
                                                                             
args = parser.parse_args()
upload_storage_client = gcs_auth_client()
# download_storage_client = gcs_auth_client()

# in_bucket = download_storage_client.bucket(args.input_bucket_name)
# files = args.input_path_filenames.strip().split(" ")
# print('files: ',files)

url = args.input_url . format(args.input_access_key, args.input_access_secret_key)

response = requests.get(url)

jsondata = json.loads(response.text)

# df_out = pd.DataFrame()

# for file in files:
#     in_blob = in_bucket.get_blob(file)
#
#     if file.endswith('pkl'):
#         in_blob.download_to_filename("temp.pkl")
#         df = pd.read_pickle("temp.pkl")
#     else:
#         in_blob.download_to_filename("temp.csv")
#         df = pd.read_csv('temp.csv')
#     print('chunk_length ',len(df))
#     df_out = df_out.append(df)
#
# print('df_out length: ',len(df_out))


out_bucket = upload_storage_client.bucket(args.output_bucket_name)
upload_blob_filename = args.output_path+'/'+args.output_filename
out_blob = out_bucket.blob(upload_blob_filename)
if args.output_filename.endswith('pkl'):
    print('pkl')
    # df_out.to_pickle(args.output_filename)
elif args.output_filename.endswith('csv'):
    print('csv')
    # df_out.to_csv(args.output_filename,index=False)
elif args.output_filename.endswith('json'):
    print('json')
    # df_out.to_csv(args.output_filename,index=False)
    out_blob.upload_from_string(data=  json.dumps(jsondata),
                                content_type='application/json')


