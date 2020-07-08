from google.cloud import storage
import json
import argparse
import os
import pandas as pd
import requests
import ast
import base64
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
    gcs_base64 = 'ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsCiAgInByb2plY3RfaWQiOiAi\
    Y3AtZ2FhLWRldi1jb25uZWN0aGVhbHRoIiwKICAicHJpdmF0ZV9rZXlfaWQiOiAi\
    OWMxNjcxMWEzODUxZWFlYzZhYTkzYjAyZTczMDgwMTk5MzVmM2YwNiIsCiAgInBy\
    aXZhdGVfa2V5IjogIi0tLS0tQkVHSU4gUFJJVkFURSBLRVktLS0tLVxuTUlJRXZn\
    SUJBREFOQmdrcWhraUc5dzBCQVFFRkFBU0NCS2d3Z2dTa0FnRUFBb0lCQVFDMWY3\
    WWgySzVXemJMZ1xuUXZ5c0FPYUJrRzE1bDh3ZnViOVVyNVdJRkpEV3p4YjNmOGdD\
    ampDTXpwSTBrUWd2SlpLSmpsdmJEMzNPUk16b1xuQ1dETG9TNytjNUh1SEJ6b0pj\
    ckRNWHdPanBhWE1aM1I2UytRNURJZjFXSEFwY3lBVVFiRi9venVERGNtbCtHUVxu\
    VXo1NHlSRjZXdmlNL2Q1bU55clBhRmlMVm52cUc2VVJyOEZGZldVTjhsZVNDSzZK\
    ZzYvSUc3dFk4WlY2VlVRclxuN25vTmR4QkYvRzkrc2FOd2Q2VjdOWU1zVmFtd3pm\
    amxCZ2g1cVVZRVhjdWNTdmtoUXM1UzhqSHNtQXkwSmJGeVxucVJ6VEFuWG9nL1hL\
    cCtrWlVSUGRkREdMa3FpZ1oxS0FHZmwrUFJ4ZnRqdTB4ZnJyTTNCUmN2WnRJZlBw\
    b1EzelxuK1NGdmpVTnhBZ01CQUFFQ2dnRUFRTllJQnF4RWp6NjVKVExZMTBzVkRQ\
    aSttdDYyclNpcUtFTDNIRjhZWkp3aVxuL0xRWmk0d20zTTRZWFkzbnlndldmcXdy\
    dFNRaUVYTHpiZnFYcVBhQjlxVHJYdytNNWdOR0hSZUsxUnByd2tRWFxudURwZ1l4\
    YVpyMVNITUk0cDhUYkF3QzhRUWlONytoM3NscVdlSGdEWjdRY2RYaVI4YzdCR2Ew\
    US95VHJTVmswQlxuUk0yemxITklIZlFCVkhSNkhjT0dZYTZqbDMxMEE4UzJoVzdP\
    enhtcnA4TGhiSzVUc3VuL1RCWXlkYTgwRkpzWVxuZUhQV1RPck0xekZvNzU1Q1lH\
    cTVpTlIrRWdneG03SnRONEp5YklmL2dwODFEWitSTE5NYnVmc0ZOei81ZFA3R1xu\
    ckNqZkpmNmtjTTVoMStTR25FQkY3dS9NWXRZamRYWnYvL1VwWmdMaTF3S0JnUUQ0\
    cUpwSEZsRm5YUW0wWnN1cFxuWjlBVlZuRUdJSmZ5R1NyaS9jbCtGUGtKb3BhbzEv\
    dkJGU1p5ZE5FcEhKd3UvVmFvaktkTzhrMXZzbXVXV0FwWFxuUDRTbFVyckluTUpi\
    N2FMVFVvclZvQ1BMR1FlS1VXTnhCbFNaR3dUdFJTczBkQVVRRmsyM2psdDdseEwy\
    NnF5Z1xubFNmU1B4MVhQSnZpRDVJcXZvUTA3bFNJaXdLQmdRQzYyNEdGRXR1Ui9k\
    M1RRclRnSmNTTTl2Q2QxRVRZZTJKN1xubm5hT0liTkdqWXRDS0xqejVpSGRvUFg4\
    Zy9uZHIxWVZDTW5tUFFSMXVrY2F1NmNoTmNyQjhKVzVuMGk1R24xMFxudFpGY3M4\
    VWc2Uk9YYXNMWnBCMkFodElsM2l2cXd2Ymd2U2lvb0k2aU4zMi84ZStaSzM1dnNI\
    dGRmMDJkbU81L1xudzNGOGhteG5jd0tCZ1FDUnJHVEZwS09JQkk5ODZvUUIrTm1F\
    eHFGc1Q0ZjB3eDNHNFpzN2pCbmh0U2pDUGRZMVxuWmFpaWdTRlFEbnpKWW8xMExR\
    S1BVNUJlRVk3S0EwWDdtekNna2Zqdk1qY0RHQks5N3F0N1AxZjQwZThDNzI5c1xu\
    ZGFMMkYwZ3BvTTRRNnE5VW4xeGtYcmZsamJvRWdiTWtXSDE5eEkveDNZWU5SOFgv\
    MktFblNVQ294UUtCZ0NFd1xuQzNrUE9HQmRycUJ2SUtFUEZESFZTamMrRkZ3K3pl\
    Y1c1Z2VvaEticlRPaWJRdjVPeVlVTHFhVUg2OEVyK3lZUlxuUVlyYkc0ZXRaaU9o\
    WUxYTlVBM1NKVVdxdk1manBKU3VDdnRrRW9jZDNwVHVLVjF1Z0s3clF6Zm1sZlpF\
    V29UZ1xuRS9DZlU3WU1GUkdhYnV2TmhjOWdrUWl5SzU3cUlySWVqSEdKam9keEFv\
    R0JBS1lHcFQwdmVaZytBZ3IrbzZMVlxuTGRWY3ZidmkwYWlwSlZRaXJ4OUlsR1Rm\
    U3ZvOVVldFZ1Y2dBL2JqMVZBQ0V1cGJBZUMxRnhQdVFENjEwckk3UlxuRTJVRVAr\
    Q0syWmZ0V3M0Q2txT1R2dGJvakNZUUFXdldvbjVyeDhyaVZLd2lhd204dlNKVmVv\
    M3YrWVVaSjVZWFxuU0g2cEl4d1pxNit1bURZbXdDRUk5ZWNWXG4tLS0tLUVORCBQ\
    UklWQVRFIEtFWS0tLS0tXG4iLAogICJjbGllbnRfZW1haWwiOiAiY29ubmVjdGhl\
    YWx0aEBjcC1nYWEtZGV2LWNvbm5lY3RoZWFsdGguaWFtLmdzZXJ2aWNlYWNjb3Vu\
    dC5jb20iLAogICJjbGllbnRfaWQiOiAiMTEzNzU3NDM1MjM5NzgyMDA1NTc1IiwK\
    ICAiYXV0aF91cmkiOiAiaHR0cHM6Ly9hY2NvdW50cy5nb29nbGUuY29tL28vb2F1\
    dGgyL2F1dGgiLAogICJ0b2tlbl91cmkiOiAiaHR0cHM6Ly9vYXV0aDIuZ29vZ2xl\
    YXBpcy5jb20vdG9rZW4iLAogICJhdXRoX3Byb3ZpZGVyX3g1MDlfY2VydF91cmwi\
    OiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vb2F1dGgyL3YxL2NlcnRzIiwK\
    ICAiY2xpZW50X3g1MDlfY2VydF91cmwiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBp\
    cy5jb20vcm9ib3QvdjEvbWV0YWRhdGEveDUwOS9jb25uZWN0aGVhbHRoJTQwY3At\
    Z2FhLWRldi1jb25uZWN0aGVhbHRoLmlhbS5nc2VydmljZWFjY291bnQuY29tIgp9\
    Cg=='

    # base64_bytes = gcs_base64.encode('ascii')
    # message_bytes = base64.b64decode(base64_bytes)
    # message = message_bytes.decode('ascii')

    gcs_sa = json.loads(gcs_base64)
    
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
    out_blob.upload_from_string(data=json.dumps(jsondata),
                                content_type='application/json')


