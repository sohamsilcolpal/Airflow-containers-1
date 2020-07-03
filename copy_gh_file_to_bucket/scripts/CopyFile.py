from google.cloud import storage
from git import Repo
import json
import argparse
import os
import glob

def gcs_auth_client():
    gcs_sa = json.loads(os.environ['GCS_KEY'])
    
    with open('gcs-sa.json', 'w') as json_file:
        json.dump(gcs_sa, json_file)
    return storage.Client.from_service_account_json('gcs-sa.json')
		
parser = argparse.ArgumentParser(description='')

parser.add_argument("--input_git_repo", type=str, required=True,
                    help="Repo that contains the file to copy.")

parser.add_argument("--input_path", type=str, required=True,
                    help="Path that containes the file to copy.")

parser.add_argument("--input_file_name", type=str, required=False,
                    help="File name to copy. Can handle wildcard (*) expressions to fetch multiple files.")

parser.add_argument("--output_bucket_name", type=str, required=True,
                    help="GCS Bucket to write to.")

parser.add_argument("--output_path", type=str, required=True,
                    help="GCS folder in bucket to write file to.")

parser.add_argument("--output_file_name", type=str, required=False,
                    help="Filename to write file to.")

args = parser.parse_args()

Repo.clone_from(args.input_git_repo, 'repo')

client = gcs_auth_client()
bucket = client.get_bucket(args.output_bucket_name)

# get a list of paths in GitHub repo that match input_file_name expression
gh_paths = glob.glob('repo/'+args.input_path+'/'+args.input_file_name)

for path in gh_paths:
    if args.output_file_name:
    	blob = bucket.blob(args.output_path+'/'+args.output_file_name)
    else:
	# if output_file_name is not given, assign it the same name as the file in GitHub
	    blob = bucket.blob(args.output_path+'/'+path.split('/')[-1])
    blob.upload_from_filename(path)
