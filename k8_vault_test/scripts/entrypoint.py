
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


parser = argparse.ArgumentParser(description='')

parser.add_argument('--annotations', metavar='N', type=str, nargs='+',
                    help='vault annotations')
args = parser.parse_args()


print("Annotations : ",args.annotations)
