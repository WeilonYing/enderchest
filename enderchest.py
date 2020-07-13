import argparse
import boto3
import os
import settings
import sys
import threading

class ProgressPercentage(object):

    def __init__(self, filename):
        self._filename = filename
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += bytes_amount
            sys.stdout.write(
                "\r%s:   %s" % (
                    self._filename, self._seen_so_far))
            sys.stdout.flush()

parser = argparse.ArgumentParser(description='Uploads files to an S3 bucket')
parser.add_argument('filename', type=str, help='Path to the file to upload')
parser.add_argument('-d', '--delete', help='Delete file if upload is successful', action='store_true')
parser.add_argument('-n', '--name', type=str, required=True, help='Name to upload this as. Defaults to filename.')

args = parser.parse_args()

session = boto3.session.Session()

client = session.client('s3',
    region_name=settings.REGION_NAME,
    endpoint_url=settings.ENDPOINT_URL,
    aws_access_key_id=settings.ACCESS_KEY,
    aws_secret_access_key=settings.SECRET)


bucket = settings.BUCKET_NAME

try:
    # Create bucket if it doesn't exist
    client.create_bucket(Bucket=bucket)

except client.meta.client.exceptions.BucketAlreadyExists as err:
    pass

filename = args.filename
key = filename
if args.name:
    key = args.name

try:
    with open(filename, 'rb') as f:
        client.upload_fileobj(
            f, bucket, key,
            ExtraArgs={'ACL': 'private'},
            Callback=ProgressPercentage(filename))

        print('\nUpload finished')
except Exception as err:
    print('Unable to upload file')
    print(err)
    sys.exit(2)

if args.delete:
    os.remove(filename)
    print(filename, 'deleted')
