import argparse
import boto3
import os
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
parser.add_argument('filename', type=str, help='Name of the file to upload')
parser.add_argument('-d', '--delete', help='Delete file if upload is successful', action='store_true')

args = parser.parse_args()

session = boto3.session.Session()

client = session.client('s3',
    region_name=os.getenv('REGION_NAME', ''),
    endpoint_url=os.getenv('ENDPOINT_URL', ''),
    aws_access_key_id=os.getenv('ACCESS_KEY', ''),
    aws_secret_access_key=os.getenv('SECRET', ''))


bucket = os.getenv('BUCKET_NAME', '')

try:
    # Create bucket if it doesn't exist
    client.create_bucket(Bucket=bucket)

except client.meta.client.exceptions.BucketAlreadyExists as err:
    pass

filename = args.filename

try:
    with open(filename, 'rb') as f:
        client.upload_fileobj(
            f, bucket, filename,
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
