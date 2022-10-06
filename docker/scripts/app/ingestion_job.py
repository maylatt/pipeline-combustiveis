"""
Processing JOB
"""

from urllib.request import urlretrieve
from google.cloud import storage
import urllib.request
from urllib.error import ContentTooShortError
import os
import json
import sys
sys.path.insert(0, './config')
from config.gcs import LANDING_BUCKET

def downloader(url, outpath, try_count = 0):
  try_count+=1
  # From URL construct the destination path and filename
  file_name = os.path.basename(urllib.parse.urlparse(url).path)
  file_path = os.path.join(outpath, file_name)
  print(file_path)
  try:
    urlretrieve(url, filename=file_path)
  except ContentTooShortError as e:
    print(f"ContentTooShortError, retrying ({try_count})...")
    if try_count < 4:
      return downloader(url, outpath, try_count)
    raise e

  return file_name, file_path

def gcs_upload(bucket_name, source_file_path, destination_file):
  storage_client = storage.Client()
  bucket = storage_client.bucket(bucket_name)
  blob = bucket.blob(destination_file)
  blob.upload_from_filename(source_file_path)
  print(f"file: {destination_file} uploaded to bucket: {bucket_name} successfully")


if __name__ == "__main__":
  print("Starting Job")
  SOURCE_URLS = os.getenv("SOURCE_URLS", "[]")
  print(f"{SOURCE_URLS}")
  urls = json.loads(SOURCE_URLS)
  print(f"Total files to extract: {len(urls)}")
  for url in urls:
    file_name, file_path = downloader(url, "./")
    print("Sending to GCS")
    gcs_upload(LANDING_BUCKET, file_path, f"combustiveis/{file_name}")
  print("Done!")