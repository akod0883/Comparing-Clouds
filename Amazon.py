# AmazonS3LargeFileUpload
To upload large files onto Amazon S3

import math, os, boto, ssl
from boto.s3.connection import S3Connection
from filechunkio import FileChunkIO
import time
import sys

_old_match_hostname = ssl.match_hostname

def _new_match_hostname(cert, hostname):
   if hostname.endswith('.s3.amazonaws.com'):
      pos = hostname.find('.s3.amazonaws.com')
      hostname = hostname[:pos].replace('.', '') + hostname[pos:]
   return _old_match_hostname(cert, hostname)

ssl.match_hostname = _new_match_hostname

# Connect to S3
connecting_to_s3 = S3Connection('(your access key', 'your secret access key')
print "connection made"

target_bucket = connecting_to_s3.get_bucket('your bucket')
print "bucket attached"

# Get file info
source_path = ('path to your file')
source_size = os.stat(source_path).st_size
print str(source_size/1000000) + ' megabyte download'

# Create a multipart upload request
mp = target_bucket.initiate_multipart_upload(os.path.basename(source_path))
print "multipart process started"



# Use a chunk size of 50 MiB (feel free to change this)
start = time.time()  # seconds since the UNIX epoch

chunk_size = 52428800
chunk_count = int(math.ceil(source_size / float(chunk_size)))
print str(chunk_count) + " part upload"

# Send the file parts, using FileChunkIO to create a file-like object
# that points to a certain byte range within the original file. We
# set bytes to never exceed the original file size.

for i in range(chunk_count):
     print "part " + str(i) + " downloaded"
     offset = chunk_size * i
     bytes = min(chunk_size, source_size - offset)
     with FileChunkIO(source_path, 'r', offset=offset,
                         bytes=bytes) as fp:
         mp.upload_part_from_file(fp, part_num=i + 1)

time_taken = time.time() - start

print "100% downloaded"
print "It took " + str(time_taken) + " seconds to download your file"


# Finish the upload
mp.complete_upload()
 
