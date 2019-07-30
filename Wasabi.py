import math, os, boto, ssl, time, sys
import boto.s3.connection
from filechunkio import FileChunkIO

access_key = 'your access key'
secret_key = 'your secret access key'

connecting_to_wasabi = boto.connect_s3(
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key,
        host = 's3.wasabisys.com',
#        is_secure=False,               # uncomment if you are not using ssl
        calling_format = boto.s3.connection.OrdinaryCallingFormat(),
        )
print 'connection made'

target_bucket = connecting_to_wasabi.get_bucket('<desired bucket>')
print 'bucket attached'
#
source_path_to_file = ('<path to your file>')
source_size = os.stat(source_path_to_file).st_size
print str(source_size/1000000) + ' megabyte download'

# Create a multipart upload request
mp = target_bucket.initiate_multipart_upload(os.path.basename(source_path_to_file))
print "multipart process started"


time_taken = time.time()  # seconds since the UNIX epoch

# Use a chunk size of 50 MiB (feel free to change this)
chunk_size = 52428800
chunk_count = int(math.ceil(source_size / float(chunk_size)))
print str(chunk_count) + " part upload"

# Send the file parts, using FileChunkIO to create a file-like object
# that points to a certain byte range within the original file. We
# set bytes to never exceed the original file size.

for i in range(chunk_count):
     print 'part ' + str(i) + ' downloaded' 
     offset = chunk_size * i
     bytes = min(chunk_size, source_size - offset)
     with FileChunkIO(source_path_to_file, 'r', offset=offset,
                         bytes=bytes) as fp:
         mp.upload_part_from_file(fp, part_num=i + 1)
elapsed = time.time() - time_taken 
print "100% downloaded"
print str(elapsed) + ' seconds'

# Finish the upload
mp.complete_upload()
