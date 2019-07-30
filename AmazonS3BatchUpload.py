import boto
import boto.s3

import os.path
import sys, ssl, time, math

_old_match_hostname = ssl.match_hostname

## def _new_match_hostname(cert, hostname):
##    if hostname.endswith('.s3.amazonaws.com'):
##      pos = hostname.find('.s3.amazonaws.com')                            ##uncomment this part if your bucket name contains . (ex. bucket.name)
##      hostname = hostname[:pos].replace('.', '') + hostname[pos:]
##      return _old_match_hostname(cert, hostname)

ssl.match_hostname = _new_match_hostname

# to link your aucount
AWS_ACCESS_KEY_ID = 'your access key'
AWS_ACCESS_KEY_SECRET = 'your secret acccess key'
# Fill in info on data to upload
# destination bucket name
bucket_name = 'desired bucket name'
# source directory
sourceDir = 'path to your directory'
# destination directory name (on s3)
destDir = ''

#max size in bytes before uploading in parts. between 1 and 5 GB recommended recommended but Wasabi cannot upload a file greater than 5 GB without creating partitions
MAX_SIZE = 20 * 1000 * 1000
#size of parts when uploading in parts
PART_SIZE = 6 * 1000 * 1000
#starts the timer so time taken to upload entire batch is recorded
start = time.time()
#connecting to to Amazon S3 API using Boto
conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY_SECRET)
#Targeting specific bucket in Amazon S3 Account
bucket = conn.get_bucket(bucket_name)

#Assists in targeting desired files to be uploaded
uploadFileNames = []
for (sourceDir, dirname, filename) in os.walk(sourceDir):
    uploadFileNames.extend(filename)
    break
#informs user percentage of downloaded files
def percent_cb(complete, total):
    sys.stdout.write('.')
    sys.stdout.flush()
#Starts the batch file upload
for filename in uploadFileNames:
    sourcepath = os.path.join(sourceDir + '/' +filename)
    destpath = os.path.join(destDir, filename)
    print(('Uploading ') + str(sourcepath) + (' to Amazon S3 bucket ') + str(bucket_name))

    filesize = os.path.getsize(sourcepath)
    #decides whether or not the specific file currently being uploaded should be uploaded in parts or as a whole
    if filesize > MAX_SIZE:
        print("multipart upload")
        print(str(filesize / 6000000) + (" part upload"))
        mp = bucket.initiate_multipart_upload(destpath)
        fp = open(sourcepath,'rb')
        fp_num = 0
        while (fp.tell() < filesize):
            fp_num += 1
            print(("uploading part ") + str(fp_num))
            mp.upload_part_from_file(fp, fp_num, cb=percent_cb, num_cb=10, size=PART_SIZE)

        mp.complete_upload()

    else:
        print("singlepart upload")
        print(str(filesize / 6000000) + (" part upload"))
        k = boto.s3.key.Key(bucket)
        k.key = destpath
        k.set_contents_from_filename(sourcepath,
                cb=percent_cb, num_cb=10)
        mp.complete_upload()


time_taken = time.time() - start

print("It took " + str(time_taken) + " seconds to download your files")
