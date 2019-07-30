import math, os, boto, ssl, time, sys
import boto.s3.connection
from filechunkio import FileChunkIO

access_key = 'your access key'   
secret_key = 'your secret key'
bucket_name = 'your bucket name'
# source directory
sourceDir = 'directory path'
#destination on Wasabi
destDir = ''

#max size in bytes before uploading in parts. between 1 and 5 GB recommended but Wasabi cannot upload a file greater than 5 GB without creating partitions
MAX_SIZE = 20 * 1000 * 1000
#size of partitions when uploading in parts
PART_SIZE = 6 * 1000 * 1000
#starts the timer so time taken to upload entire batch is recorded
start = time.time() 

#conecting to Wasabi Bucket using Boto. Wasabi uses Amazon S3 API
connecting_to_wasabi = boto.connect_s3(
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key,
        host = 's3.wasabisys.com',
#        is_secure=False,               # uncomment if you are not using ssl
        calling_format = boto.s3.connection.OrdinaryCallingFormat(),
        )
#make sure the connection is made
print('connection made')

#specifying the specific bucket in Wasabi that the files are being uploaded to 
target_bucket = connecting_to_wasabi.get_bucket(bucket_name)
#make sure specific bucket is attached
print('bucket attached')


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
    sourcepath = os.path.join(sourceDir + '/' +filename) #path to file
    destpath = os.path.join(destDir, filename) #path to destination in Wasabi
    print(('Uploading ') + str(sourcepath) + (' Wasabi bucket ') + str(bucket_name)) 
#file size
    filesize = os.path.getsize(sourcepath)
#decides whether or not the specific file currently being uploaded should be uploaded in parts or as a whole 
    if filesize > MAX_SIZE:
        print("multipart upload")
        print(str(filesize / 6000000) + (" part upload"))
        mp = target_bucket.initiate_multipart_upload(destpath)
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
        k = boto.s3.key.Key(target_bucket)
        k.key = destpath
        k.set_contents_from_filename(sourcepath,
                cb=percent_cb, num_cb=10)
        mp.complete_upload()

time_taken = time.time() - start 
print("It took " + str(time_taken) + " seconds to download your files")
