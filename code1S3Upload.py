#CODE 1 - S3 Upload
import boto
import os
import pandas as pd
import ssl
import sys
from boto.s3.connection import S3Connection

def main():
    '''
    This pulls xml casefiles
    1. Import credentals
    2. Retrieve Casefiles
    3. Loop Casefiles to Push to s3
    '''
    pass

# AWS Credentials
S3Conn = S3Connection('MyAWSaccessKey','MyAWSsecretKey')
greatbucket =S3Conn.get_bucket('bucketname')

###  Accessing Desktop Folder
path = '/Users/mycomputerName/Desktop/myFolder/'

# UPLOAD BY LOOPING THRU BY INDIVIDUAL XML FILE
for i in (os.listdir(path)):
     file = greatbucket.new_key(i)
     file.set_contents_from_filename(path+'/'+i)

if __name__ == '__main__':
     main()
