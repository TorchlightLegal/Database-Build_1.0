# CODE 3 - SQL DATA QUERY
#for SQL Database access to Tables:
import psycopg2

#for pythonic activities:
import pandas as pd
from pandas import *
import numpy as np
import sys

# for AWS connections:
import boto
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from collections import Counter

conn = psycopg2.connect(**{
         'dbname':'dbname',
         'host':'dbname.awsaddress.usa-location-3.rds.amazonaws.com',
         'user':'RDSserverUser',
         'password':'password'
         })

cur = conn.cursor()


#get rows with unique Judicial rulings from the database
# bold column has at least one judicial ruling rendered in the case
cur.execute('SELECT bold FROM "CASE2"')
#cur.execute('SELECT COUNT(DISTINCT bold) FROM "CASE2"')

#put SQL querried rows in a pandas dataframe
rows = cur.fetchall()
### Below is random exploration of the SQL to PYTHON (pandas) Experience
#rowsDF = pd.DataFrame(rows)
#print(rowsDF)
#print(rows)
#print(rows[3])
##cur.execute('ROLLBACK')
#rowsDF = pd.DataFrame(rows)
#type(rows)
#print(rowsDF[0])

#print(rows) #a list
str1 = str(rows) #a string
#type(str1)
every_word = str1.split(" ")
rule_wdcount = Counter(every_word)
rulings = rule_wdcount.most_common(150)
rule_df=pd.DataFrame(rulings, columns = ['WORD', 'FREQUENCY']) #pandas
# print(rule_df)

## Convert DF to HTML
rule_df.to_html('rulingWdTally.html')
#Counter(rowsDF[0])

# Connecting to Desired AWS S3 bucket
connS3 = S3Connection('AWSACCESSKEY', 'AWSSECRETKEY')  #need con
postingBucket = connS3.get_bucket('output6007')

# Tossing-up Output (Wd Ct for Judicial Rulings in dB) on the (AWS) web
postingBucket.get_all_keys()
file_key = postingBucket.new_key('rulingWdTally.html')

file_key.content_type = 'text/html'
file_key.set_contents_from_filename('rulingWdTally.html', policy='public-read')
