#Code 2 - RDS BUILD
import sys
from bs4 import BeautifulSoup
import pandas as pd
import boto
from boto.s3.connection import S3Connection
import os
import ssl
from pyspark import SparkContext
from pyspark.sql import SparkSession
import psycopg2
import string

###########
# AWS Credentials
#S3Conn = S3Connection('MyAWSaccessKey','MyAWSsecretKey')

# ------------ STEP ----------------#
S3Conn = S3Connection('MyAWSaccessKey', 'MyAWSsecretKey',
                      host='s3.amazonaws.com')
greatbucket =S3Conn.get_bucket('bucketname')
#greatbucket =S3Conn.get_bucket('testbucket')


#spark context initialization & spark SQL initialize
sc = SparkContext("local[*]")
spark = SparkSession(sc)

# assigning soup as RDD
path = "s3://bucket/"
#path = "s3://testbucket/"

#  --------STEP -------------#
# create database connection
dbConn = psycopg2.connect(**{
         'dbname':'dbname',
         'host':'host.usa-9.rds.amazonaws.com',
         'user':'user',
         'password':'pwd'
         })
cur = dbConn.cursor()
#cur.execute('INSERT INTO "CASE" VALUES (%s, %s, now(), %s, %s)', ("1", "2", "3", "4") )
#dbConn.commit()

def main():
    '''
    This pulls xml casefiles on S3 bucketname bucket
    1. Import credentals
    2. Loop Casefiles to retrieve contents
    3. while looping parsing and storing conents in dictionaries for corrsponding table
    4.  generating a RDD on S3 based on dictionary values
    '''
    print( "PATH: ", path )
    partition( path )
    #partition( "foldername" )
    #pass

# function created for list of files: os.listdir(path)
# function to read xml files
def partition(path):
    counterDocket = 0
    counterDuplicate = 0
    counterSkip = 0
    dockets = {}

    #for i in (os.listdir(path)):
    for key in greatbucket.list():
            # if true or case with many names
        if 1==1 or key.name == 'singlecaseName.xml':
            #print("MYBUCKET: ",  key.name.encode('utf-8') )
            print("MYFILE: ",  key.name )
            #print("MYCONTENT: ",  key.get_contents_as_string() )

            soup = BeautifulSoup( key.get_contents_as_string() , 'xml')

            dataCASE = {
                'docket' : '',
                'reporter_caption' : '',
                'date' : '',
                'opinion_text' : '',
                'find_order' : ''
            }

            cols = soup.find_all('docket')

            dataCASE['docket'] = None
            if len(cols) > 0:

                # checking for spaces (on either side) on docket
                # also works to specifically erase unwanted char- lft string
                col0Trimmed = cols[0].get_text().lstrip()
                if col0Trimmed.split(" ") > 1:
                    dataCASE['docket'] = col0Trimmed.split(" ")[0]
                else:
                    dataCASE['docket'] = col0Trimmed

                # checking for dots on docket
                if dataCASE['docket'].split(".") > 1:
                    dataCASE['docket'] = dataCASE['docket'].split(".")[0]

                #print("MYDOCKET w true: ", len(cols), dataCASE['docket'], dataCASE['docket'] in dockets)

            #print("SIZE: ", len(dockets) )
            #if dataCASE['docket'] in dockets:
                #print("IS DUPKICATE: ", dockets[dataCASE['docket']])
# RAW DATA - VERY PROBLEMATIC BEGINNING WITH "DOCKET"
            # if no docket, SKIP
            if len(cols) > 0 and dataCASE['docket'] is not None and not dataCASE['docket'] in dockets:

                #print("MYDOCKET IN: ", len(cols), dataCASE['docket'] )
                # to keep track of dockets
                counterDocket = counterDocket + 1
                dockets[dataCASE['docket']] = dataCASE['docket']
                #print("LENGTH XXX: ", len(dockets))

                cols = soup.find_all('reporter_caption')
                #print("MYREPORTER: ", len(cols), cols[0].get_text() )
                dataCASE['reporter_caption'] = cols[0].get_text()

                cols = soup.find_all('date')
                #print("MYDATE: ", len(cols), cols[0].get_text() )
                dataCASE['date'] = cols[0].get_text()

                cols = soup.find_all('opinion_text')
                if len(cols) > 0:
                    #print("MYOPINION: ", len(cols), cols[0].get_text() )
                    dataCASE['opinion_text'] = cols[0].get_text()
                else:
                    dataCASE['opinion_text'] = ''
                    #print("MYOPINION: ", len(cols) )

                # try to get order
                bold = find_order( soup )
                dataCASE['find_order'] = bold
                #print("MYORDER: ", len(cols), bold )

                # filter out binary data from find order / bold
                #stripping non printable characters from a string
                if dataCASE['find_order'] is not None:
                    dataCASE['find_order'] = filter(lambda x: x in string.printable, dataCASE['find_order'])

                # LOAD INTO DATABASE
                cur.execute('INSERT INTO "CASE" ' +
                    '(docket, reporter_caption, date, opinion_byline, bold) ' +
                    'VALUES (%s, %s, %s, %s, %s)', (
                        dataCASE['docket'],
                        dataCASE['reporter_caption'],
                        dataCASE['date'],
                        dataCASE['opinion_text'],
                        dataCASE['find_order']
                    ))
            else:
                if len(cols) <= 0:
                    counterSkip = counterSkip + 1
                else:
                    counterDuplicate = counterDuplicate + 1

            print("Dockets Loded: ", counterDocket)
            print("Dockets Duplicate : ", counterDuplicate)
            print("Dockets Skip : ", counterSkip)

    dbConn.commit()
    print("Dockets Loded: ", counterDocket)
    print("Dockets Duplicate : ", counterDuplicate)
    print("Dockets Skip : ", counterSkip)

#removing control characters/codes fm a string
# aka nonprintable to
# in this case it was <tag>[fn6]<tag>
#https://github.com/nlplab/brat/blob/master/server/src/realmessage.py

def remove_control_chars(s):
    return control_char_re.sub('', s)

# this function gets the Order for a Case
def find_order(s):
    result = None
    for i in s.find_all('bold'):
        #print("BOLD len: ", len(i))
        #print("BOLD: ", str(i))
        if str(i) == '<bold>ORDER:</bold>':
            result = i.next_sibling
            #print("GOT IN: ", len(i))
            #print("RESULT BOLD: ", result)
    #print("RESULT: ", result)
    return result


### TODO: THIS IS NOT BEING CALLED ###
def opinion():
    ##  Load opinion
    dataOPINION = {
        'opinion_text' : [],
        'page_number' : [],
        'footnote_number' : [],
        'footnote_body' : [],
    }

    for soup in soups:
        dataOPINION['docket'].append( cols[0].get_text() )
        cols = soup.find_all('opinion_text')
        dataOPINION['opinion_text'].append( cols[1].get_text() )
        cols = soup.find_all('page_number')
        dataOPINION['page_number'].append( cols[2].get_text() )
        cols = soup.find_all('footnote_number')
        dataOPINION['footnote_number'].append( cols[3].get_text() )
        cols = soup.find_all('footnote_body')
        dataOPINION['footnote_body'].append( cols[4].get_text() )
    ##
    dataCCASES = {
        'cross_reference' : []
    }

    for soup in soups:
        dataCCASES['docket'].append( cols[0].get_text() )
        cols = soup.find_all('cross_reference')
        dataCCASES['cross_reference'].append( cols[1].get_text() )
    ##
    dataPLAYERS = {
        'panel' : [],
        'attorneys' : []
    }

    for soup in soups:
        dataPLAYERS['docket'].append( cols[0].get_text() )
        cols = soup.find_all('panel')
        dataPLAYERS['panel'].append( cols[1].get_text() )
        cols = soup.find_all('attorneys')
        dataPLAYERS['attorneys'].append( cols[2].get_text() )
    ##
    dataDISSENT = {
        'dissent_byline' : [],
        'dissent_text' : []
    }

    for soup in soups:
        dataDISSENT['docket'].append( cols[0].get_text() )
        cols = soup.find_all('dissent_byline')
        dataDISSENT['dissent_byline'].append( cols[1].get_text() )
        cols = soup.find_all('dissent_text')
        dataDISSENT['dissent_text'].append( cols[2].get_text() )

    #load data into database
    #rdd.foreachPartition(soup)

#caseData = pd.DataFrame( dataCASE )
#caseData.to_csv("caseData.csv")
############
