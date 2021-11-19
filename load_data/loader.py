# kudos: https://www.geeksforgeeks.org/mongodb-python-insert-update-data/

# Python code to illustrate
# inserting data in MongoDB
from pymongo import MongoClient
import bz2file as bz2                       # To decompress the Betfair data files.
import glob2 as glob                        # To scan across folders looking for data files.
import json                                 # To decode Betfair data.

import logging
import sys

logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', 
                              '%m-%d-%Y %H:%M:%S')

stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.DEBUG)
stdout_handler.setFormatter(formatter)

file_handler = logging.FileHandler('loader.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stdout_handler)

try:
    conn = MongoClient('localhost', 27017)
    logger.info("Connected successfully!!!")
except:  
    logger.info("Could not connect to MongoDB")
  
# database
db = conn.database
  
# Created or Switched to collection names: my_gfg_collection
collection = db.horses_collection

logger.info("estimated_document_count %s", collection.estimated_document_count())  

# unzip data.tar and access the folder it creates called BASIC, 
# the sub zipped files will be decoded with the below code
filelist = glob.glob('F:/betfair/horses/BASIC/**/*.bz2')
logger.info('Number of files found =%s', len(filelist))

def read_fun_generator(filename):
    with bz2.open(filename, 'rb') as f:
        for line in f:
            # decode - turns binary object into string.
            # json.loads - turns JSON string into Python dictionary.
            jsonLine = json.loads(line.strip().decode())
            #logger.info(jsonLine)
            yield jsonLine

# delete all data first
#collection.delete_many({})

# load all the data from the filelist .. might take awhile
# use "tail -100f logs.txt" in Ubuntu in Windows Terminal to see the progress
c=0
for file in filelist:
    for jsonLine in read_fun_generator(file):
        rec_id1 = collection.insert_one(jsonLine) # Insert Data  
        #logger.info("Data inserted with record ids:", rec_id1)
    c+=1
    logger.info("Finished loading file " + str(c) + ": " + str(file))

# Printing the data inserted
#cursor = collection.find()
#for record in cursor:
#    logger.info(record)

logger.info("estimated_document_count %s", collection.estimated_document_count()) 