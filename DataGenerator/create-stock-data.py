import random
from random import seed
from random import randint
import csv
import argparse
import pymongo
from pymongo import errors
from datetime import date, timedelta, datetime as dt
import threading
import sys
import json
import os
import time

lock = threading.Lock()

volatility = 1  # .001

#arrays used to store ficticous securities
company_symbol=[]
company_name=[]

#the following two functions are used to come up with some fake stock securities
def generate_symbol(a,n,e):
    #we need to break this out into its own function to do checks to make sure we dont have duplicate symbols
    for x in range(1,len(a)):
        symbol=str(a[:x]+n[:1]+e[:1])
        if symbol not in company_symbol:
            return symbol

def generate_securities(numberofsecurities):
    with open('adjectives.txt', 'r') as f:
        adj = f.read().splitlines()
    with open('nouns.txt', 'r') as f:
        noun = f.read().splitlines()
    with open('endings.txt', 'r') as f:
        endings = f.read().splitlines()
    for i in range(0,numberofsecurities,1):
        a=adj[randint(0,len(adj)-1)].upper()
        n=noun[randint(0,len(noun))].upper()
        e=endings[randint(0,len(endings)-1)].upper()
        company_name.append(a + ' ' + n + ' ' + e)
        company_symbol.append(generate_symbol(a,n,e))

#this function is used to randomly increase/decrease the value of the stock, tweak the random.uniform line for more dramatic changes
def getvalue(old_value):
    change_percent = volatility * \
        random.uniform(0.0, .001)  # 001 - flat .01 more
    change_amount = old_value * change_percent
    if bool(random.getrandbits(1)):
        new_value = old_value + change_amount
    else:
        new_value = old_value - change_amount
    return round(new_value, 2)

#When the conainters launch, we want to not crash out if the MongoDB Server container isn't fully up yet so here we wait until we can connect
def checkmongodbconnection():
    try:
        c = pymongo.MongoClient(MONGO_URI)
        c.admin.command('ismaster')
        time.sleep(2)
        c.close()
        return True
    except pymongo.errors.ServerSelectionTimeoutError as e:
        print('Could not connect to server: %s',e)
        return False
    else:
        c.close()
        return False

def main():
    global args
    global MONGO_URI
    # capture parameters from the command line
    parser = argparse.ArgumentParser()
    parser.add_argument("-s","--symbols", type=int, default=10, help="number of financial stock symbols")
    #MongoDB Connector for Apache Kafka requires the source to be a replica set
    parser.add_argument("-c","--connection", default='mongodb://localhost', help="MongoDB connection string")
    #if running inside a docker container on the localnet network use default= mongodb://mongo1:27017,mongo2:27018,mongo3:27019/?replicaSet=rs0
    parser.add_argument("-d","--database", default='Stocks', help="MongoDB database name")
    parser.add_argument("-w","--write", default='Source', help="MongoDB write to collection name")
    parser.add_argument("-r","--read", default='Sink', help="MongoDB read from collection")
  
    args = parser.parse_args()

    if args.symbols:
        if args.symbols < 1:
            args.symbols = 1

    MONGO_URI=args.connection

    threads = []

    generate_securities(args.symbols)

    t = threading.Thread(target=worker, args=[0, int(args.symbols)])
    d = threading.Thread(target=display)
    threads.append(t)
    threads.append(d)
    for x in threads:
        x.start()
    for y in threads:
        y.join()

def display():
    try:
        print('READ: Connecting to MongoDB')
        c = pymongo.MongoClient(MONGO_URI)
        db = c.get_database(name=args.database)
        resume_token = None
        pipeline = [{'$match': {'operationType': 'insert'}}]
        with db[args.read].watch(pipeline) as stream:
            for insert_change in stream:
                print(insert_change["fullDocument"])
                resume_token = stream.resume_token
    except pymongo.errors.PyMongoError:
        if resume_token is None:
            print('Failure during change stream initialization')
            raise
        else:
            with db[args.read].watch(
                pipeline, resume_after=resume_token) as stream:
                for insert_change in stream:
                    print(insert_change["fullDocument"])
    except:
        print('READ: Unexpected error :', sys.exc_info()[0])
        raise

def worker(workerthread, numofsymbols):
    #At the moment we kept the workerthread which is 0 in the future could spin up multiple write threads
    try:
        #Create an initial value for each security
        last_value=[]
        for i in range(0,numofsymbols):
            last_value.append(round(random.uniform(1, 100), 2))

        #Wait until MongoDB Server is online and ready for data
        while True:
            print('WRITE: Checking MongoDB Connection')
            if checkmongodbconnection()==False:
                print('WRITE: Problem connecting to MongoDB, sleeping 10 seconds')
                time.sleep(10)
            else:
                break
        print('WRITE: Successfully connected to MongoDB')

        c = pymongo.MongoClient(MONGO_URI)
        db = c.get_database(name=args.database) #'Stocks'
        write_status=False
        while True:
            for i in range(0,numofsymbols):
                #Get the last value of this particular security
                x = getvalue(last_value[i])
                last_value[i] = x
                txtime = dt.now()
                try:
                    #For now we are writing a flat schema model (one document per data point) since the demo focus is the data flow through Kafka
                    #For performance you would want to use a fixed based schema for time-series data in MongoDB
                    result=db[args.write].insert_one( { 'company_symbol' : company_symbol[i], 'company_name': company_name[i],'price': x, 'tx_time': txtime.strftime('%Y-%m-%dT%H:%M:%SZ')})
                    if write_status==False:
                        print('WRITE: Successfully writing stock data, when you see data you are looking at the data in the sink collection.\ne.g. It has made a round trip through Kafka back to another MongoDB collection\n\n')
                        write_status=True
                except Exception as e:
                    print("WRITE: mongo error: " + str(e))
            time.sleep(1)
        db.close()
    except:
        print('WRITE: Unexpected error:', sys.exc_info()[0])
        raise

main()

