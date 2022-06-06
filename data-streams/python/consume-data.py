from asyncio.windows_events import NULL
import boto3
import json
import datetime
import random
import uuid

streamName="kinesis-demo-data-stream"
startTime=datetime.datetime.now()
print("started data consumption for stream ["+streamName+"] at ["+str(startTime)+"]...")
kinesis = boto3.client("kinesis", region_name="eu-central-1")

i=0
getLimit=1000
#random.seed()
#while i<rowsToGet:
#    kinesis.get_record(StreamName=streamName)
#    i+=1

streamDict = kinesis.describe_stream(StreamName=streamName)
#stream=json.loads(str(streamJSON))
#print(stream)
shardDict=streamDict['StreamDescription']['Shards']
totalNumberOfRecordsFound=0
print(shardDict)
for shard in shardDict:
    shardId = shard['ShardId']
    #print("shardId=["+shardId+"]")    
    shardBucketIterator = kinesis.get_shard_iterator(StreamName=streamName,ShardId=shardId,ShardIteratorType='TRIM_HORIZON')['ShardIterator'] # or trim_horizon, LATEST
    #print("shardIterator=["+str(shardBucketIterator)+"]")
    upToDate=False
    while shardBucketIterator!=NULL and upToDate==False:
        getRecordsResponseDict = kinesis.get_records(ShardIterator=shardBucketIterator,Limit=getLimit)        
        records=getRecordsResponseDict["Records"]
        if(len(records)>0):
            print("Found records in current shard bucket:")
            print("Number of records found ["+str(len(records))+"]")
            totalNumberOfRecordsFound+=len(records)
        #if(records!=NULL and records.count>0): print(getRecordsResponseDict)
        millisBehindLatest=getRecordsResponseDict["MillisBehindLatest"]
        if(millisBehindLatest==0): 
            upToDate=True
            print("All caught up, exiting loop...")
        else: shardBucketIterator=getRecordsResponseDict["NextShardIterator"]
        #print("shardIterator=["+str(shardBucketIterator)+"]")        
    
    #records = kinesis.get_records(ShardIterator=shardIterator['ShardIterator'],Limit=10)
    #print(records)

endTime=datetime.datetime.now()
duration=(endTime-startTime).microseconds/1000
print("duration=["+str(round(duration))+"] ms")
print("avg message get duration=["+str(round(duration/totalNumberOfRecordsFound))+"] ms")
print("data consumption ended at ["+str(endTime)+"].")