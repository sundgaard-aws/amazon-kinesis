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
rowsToGet=1
#random.seed()
#while i<rowsToGet:
#    kinesis.get_record(StreamName=streamName)
#    i+=1

streamDict = kinesis.describe_stream(StreamName=streamName)
#stream=json.loads(str(streamJSON))
#print(stream)
shardDict=streamDict['StreamDescription']['Shards']
print(shardDict)
for shard in shardDict:
    shardId = shard['ShardId']
    print("shardId=["+shardId+"]")

shardIterator = kinesis.get_shard_iterator(StreamName=streamName,ShardId=shardId,ShardIteratorType='LATEST')
records = kinesis.get_records(ShardIterator=shardIterator['ShardIterator'],Limit=10)
print(records)

endTime=datetime.datetime.now()
duration=(endTime-startTime).microseconds/1000
print("duration=["+str(round(duration))+"] ms")
print("avg message get duration=["+str(round(duration/rowsToGet))+"] ms")
print("data generation ended at ["+str(endTime)+"].")