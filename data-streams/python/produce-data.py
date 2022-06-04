import boto3
import json
import datetime
import random
import uuid

streamName="kinesis-demo-data-stream"
startTime=datetime.datetime.now()
print("started data production for stream ["+streamName+"] at ["+str(startTime)+"]...")
kinesis = boto3.client("kinesis", region_name="eu-central-1")

i=0
rowsToPut=100
random.seed()
while i<rowsToPut:
    tradeAmount=(round(random.random()*1000000))
    fxCurrencies=['EUR','DKK','GBP','USD']
    fxIndex=(round(random.random()*3))
    fxCurrency=fxCurrencies[fxIndex]
    day=(round(random.random()*30))
    month=(round(random.random()*12))
    tradeDate=str(day).zfill(2)+"-"+str(month).zfill(2)+"-2021"
    traderId=(round(random.random()*100))
    counterpartyId=(round(random.random()*800))
    tradeId=str(uuid.uuid4())
    trade={"trade_id":tradeId,"trade_type":"FXSwap","trade_amount":tradeAmount,"trade_ccy":fxCurrency,"trade_date":tradeDate,"trade_id":traderId}
    kinesis.put_record(StreamName=streamName, Data=json.dumps(trade), PartitionKey=tradeId)
    i+=1

endTime=datetime.datetime.now()
duration=(endTime-startTime).microseconds/1000
print("duration=["+str(round(duration))+"] ms")
print("avg message put duration=["+str(round(duration/rowsToPut))+"] ms")
print("data generation ended at ["+str(endTime)+"].")