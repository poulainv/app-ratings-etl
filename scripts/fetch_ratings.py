from time import sleep
import boto3
import time
import json
from random import (randrange, randint)
from datetime import datetime
from datetime import timedelta

PLATFORMS = ['iOS', 'Android']

def random_date(start, end):
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)

def create_client(service, region):
    return boto3.client(service, region_name=region)

def load_ratings(start, end, count):
    return [{
        "published_date": random_date(start, end).strftime('%Y-%m-%d'),
        "author": "id-{}".format(i),
        "rating": randint(0, 5),
        "platform": PLATFORMS[randint(0, 1)]
    } for i in range(count)]

# # function for sending data to Kinesis at the absolute maximum throughput
def send_kinesis(kinesis_client, kinesis_stream_name, kinesis_shard_count, data):
    for row in data: 
        encodedValues = bytes(json.dumps(row), 'utf-8') # encode the string to bytes

        # put the records to kinesis
        response = kinesis_client.put_record(
            Record={
                "Data": encodedValues # data byte-encoded
            },
            DeliveryStreamName=kinesis_stream_name
        )

        print(response)
            
    print('Total Records sent to Kinesis: {0}'.format(len(data)))

def main():
    
    start = datetime.strptime('1/10/2020', '%m/%d/%Y')
    end = datetime.strptime('2/16/2020', '%m/%d/%Y')
    ratings_count = 100

    data = load_ratings(start, end, ratings_count)
    
    kinesis = create_client('firehose','eu-west-1')
    
    stream_name = "firehose-blinkist-ratings"
    stream_shard_count = 1
    
    send_kinesis(kinesis, stream_name, stream_shard_count, data)
    
if __name__ == "__main__":
    main()