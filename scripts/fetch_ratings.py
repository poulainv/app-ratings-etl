import boto3
import os
import requests
import json
from random import (randrange, randint)
from datetime import datetime
from datetime import timedelta

APPTWEAK_API_KEY = os.getenv('APPTWEAK_API_KEY')
FIREHOSE_STREAM_NAME = 'firehose-blinkist-ratings'
COUNTRY = 'de'
BLINKIST_APPS = {
    'ios': '568839295',
    'android': 'com.blinkslabs.blinkist.android'
}


def fetch_reviews(api_key, store, app_id, country):
    '''
    Fetch reviews from AppTweak API 
    Returns top 100 arrays of reviews
    '''
    return requests.get('https://api.apptweak.com/{store}/applications/{app_id}/reviews.json?country={country}'.format(
        store=store, app_id=app_id, country=country), headers={'X-Apptweak-Key': api_key}).json()


def create_client(service, region):
    return boto3.client(service, region_name=region)


def transform_apptweak_reviews(reviews, metadata):
    '''
    Transform reviews fetched from AppTweak into expected format used by Kinesis / Glue
    '''
    # TODO Specific better failure behavior
    return [{
        "published_date": datetime.strptime(review['date'], '%Y-%m-%dT%H:%M:%Sz').strftime('%Y-%m-%d'),
        "author": review.get('id'),
        "rating": review['rating'],
        "platform": metadata['request']['store']
    } for review in reviews]


def send_kinesis(kinesis_client, kinesis_stream_name, data):
    # TODO use batch records :)
    for row in data:
        # encode the string to bytes
        encodedValues = bytes(json.dumps(row), 'utf-8')

        # put the records to kinesis
        response = kinesis_client.put_record(
            Record={
                "Data": encodedValues  # data byte-encoded
            },
            DeliveryStreamName=kinesis_stream_name
        )

    print('Total Records sent to Kinesis: {0}'.format(len(data)))


def get_reviews(api_key, store, app_id, country):
    apptweak_reviews = fetch_reviews(api_key, store, app_id, COUNTRY)
    transformed_data = transform_apptweak_reviews(
        apptweak_reviews['content'], apptweak_reviews['metadata'])
    return transformed_data


def main():
    # Fetch data without silent errors

    # Android
    android_reviews = get_reviews(
        APPTWEAK_API_KEY, 'android', BLINKIST_APPS['android'], COUNTRY)
    print('Apptweak Android reviews fetched: {}'.format(len(android_reviews)))

    # iOS
    ios_reviews = get_reviews(APPTWEAK_API_KEY, 'ios',
                              BLINKIST_APPS['ios'], COUNTRY)
    print('Apptweak iOs reviews fetched: {}'.format(len(android_reviews)))

    kinesis = create_client('firehose', 'eu-west-1')
    send_kinesis(kinesis, FIREHOSE_STREAM_NAME, android_reviews + ios_reviews)


if __name__ == "__main__":
    main()
