import boto3
from settings import SILVER_BUCKET, GOLD_BUCKET
from itertools import groupby
from operator import itemgetter
import json


def main(event, context):
    try:
        print('Starting datalake gold layer')

        year = event['year']
        search = event['search']
        key = event['key']
        silver_bucket = SILVER_BUCKET
        gold_bucket = GOLD_BUCKET

        print('Year: {} | Search: {} | Key: {}'.format(year, search, key))

        data = get_data_from_s3(silver_bucket=silver_bucket, year=year, search=search)
        data = group_by_key(data=data, k=key)
        send_to_gold_layer(data=data, year=year, search=search, gold_bucket=gold_bucket)

    except Exception as exc:
        print('Erron on starting datalake gold: {}'.format(str(exc)))

def get_data_from_s3(silver_bucket, year, search):
    try:
        s3_client = boto3.resource('s3')
        obj = s3_client.Bucket(silver_bucket).Object('{}/{}.json'.format(year, search))
        file_stream = obj.get()['Body'].read()

        return json.loads(file_stream)
    except Exception as exc:
        raise Exception(exc)

def group_by_key(data, k):
    try:
        output = dict()
        d = sorted(data, key=itemgetter(k))
        for key, value in groupby(d, key=itemgetter(k)):
            output.update({key: list(value)})

        return output
    except Exception as exc:
        raise Exception(exc)

def send_to_gold_layer(data, year, search, gold_bucket):
    s3 = boto3.client('s3')

    for key, value in data.items():
        filename = year + '/' + key + '/' + search + '.json'
        content = bytes(json.dumps(value).encode('utf-8'))
        s3.put_object(Bucket=gold_bucket, Key=filename, Body=content)
        print('{} data have been ingested'.format(key))


if __name__ == '__main__':
    event = {
        'search': 'fear',
        'year': '2015',
        'key': 'Type'
    }
    main(event=event, context=None)
