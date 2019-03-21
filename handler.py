import json
import uuid
import boto3
from boto3 import resource
from boto3.dynamodb.condition import key
import datetime

def generate_s3_presigned_url(event, context):

    print('generate_s3_presigned_url function executing')
    print('Event passed to handler: '+json.dumps(event))
    print('request body: '+event['body'])

    request = json.loads(event['body'])
    presets = request['presets']

    presetValues = []
    for x in presets:
        print('preset: '+x)
        presetValues.append({'id':x, 'conversionStatus':'processing','outputfile':'default', 'downloadCount':'0'})

    dynamodb_resource = resource('dynamodb')
    table = dynamodb_resource.Table('media-transcoder')

    # print('get item by index')
    # response = table.query(IndexName = 'filename-index', KeyConditionExpress = Key('filename').eq(request['filename']))
    # record = json.dumps(response)
    # print('record found: '+record)

    table.put_item(
        Item = {
            'username': request['username'],
            'filename': request['filename'],
            'processingStatus': 'inprogress',
            'creationDate': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'presets': presetValues
        }
    )

    # Get service client
    s3Client = boto3.client('s3')

    # Generate random s3 key name
    # upload_key = uuid.uuid4().hex
    upload_key = request['filename'] + '-' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Generate presigned url for put request
    presigned_url = s3Client.generate_presigned_url(
        ClientMethod = 'put_object',
        Params = {
            'Bucket': 'mediaconverter-input-bucket',
            'Key': upload_key,
            'ContentType': 'application/octet-stream'
        }
    )

    # Return presigned url

    body =  json.dumps({
            'url' : presigned_url
        })

    response = {
        "statusCode": 200,
        "body": body,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*", # required for cors support
            "Access-Control-Allow-Credentials": "true" # required for cookies, authorization headers with https
        }
    }

    return response

def read_table_item(table_name, pk_name, pk_value):
    """
    Return item read by primary key
    """
    dynamodb_resource = resource('dynamodb')
    table = dynamodb_resource.Table(table_name)
    response = table.get_item(Key={pk_name: pk_value})
    return response

def add_item(table_name, col_dict):
    """
    Add one item to table. col_dict is a dictionary {col_name: value}
    """
    dynamodb_resource = resource('dynamodb')
    table = dynamodb_resource.Table(table_name)
    response = table.put_item(Item=col_dict)
    return response 
