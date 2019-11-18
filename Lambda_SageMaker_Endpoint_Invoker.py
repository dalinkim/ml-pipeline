import os
import boto3
import json

"""
Lambda handler to call SageMaker model endpoint when request is sent using invoke URL 
"""
def lambda_handler(event, context):
    print("Received event: " + json.dumps(event))

    data = json.loads(json.dumps(event))
    data_body = data['data']

    response = boto3.client('runtime.sagemaker').invoke_endpoint(EndpointName=os.environ['ENDPOINT_NAME'],
                                                                 ContentType='text/csv',
                                                                 Body=data_body)
    print(response)

    result = json.loads(response['Body'].read().decode("utf-8"))
    return result