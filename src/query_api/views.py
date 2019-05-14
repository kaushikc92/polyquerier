from django.conf import settings

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.parsers import FormParser, MultiPartParser, JSONParser

import boto3
from botocore.client import Config

import pdb

# Create your views here.
class UploadTableView(APIView):
    parser_class = (MultiPartParser, FormParser)

    def post(self, request):
        data = request.data
        pdb.set_trace()
        return Response(status=status.HTTP_201_CREATED)

class RunQueryView(APIView):
    parser_class = (MultiPartParser, FormParser)

    def post(self, request):
        query_string = request.data['query']
        client = boto3.client('athena', region_name='us-east-1')
        response = client.start_query_execution(
            QueryString = query_string,
            QueryExecutionContext = {
                'Database': 'lakesites'
            },
            ResultConfiguration = {
                'OutputLocation': 's3://lake-data/output/'
            }
        )
        return Response({'queryExecutionId': response['QueryExecutionId']}, status=status.HTTP_201_CREATED)

class QueryStatusView(APIView):
    parser_class = (JSONParser,)

    def get(self, request):
        queryExecutionId = request.query_params['query_id']
        athena = boto3.client('athena', region_name='us-east-1')
        response = athena.get_query_execution(
            QueryExecutionId = queryExecutionId
        )
        data = {}
        state = response['QueryExecution']['Status']['State']
        data['state'] = state
        #data['runtime'] = response['QueryExecution']['Statistics']['EngineExecutionTimeInMillis']
        if state == 'SUCCEEDED':
            s3 = boto3.client(
                's3',
                region_name = 'us-east-1',
                config=Config(signature_version='s3v4'),
                aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            )

            data['downloadUrl'] = s3.generate_presigned_url(
                ClientMethod='get_object',
                Params={
                    'Bucket': settings.AWS_STORAGE_BUCKET_NAME,
                    'Key': 'output/' + queryExecutionId + '.csv'
                },
                ExpiresIn=3600
            )

        return Response(data, status=status.HTTP_200_OK)
