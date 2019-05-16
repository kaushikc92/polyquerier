from django.conf import settings

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.parsers import FormParser, MultiPartParser, JSONParser

import boto3
from botocore.client import Config

# Create your views here.
class UploadTableView(APIView):
    parser_class = (MultiPartParser, FormParser)

    def post(self, request):
        table_name = request.data['table_name']
        s3 = boto3.client(
            's3',
            region_name = 'us-east-1',
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        )
        for csvfile in request.data.getlist('files[]'):
            object_path = table_name + '/' + csvfile.name
            s3.upload_fileobj(csvfile, settings.AWS_STORAGE_BUCKET_NAME, object_path)

        schema = str(request.data['schema_file'].read(), 'utf-8').rstrip()

        query_string = (
            'CREATE EXTERNAL TABLE IF NOT EXISTS '
            + table_name
            + '('
            + schema
            + ')'
            + ' ROW FORMAT SERDE \'org.apache.hadoop.hive.serde2.OpenCSVSerde\''
            + ' LOCATION \'s3://'
            + settings.AWS_STORAGE_BUCKET_NAME
            + '/'
            + table_name
            + '/\''
            + ' TBLPROPERTIES ('
            + ' \'skip.header.line.count\' = \'1\''
            + ' )'
        )

        athena = boto3.client(
            'athena',
            region_name = 'us-east-1',
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        )

        athena.start_query_execution(
            QueryString = query_string,
            QueryExecutionContext = {
                'Database': 'lakesites'
            },
            ResultConfiguration = {
                'OutputLocation': 's3://' + settings.AWS_STORAGE_BUCKET_NAME + '/output/'
            }
        )

        return Response(status=status.HTTP_201_CREATED)

class RunQueryView(APIView):
    parser_class = (MultiPartParser, FormParser)

    def post(self, request):
        query_string = request.data['query']
        athena = boto3.client(
            'athena',
            region_name = 'us-east-1',
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        )
        response = athena.start_query_execution(
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
        athena = boto3.client(
            'athena',
            region_name = 'us-east-1',
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        )
        response = athena.get_query_execution(
            QueryExecutionId = queryExecutionId
        )
        data = {}
        state = response['QueryExecution']['Status']['State']
        data['state'] = state
        if 'EngineExecutionTimeInMillis' in response['QueryExecution']['Statistics']:
            data['runtime'] = response['QueryExecution']['Statistics']['EngineExecutionTimeInMillis']
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
