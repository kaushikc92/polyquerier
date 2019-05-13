from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.parsers import FormParser, MultiPartParser

import boto3

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

