from django.http import HttpResponse
from rest_framework.views import APIView
from rest_framework import status
from rest_framework.response import Response
import os
from .db_connector import DbConnector, MongoDbConnector


db = DbConnector(
    hostname=os.environ.get('ARCHIVE_DB_HOST', 'archive-db'),
    username=os.environ.get('ARCHIVE_DB_USERNAME', 'archiver'),
    password=os.environ.get('ARCHIVE_DB_PASSWORD', 'archiver'),
    database=os.environ.get('ARCHIVE_DB_DATABASE', 'archiver'),
)

# Get global instance of the job handler database interface
mongodb = MongoDbConnector(
    hostname=os.environ.get('MONGO_DB_HOSTNAME', 'mongo-db'),
    username=os.environ.get('MONGO_DB_USERNAME', 'root'),
    password=os.environ.get('MONGO_DB_PASSWORD', 'password'),
    database=os.environ.get('MONGO_DB_DATABASE', 'SCiMMA_archive'),
    collection=os.environ.get('MONGO_DB_COLLECTION', 'SCiMMA_public'),
    port=os.environ.get('MONGO_DB_PORT', '27017'),
)

class list_topic_range(APIView):
    def post(self, request):
        if 'start_date' not in request.data or 'end_date' not in request.data:
            return Response(status=status.HTTP_400_BAD_REQUEST)
        
        messages = db.list_time_range(start_date=request.data['start_date'],end_date=request.data['end_date'])
        data = {
            'topic': "",
            'messages': messages,
        }
        return Response(status=status.HTTP_200_OK, data=data)

class list_topic(APIView):
    def get(self, request, topic):
        if not topic:
            return Response(status=status.HTTP_400_BAD_REQUEST)
        messages = db.list_topic(topic=topic)
        data = {
            'topic': topic,
            'messages': messages,
        }
        return Response(status=status.HTTP_200_OK, data=data)

class list_topics(APIView):
    def get(self, request):
        topics = db.list_topics()
        data = {
            'topics': topics,
        }
        return Response(status=status.HTTP_200_OK, data=data)

class message_details(APIView):
    def get(self, request, id):
        message_details = mongodb.message_details(id=id)
        # message_details = db.message_details(id=id)
        data = {
            'id': id,
            'details': message_details,
        }
        return Response(status=status.HTTP_200_OK, data=data)
