from django.http import HttpResponse
from rest_framework.views import APIView
from rest_framework import status
from rest_framework.response import Response
import os
from .db_connector import DbConnector


# Get global instance of the job handler database interface
db = DbConnector(
    hostname=os.environ.get('ARCHIVE_DB_HOST', 'archive-db'),
    username=os.environ.get('ARCHIVE_DB_USERNAME', 'archiver'),
    password=os.environ.get('ARCHIVE_DB_PASSWORD', 'archiver'),
    database=os.environ.get('ARCHIVE_DB_DATABASE', 'archiver'),
)


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
        message_details = db.message_details(id=id)
        data = {
            'id': id,
            'details': message_details,
        }
        return Response(status=status.HTTP_200_OK, data=data)
