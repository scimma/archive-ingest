from django.http import HttpResponse
from rest_framework.views import APIView
from rest_framework import status
from rest_framework.response import Response


def message(request):
    return HttpResponse("Here is a message!")

class test_endpoint(APIView):
    def post(self, request):
        if 'param1' not in request.data or 'param2' not in request.data:
            return Response(status=status.HTTP_400_BAD_REQUEST)
        param1 = request.data['param1']
        param2 = request.data['param2']
        data = {
            'message': f'''param1: {param1}, param2: {param2}'''
        }
        return Response(status=status.HTTP_200_OK, data=data)
