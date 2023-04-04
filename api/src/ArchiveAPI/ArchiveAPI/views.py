from django.http import HttpResponse


def message(request):
    return HttpResponse("Here is a message!")
