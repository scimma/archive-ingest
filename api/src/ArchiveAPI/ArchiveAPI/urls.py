"""ArchiveAPI URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, include
import os
from . import views

base_path = os.environ.get('DJANGO_URL_BASE_PATH', '').strip('/')
trailing_slash = ''
if base_path:
    trailing_slash = '/'

urlpatterns = [
    path(f'''{base_path}{trailing_slash}admin/''', admin.site.urls),
    path(f'''{base_path}{trailing_slash}api/topic/range''', views.list_topic_range.as_view(), name="get-messages-range"),
    path(f'''{base_path}{trailing_slash}api/topic/<str:topic>''', views.list_topic.as_view(), name="get-messages"),
    path(f'''{base_path}{trailing_slash}api/topics''', views.list_topics.as_view(), name="get-topics"),
    path(f'''{base_path}{trailing_slash}api/message/<str:id>''', views.message_details.as_view(), name="get-message"),
]
