"""lpr URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.2/topics/http/urls/
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
from django.urls import path
from lprApp.views import (ImageUploadView, file_upload_view,process_images,VideoUploadView, download,video_upload_view,
process_videos,download, download_video)

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', ImageUploadView.as_view(), name = 'image'),
    path('video/', VideoUploadView.as_view(), name = 'video'),
    path('process_images/<int:id>/',process_images, name = 'process-images'),
    path('process_videos/<int:id>/',process_videos, name = 'process-videos'),
    path('upload/<int:id>', file_upload_view, name = 'upload-view'),
    path('upload-video/<int:id>', video_upload_view, name = 'upload-video'),
    path('download-image/<int:id>', download, name = 'download-image'),
    path('download-video/<int:id>', download_video, name = 'download-video'),
]
