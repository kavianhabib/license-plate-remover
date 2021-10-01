from django.contrib import admin
from .models import Video, Image, VideoSession, ImageSession

admin.site.register(Video)
admin.site.register(Image)
admin.site.register(ImageSession)
admin.site.register(VideoSession)