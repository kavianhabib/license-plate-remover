from django.db import models
from django.shortcuts import reverse

# Create your models here.

class Image(models.Model):
    upload = models.ImageField(upload_to = 'images/')
    processed_image = models.ImageField(null = True)

    def __str__(self):
        return str(self.pk)

class ImageSession(models.Model):
    images = models.ManyToManyField(Image)
    processed = models.BooleanField(default=False)
    device = models.CharField(max_length=100, null = True)

    def __str__(self):
        return str(self.id)

    def get_process_url(self):
        return reverse('process-images', kwargs = {
            'id':self.id
        })

    def get_upload_url(self):
        return reverse('upload-view', kwargs = {
            'id': self.id
        })
    def get_download_url(self):
        return reverse('download-image',kwargs ={
            'id' : self.id
        })

    

class Video(models.Model):
    upload = models.FileField(upload_to = 'videos/')
    processed_video = models.FileField(null = True)
    
    def __str__(self):
        return str(self.pk)

class VideoSession(models.Model):
    videos = models.ManyToManyField(Video)
    processed = models.BooleanField(default = False)
    device = models.CharField(max_length=100, null = True)
    def __str__(self):
        return str(self.id)

    def get_process_url(self):
        return reverse('process-videos', kwargs = {
            'id':self.id
    })

    def get_upload_url(self):
        return reverse('upload-video', kwargs = {
            'id': self.id
        })
    def get_download_url(self):
        return reverse('download-image',kwargs ={
            'id' : self.id
        })


