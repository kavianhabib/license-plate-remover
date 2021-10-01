from django.http.response import Http404
from django.shortcuts import redirect, render
from django.views.generic import TemplateView, View
from django.http import HttpResponse, JsonResponse
from .models import Image, Video, ImageSession, VideoSession
# Create your views here.
from lprApp.yolo_object_detection.yolo import process_image
from lprApp.yolo_object_detection.yolo_video import process_video
from lpr.settings import MEDIA_ROOT, BASE_DIR
from io import BytesIO
import zipfile
import os
from django.contrib import messages
import json

class MainView(TemplateView):
    template_name = 'docs/main.html'

class ImageUploadView(View):
    def get(self, *args, **kwargs):
        device = self.request.COOKIES['device']
        # queryset = ImageSession.objects.filter(processed = False)
        # if queryset.exists():
        #     session = queryset[0]
        #     context = {'session': session}
        #     return render(self.request, "image_upload.html", context)
        # else:
        session = ImageSession.objects.filter(processed = False, device = device)
        if session.exists():
            session[0].images.all().delete()
        
        session = ImageSession.objects.create(processed = False, device = device)
        session.save()
        context = {'session': session}
        return render(self.request, "image_upload.html", context)
    def post(self, *args, **kwargs):
        device = self.request.COOKIES['device']
        session = ImageSession.objects.filter( processed = False, device=device)[0]
        if(len(session.images.all()) <= 0):
            context = {
                "session":session
            }
            messages.error(self.request, "Please, select an image")
            context = {'session': session}
            return render(self.request, "image_upload.html", context)
        elif(session.processed == False):
            img = session.images.all()
            for image in img:
                process_image(image)
        
            return render(self.request, 'download.html', {'session':session})
    # def post(self, *args,**kwargs):


    # def post(self, *args, **kwargs):
    #     session = ImageSession.objects.filter(processed = False)[0]
    #     file = self.request.FILE.get('file')
    #     image =Image.objects.create(upload = file)
    #     image.save()

    #     session.images.add(image)
    #     session.save()
    #     return HttpResponse('')





class VideoUploadView(View):
    def get(self, *args, **kwargs):
        device = self.request.COOKIES['device']
        # queryset = ImageSession.objects.filter(processed = False)
        # if queryset.exists():
        #     session = queryset[0]
        #     context = {'session': session}
        #     return render(self.request, "image_upload.html", context)
        # else:
        session = VideoSession.objects.filter(processed = False, device = device)
        if session.exists():
            session[0].videos.all().delete()
        
        session = VideoSession.objects.create(processed = False, device = device)
        session.save()
        context = {'session': session}
        return render(self.request, "video_upload.html", context)
    def post(self, *args, **kwargs):
        device = self.request.COOKIES['device']
        session = VideoSession.objects.filter( processed = False, device=device)[0]
        if(len(session.videos.all()) <= 0):
            context = {
                "session":session
            }
            messages.error(self.request, "Please, select an image")
            context = {'session': session}
            return render(self.request, "video_upload.html", context)
        elif(session.processed == False):
            vid = session.videos.all()
            for video in vid:
                process_video(video)
        
            return render(self.request, 'download_video.html', {'session':session})
       

def file_upload_view(request, id):
    if request.method == 'POST':
        device = request.COOKIES['device']
        session = ImageSession.objects.filter(processed = False, device=device)[0]
        file = request.FILES.get('file')
        image =Image.objects.create(upload = file)
        image.save()

        session.images.add(image)
        session.save()

        return JsonResponse({'hasImage':True})
    return JsonResponse({'post':'false'})

def video_upload_view(request, id):
    if request.method == 'POST':
        device = request.COOKIES['device']
        session = VideoSession.objects.filter(processed = False, device=device)[0]
        file = request.FILES.get('file')

        video =Video.objects.create(upload = file)
        video.save()

        session.videos.add(video)
        session.save()
        return JsonResponse({'hasImage':True})
    return JsonResponse({'post':'false'})

def process_videos(request, id):
    if request.method == 'POST':
        device = request.COOKIES['device']
        session = VideoSession.objects.filter(device=device, processed = False)[0]
        if(len(session.videos.all()) <= 0):
            context = {
                "session":session
            }
            messages.error(request, "Please, select an image")
            redirect("...")
        elif(session.processed == False):
            try:
                img = session.videos.all()
                for image in img:
                    process_video(image)
                return render(request, 'download_video.html', {'session':session})
            except:
                return render(request, 'download_video.html', {'session':session})
    else:
        session = VideoSession.objects.filter( id = id)[0]
        return render(request, 'download_video.html', {'session':session})

def process_images(request, id):
    if request.method == 'POST':
        device = request.COOKIES['device']
        session = ImageSession.objects.filter(device=device, processed = False)[0]
        if(len(session.images.all()) <= 0):
            context = {
                "session":session
            }
            messages.error(request, "Please, select an image")
            redirect("...")
        elif(session.processed == False):
            img = session.images.all()
            for image in img:
                process_image(image)
        
            return render(request, 'download.html', {'session':session})
    else:
        session = ImageSession.objects.filter( id = id)[0]
        return render(request, 'download.html', {'session':session})
        # try:
        # images = session.images.all()
        

        # zip_dir = "FILE"
        # zip_name = "%s.zip" %zip_dir

        # st = BytesIO()
        
        # zip_file = zipfile.ZipFile(st,"w")
        # for image in images:
        #     # print()
        #     file_dir, file_name = os.path.split(image.upload.url)
        #     print(file_name)
        #     zip_path = os.path.join(zip_dir, file_name)
        #     print(zip_path)
        #     zip_file.write(str(BASE_DIR)+image.upload.url, zip_path)
        #     print(zip_file)

        # zip_file.close()
        # print("inside the try --------------------------------------------")
        # response = HttpResponse(st.getvalue(), content_type = 'application/x-zip-compressed')
        # response['Content-Disposition'] = 'inline; filename=' + zip_name
        # return response
        # except:
        #     raise Http404
        # return render(request, 'download_page.html', context)

    
def download_video(request,id):
    if request.method =='POST':
        return render(request, 'download_video.html',{})
    else:
        return render(request, 'download_video.html',{})

def download(request, id):
    print("inside else ------------------------------------")
    if request.method == 'POST':
        try:
            session = ImageSession.objects.filter(processed = False, id = id)[0]
            images = session.images.all()
            zip_dir = "FILE"
            zip_name = "%s.zip" %zip_dir
            st = BytesIO()
            zip_file = zipfile.ZipFile(st,"w")
            for image in images:
                print("checking ----------------------------------------")
                file_dir, file_name = os.path.split(image.upload.url)
                zip_path = os.path.join(zip_dir, file_name)
                zip_file.write(str(BASE_DIR)+image.upload.url, zip_path)

            zip_file.close()
            session.processed = True
            session.save()
            response = HttpResponse(st.getvalue(), content_type = 'application/x-zip-compressed')
            response['Content-Disposition'] = 'inline; filename=' + zip_name
            return response
        except:
            raise Http404
    else:
       
        # try:
        session = ImageSession.objects.filter(id = id)[0]
        images = session.images.all()
    
        zip_dir = "FILE"
        zip_name = "%s.zip" %zip_dir

        st = BytesIO()
        
        zip_file = zipfile.ZipFile(st,"w")
        for image in images:
            print("checking ----------------------------------------")
            file_dir, file_name = os.path.split(image.upload.url)
            zip_path = os.path.join(zip_dir, file_name)
            zip_file.write(str(BASE_DIR)+image.upload.url, zip_path)

        zip_file.close()
        session.processed = True
        session.save()
        response = HttpResponse(st.getvalue(), content_type = 'application/x-zip-compressed')
        response['Content-Disposition'] = 'inline; filename=' + zip_name
        return response
        # except:
        #     raise Http404

    