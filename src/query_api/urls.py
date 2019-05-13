from django.urls import path
from . import views

urlpatterns = [
    path('upload/', views.UploadTableView.as_view()),
    path('run-query/', views.RunQueryView.as_view()),
]
