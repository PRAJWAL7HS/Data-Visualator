from django.conf.urls import url
from . import views

urlpatterns = [
	url(r'^$',views.index,name='index'),
	url(r'^output/', views.output,name='output'),
	url(r'^output2/', views.output2,name='output2'),
	url(r'^keyerror/', views.keyerror,name='keyerror')
]