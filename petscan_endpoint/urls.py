from django.urls import include, path

from . import views

urlpatterns = [
    path("", views.home, name="root_home"),
    path("petscan/", include("petscan.urls")),
    path("incubator/", include("incubator.urls")),
    path("quarry/", include("quarry.urls")),
]
