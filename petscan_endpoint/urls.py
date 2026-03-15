from django.urls import include, path
from django.views.generic import RedirectView

urlpatterns = [
    path("", RedirectView.as_view(url="/petscan/", permanent=False), name="root_redirect"),
    path("petscan/", include("petscan.urls")),
]
