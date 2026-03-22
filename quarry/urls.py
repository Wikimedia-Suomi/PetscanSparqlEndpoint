from django.urls import path

from . import views

urlpatterns = [
    path("", views.index, name="quarry_index"),
    path("api/structure", views.structure_endpoint, name="quarry_structure_endpoint"),
    path("sparql/<path:service_params>", views.sparql_endpoint, name="quarry_sparql_endpoint"),
]
