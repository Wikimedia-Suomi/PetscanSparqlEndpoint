from django.urls import path

from . import views

urlpatterns = [
    path("", views.index, name="pagepile_index"),
    path("api/structure", views.structure_endpoint, name="pagepile_structure_endpoint"),
    path("sparql/<path:service_params>", views.sparql_endpoint, name="pagepile_sparql_endpoint"),
]
