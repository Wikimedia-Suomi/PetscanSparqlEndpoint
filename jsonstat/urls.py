from django.urls import path

from . import views

urlpatterns = [
    path("", views.index, name="jsonstat_index"),
    path("api/structure", views.structure_endpoint, name="jsonstat_structure_endpoint"),
    path("sparql/<path:service_params>", views.sparql_endpoint, name="jsonstat_sparql_endpoint"),
]
