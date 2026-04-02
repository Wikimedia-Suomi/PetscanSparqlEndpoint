from django.urls import path

from . import views

urlpatterns = [
    path("", views.index, name="incubator_index"),
    path("api/structure", views.structure_endpoint, name="incubator_structure_endpoint"),
    path("sparql", views.sparql_endpoint, name="incubator_sparql_root_endpoint"),
    path("sparql/<path:service_params>", views.sparql_endpoint, name="incubator_sparql_endpoint"),
]
