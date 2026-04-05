from django.urls import path

from . import views

urlpatterns = [
    path("", views.index, name="newpages_index"),
    path("api/structure", views.structure_endpoint, name="newpages_structure_endpoint"),
    path("sparql", views.sparql_endpoint, name="newpages_sparql_root_endpoint"),
    path("sparql/<path:service_params>", views.sparql_endpoint, name="newpages_sparql_endpoint"),
]

