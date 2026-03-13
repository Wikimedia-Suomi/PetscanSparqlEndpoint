from django.urls import path

from . import views

urlpatterns = [
    path("", views.index, name="index"),
    path("api/structure", views.structure_endpoint, name="structure_endpoint"),
    path("sparql", views.sparql_endpoint, name="sparql_endpoint"),
]
