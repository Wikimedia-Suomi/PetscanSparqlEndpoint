from django.urls import path

from . import views

urlpatterns = [
    path("", views.index, name="index"),
    path("api/load", views.load_psid, name="load_psid"),
    path("api/query", views.run_query, name="run_query"),
    path("sparql", views.sparql_endpoint, name="sparql_endpoint"),
]
