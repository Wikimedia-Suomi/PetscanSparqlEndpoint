from django.urls import re_path

from . import views

urlpatterns = [
    re_path(r"^$", views.index, name="placenames_index"),
    re_path(r"^api/structure/?$", views.structure_endpoint, name="placenames_structure"),
    re_path(
        r"^sparql/(?P<service_params>.+)$",
        views.sparql_endpoint,
        name="placenames_sparql",
    ),
]
