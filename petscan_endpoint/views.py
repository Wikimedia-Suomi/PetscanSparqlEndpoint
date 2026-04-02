from django.http import HttpRequest, HttpResponse
from django.shortcuts import render

from petscan_endpoint.example_queries import build_incubator_example_query_url


def home(request: HttpRequest) -> HttpResponse:
    return render(
        request,
        "home.html",
        {
            "incubator_example_query_url": build_incubator_example_query_url(),
        },
    )
