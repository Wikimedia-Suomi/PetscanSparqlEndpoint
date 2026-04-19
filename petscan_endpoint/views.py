from django.http import HttpRequest, HttpResponse
from django.shortcuts import render

from petscan_endpoint.example_queries import (
    build_incubator_example_query_url,
    build_newpages_example_query_url,
    build_pagepile_example_query_url,
    build_petscan_example_query_url,
    build_quarry_example_query_url,
)


def home(request: HttpRequest) -> HttpResponse:
    return render(
        request,
        "home.html",
        {
            "incubator_example_query_url": build_incubator_example_query_url(),
            "newpages_example_query_url": build_newpages_example_query_url(),
            "pagepile_example_query_url": build_pagepile_example_query_url(),
            "petscan_example_query_url": build_petscan_example_query_url(),
            "quarry_example_query_url": build_quarry_example_query_url(),
        },
    )
