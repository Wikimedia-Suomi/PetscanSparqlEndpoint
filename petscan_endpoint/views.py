from django.http import HttpRequest, HttpResponse
from django.shortcuts import render

from jsonstat import service_source as jsonstat_source


def home(request: HttpRequest) -> HttpResponse:
    return render(
        request,
        "home.html",
        {"jsonstat_allowed_source_domains": jsonstat_source.allowed_source_domains()},
    )
