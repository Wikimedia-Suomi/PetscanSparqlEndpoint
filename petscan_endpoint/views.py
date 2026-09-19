from typing import Callable, Dict, TypeVar, cast
from urllib.parse import urlencode

from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt

from incubator import views as incubator_views
from jsonstat import service_source as jsonstat_source
from jsonstat import views as jsonstat_views
from newpages import views as newpages_views
from pagepile import views as pagepile_views
from petscan import views as petscan_views
from placenames import views as placenames_views
from quarry import views as quarry_views

_SparqlView = Callable[..., HttpResponse]
_ViewFunc = TypeVar("_ViewFunc", bound=Callable[..., HttpResponse])
_SPARQL_ENDPOINTS: Dict[str, _SparqlView] = {
    "incubator": incubator_views.sparql_endpoint,
    "jsonstat": jsonstat_views.sparql_endpoint,
    "newpages": newpages_views.sparql_endpoint,
    "pagepile": pagepile_views.sparql_endpoint,
    "petscan": petscan_views.sparql_endpoint,
    "placenames": placenames_views.sparql_endpoint,
    "quarry": quarry_views.sparql_endpoint,
}
_PLACENAMES_DEFAULT_DATASET = "saami"


def _csrf_exempt(view_func: _ViewFunc) -> _ViewFunc:
    return cast(_ViewFunc, csrf_exempt(view_func))


def home(request: HttpRequest) -> HttpResponse:
    return render(
        request,
        "home.html",
        {"jsonstat_allowed_source_domains": jsonstat_source.allowed_source_domains()},
    )


def _add_cors_headers(response: HttpResponse) -> HttpResponse:
    response["Access-Control-Allow-Origin"] = "*"
    response["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    response["Access-Control-Allow-Headers"] = "Content-Type, Accept"
    return response


def _dataset_name(request: HttpRequest) -> str:
    values = request.GET.getlist("dataset")
    if len(values) != 1:
        raise ValueError("Exactly one dataset query parameter is required.")
    dataset = str(values[0]).strip().lower()
    if not dataset:
        raise ValueError("Exactly one dataset query parameter is required.")
    if dataset not in _SPARQL_ENDPOINTS:
        allowed = ", ".join(sorted(_SPARQL_ENDPOINTS))
        raise ValueError("Unsupported dataset. Allowed values: {}.".format(allowed))
    return dataset


def _service_params(request: HttpRequest, dataset: str) -> str:
    pairs: list[tuple[str, str]] = []
    for key in request.GET.keys():
        if key.lower() in {"dataset", "query"}:
            continue
        for value in request.GET.getlist(key):
            pairs.append((key, value))

    if dataset == "placenames":
        placenames_datasets = [
            str(value).strip()
            for value in request.GET.getlist("placenames_dataset")
            if str(value).strip()
        ]
        pairs = [(key, value) for key, value in pairs if key.lower() != "placenames_dataset"]
        if len(placenames_datasets) > 1:
            raise ValueError("At most one placenames_dataset query parameter is allowed.")
        pairs.insert(
            0,
            (
                "dataset",
                placenames_datasets[0] if placenames_datasets else _PLACENAMES_DEFAULT_DATASET,
            ),
        )

    return urlencode(pairs, doseq=True)


@_csrf_exempt
def sparql_endpoint(request: HttpRequest) -> HttpResponse:
    """Dispatch the fixed, WQS-allowlistable SPARQL endpoint to a data source."""

    if request.method == "OPTIONS" and not request.GET.getlist("dataset"):
        return _add_cors_headers(HttpResponse(status=204))

    try:
        dataset = _dataset_name(request)
        service_params = _service_params(request, dataset)
    except ValueError as exc:
        return _add_cors_headers(
            HttpResponse(str(exc), status=400, content_type="text/plain; charset=utf-8")
        )

    return _SPARQL_ENDPOINTS[dataset](request, service_params=service_params)
