import { buildExampleQueryUrl } from "./example_query_urls.js";

export function applyExampleQueryLinks(root = document) {
  root.querySelectorAll("[data-example-query-source]").forEach(function (link) {
    var href = buildExampleQueryUrl(link.getAttribute("data-example-query-source"));
    if (href) {
      link.setAttribute("href", href);
    }
  });
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", function () {
    applyExampleQueryLinks(document);
  });
} else {
  applyExampleQueryLinks(document);
}
