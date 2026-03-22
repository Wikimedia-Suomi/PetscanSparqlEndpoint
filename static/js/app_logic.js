export const DEFAULT_SELECTED_QUERY_FIELDS = ["title", "namespace"];
export const PETSCAN_ONTOLOGY_PREFIX = "petscan";
export const PETSCAN_ONTOLOGY_BASE = "https://petscan.wmcloud.org/ontology/";
export const QUARRY_ONTOLOGY_PREFIX = "quarrycol";
export const QUARRY_ONTOLOGY_BASE = "https://quarry.wmcloud.org/ontology/";
export const QUARRY_QUERY_PREFIX = "quarry";
export const QUARRY_QUERY_BASE = "https://quarry.wmcloud.org/query/";

export const OPEN_QUERY_TARGETS = [
  { value: "wdqs", label: "Wikidata Query Service (via Sophox)" },
  { value: "sophox", label: "Sophox" },
  { value: "qlever", label: "QLever endpoint" },
];

export function buildDefaultQueryText(prefixName, ontologyBase, subjectVariableName, extraPrefixEntries) {
  var normalizedPrefix = String(prefixName || "").trim() || PETSCAN_ONTOLOGY_PREFIX;
  var normalizedBase = String(ontologyBase || "").trim() || PETSCAN_ONTOLOGY_BASE;
  var normalizedSubjectVariableName = String(subjectVariableName || "").trim() || "item";
  var subjectVariable = "?" + normalizedSubjectVariableName;
  var prefixLines = [
    "PREFIX " + normalizedPrefix + ": <" + normalizedBase + ">",
  ];
  (Array.isArray(extraPrefixEntries) ? extraPrefixEntries : []).forEach(function (entry) {
    if (!Array.isArray(entry) || entry.length < 2) {
      return;
    }
    var extraPrefixName = String(entry[0] || "").trim();
    var extraPrefixBase = String(entry[1] || "").trim();
    if (!extraPrefixName || !extraPrefixBase) {
      return;
    }
    prefixLines.push("PREFIX " + extraPrefixName + ": <" + extraPrefixBase + ">");
  });
  return prefixLines.concat([
    "SELECT " + subjectVariable + " ?title ?ns",
    "WHERE {",
    "  " + subjectVariable + " a " + normalizedPrefix + ":Page .",
    "  OPTIONAL { " + subjectVariable + " " + normalizedPrefix + ":title ?title }",
    "  OPTIONAL { " + subjectVariable + " " + normalizedPrefix + ":namespace ?ns }",
    "}",
    "LIMIT 50",
  ]).join("\n");
}

export function defaultQueryText() {
  return buildDefaultQueryText(PETSCAN_ONTOLOGY_PREFIX, PETSCAN_ONTOLOGY_BASE);
}

export function parseForwardedPetscanParams(rawValue) {
  var raw = String(rawValue || "").trim();
  if (!raw) {
    return [];
  }

  var normalized = raw.charAt(0) === "?" ? raw.slice(1) : raw;
  var parsed;
  try {
    parsed = new URLSearchParams(normalized);
  } catch (_err) {
    return [];
  }

  var reserved = {
    psid: true,
    format: true,
    query: true,
    refresh: true,
    output_limit: true,
    limit: true,
  };
  var entries = [];

  parsed.forEach(function (value, key) {
    var normalizedKey = String(key || "").trim();
    var normalizedValue = String(value || "").trim();
    if (!normalizedKey || !normalizedValue) {
      return;
    }
    if (reserved[normalizedKey.toLowerCase()]) {
      return;
    }
    entries.push([normalizedKey, normalizedValue]);
  });

  return entries;
}

export function appendOutputLimit(entries, limitValue) {
  var nextEntries = Array.isArray(entries) ? entries.slice() : [];
  var normalizedLimit = String(limitValue || "").trim();
  if (normalizedLimit) {
    nextEntries.push(["output_limit", normalizedLimit]);
  }
  return nextEntries;
}

export function buildNamedServiceParamPath(idParamName, idValue, extraEntries, refresh) {
  var paramName = String(idParamName || "").trim();
  var normalizedIdValue = String(idValue || "").trim();
  var entries = [];

  if (paramName && normalizedIdValue) {
    entries.push([paramName, normalizedIdValue]);
  }
  if (refresh) {
    entries.push(["refresh", "1"]);
  }

  if (Array.isArray(extraEntries)) {
    extraEntries.forEach(function (entry) {
      entries.push([entry[0], entry[1]]);
    });
  }

  if (!entries.length) {
    return "";
  }

  return entries
    .map(function (entry) {
      return encodeURIComponent(entry[0]) + "=" + encodeURIComponent(entry[1]);
    })
    .join("&");
}

export function buildServiceParamPath(psidValue, effectivePetscanParams, refresh) {
  return buildNamedServiceParamPath("psid", psidValue, effectivePetscanParams, refresh);
}

export function buildPetscanServiceUrl(origin, sparqlBasePath, servicePath) {
  if (!servicePath) {
    return String(origin || "") + String(sparqlBasePath || "");
  }
  return String(origin || "") + String(sparqlBasePath || "") + String(servicePath || "");
}

export function buildPetscanQueryUrl(psidValue, effectivePetscanParams) {
  var psid = String(psidValue || "").trim();
  if (!psid) {
    return "https://petscan.wmcloud.org/";
  }

  var params = new URLSearchParams();
  params.set("psid", psid);
  if (Array.isArray(effectivePetscanParams)) {
    effectivePetscanParams.forEach(function (entry) {
      params.append(entry[0], entry[1]);
    });
  }
  return "https://petscan.wmcloud.org/?" + params.toString();
}

export function buildPetscanJsonUrl(psidValue, effectivePetscanParams) {
  var psid = String(psidValue || "").trim();
  if (!psid) {
    return "https://petscan.wmcloud.org/";
  }

  var params = new URLSearchParams();
  params.set("psid", psid);
  params.set("format", "json");
  if (Array.isArray(effectivePetscanParams)) {
    effectivePetscanParams.forEach(function (entry) {
      params.append(entry[0], entry[1]);
    });
  }
  return "https://petscan.wmcloud.org/?" + params.toString();
}

export function buildQuarryQueryUrl(quarryIdValue) {
  var quarryId = String(quarryIdValue || "").trim();
  if (!quarryId) {
    return "https://quarry.wmcloud.org/";
  }
  return "https://quarry.wmcloud.org/query/" + encodeURIComponent(quarryId);
}

export function buildQuarryJsonUrl(qrunIdValue) {
  var qrunId = String(qrunIdValue || "").trim();
  if (!qrunId) {
    return "";
  }
  return "https://quarry.wmcloud.org/run/" + encodeURIComponent(qrunId) + "/output/0/json";
}

export function inferQueryType(query) {
  var remaining = String(query || "").replace(/^\s*#.*$/gm, "");

  while (true) {
    var prefixMatch = remaining.match(/^\s*PREFIX\s+[A-Za-z][A-Za-z0-9._-]*:\s*<[^>]*>/i);
    if (prefixMatch) {
      remaining = remaining.slice(prefixMatch[0].length);
      continue;
    }
    var baseMatch = remaining.match(/^\s*BASE\s*<[^>]*>/i);
    if (baseMatch) {
      remaining = remaining.slice(baseMatch[0].length);
      continue;
    }
    break;
  }

  var formMatch = remaining.match(/^\s*(SELECT|ASK|CONSTRUCT|DESCRIBE)\b/i);
  if (!formMatch) {
    return "";
  }
  return String(formMatch[1]).toUpperCase();
}

export function splitSparqlPrologue(queryText) {
  var remaining = String(queryText || "");
  var prologueLines = [];

  while (true) {
    var prefixMatch = remaining.match(/^\s*PREFIX\s+[A-Za-z][A-Za-z0-9._-]*:\s*<[^>]*>\s*/i);
    if (prefixMatch) {
      prologueLines.push(prefixMatch[0].trim());
      remaining = remaining.slice(prefixMatch[0].length);
      continue;
    }

    var baseMatch = remaining.match(/^\s*BASE\s*<[^>]*>\s*/i);
    if (baseMatch) {
      prologueLines.push(baseMatch[0].trim());
      remaining = remaining.slice(baseMatch[0].length);
      continue;
    }
    break;
  }

  return {
    prologueLines: prologueLines,
    body: remaining.trim(),
  };
}

export function buildFederatedQueryText(serviceUrl, queryText) {
  var normalizedServiceUrl = String(serviceUrl || "");
  if (!normalizedServiceUrl || /\/sparql\/$/.test(normalizedServiceUrl)) {
    return String(queryText || "");
  }

  var split = splitSparqlPrologue(queryText);
  var queryBody = split.body || "SELECT * WHERE { ?item ?p ?o . } LIMIT 50";
  var queryType = inferQueryType(queryBody);
  var lines = split.prologueLines.slice();

  lines.push("SELECT * WHERE {");
  lines.push("  SERVICE <" + normalizedServiceUrl + "> {");
  if (queryType === "SELECT") {
    queryBody.split(/\r?\n/).forEach(function (line) {
      lines.push("    " + line);
    });
  } else {
    lines.push("    # Original query was not SELECT. Adapt this federated template as needed.");
    lines.push("    ?item ?p ?o .");
    queryBody.split(/\r?\n/).forEach(function (line) {
      if (line.trim()) {
        lines.push("    # " + line);
      }
    });
  }
  lines.push("  }");
  lines.push("}");
  lines.push("LIMIT 100");

  return lines.join("\n");
}

export function buildWdqsFederatedQueryViaSophox(serviceUrl, queryText) {
  var normalizedServiceUrl = String(serviceUrl || "");
  if (!normalizedServiceUrl || /\/sparql\/$/.test(normalizedServiceUrl)) {
    return String(queryText || "");
  }

  var split = splitSparqlPrologue(queryText);
  var queryBody = split.body || "SELECT * WHERE { ?item ?p ?o . } LIMIT 50";
  var queryType = inferQueryType(queryBody);
  var lines = split.prologueLines.slice();

  lines.push("SELECT * WHERE {");
  lines.push("  SERVICE <https://sophox.org/sparql> {");
  lines.push("    SERVICE <" + normalizedServiceUrl + "> {");
  if (queryType === "SELECT") {
    queryBody.split(/\r?\n/).forEach(function (line) {
      lines.push("      " + line);
    });
  } else {
    lines.push("      # Original query was not SELECT. Adapt this federated template as needed.");
    lines.push("      ?item ?p ?o .");
    queryBody.split(/\r?\n/).forEach(function (line) {
      if (line.trim()) {
        lines.push("      # " + line);
      }
    });
  }
  lines.push("    }");
  lines.push("  }");
  lines.push("}");
  lines.push("LIMIT 100");

  return lines.join("\n");
}

export function buildOpenQueryUrl(target, queryText, serviceUrl) {
  var encodedQuery = "";
  if (target === "wdqs") {
    encodedQuery = encodeURIComponent(buildWdqsFederatedQueryViaSophox(serviceUrl, queryText));
    return "https://query.wikidata.org/#" + encodedQuery;
  }
  if (target === "sophox") {
    encodedQuery = encodeURIComponent(buildFederatedQueryText(serviceUrl, queryText));
    return "https://sophox.org/#" + encodedQuery;
  }
  if (target === "qlever") {
    encodedQuery = encodeURIComponent(buildFederatedQueryText(serviceUrl, queryText));
    return "https://qlever.wikidata.dbis.rwth-aachen.de/wikidata/?query=" + encodedQuery;
  }
  return "";
}

export function decodeUriComponentSafe(value) {
  try {
    return decodeURIComponent(String(value || ""));
  } catch (_err) {
    return String(value || "");
  }
}

export function formatUriText(uriValue) {
  var value = String(uriValue || "").trim();
  if (!value) {
    return "";
  }

  var commonsEntityMatch = value.match(/^https?:\/\/commons\.wikimedia\.org\/entity\/(M\d+)$/i);
  if (commonsEntityMatch) {
    return "sdc:" + commonsEntityMatch[1];
  }

  var wikidataEntityMatch = value.match(/^https?:\/\/www\.wikidata\.org\/entity\/(Q\d+)$/i);
  if (wikidataEntityMatch) {
    return "wd:" + wikidataEntityMatch[1];
  }

  var wikidataWikiMatch = value.match(/^https?:\/\/www\.wikidata\.org\/wiki\/([^?#]+)$/i);
  if (wikidataWikiMatch) {
    return "d:" + decodeUriComponentSafe(wikidataWikiMatch[1]);
  }

  var wikipediaMatch = value.match(/^https?:\/\/([a-z0-9-]+)\.wikipedia\.org\/wiki\/([^?#]+)$/i);
  if (wikipediaMatch) {
    return "w:" + wikipediaMatch[1].toLowerCase() + ":" + decodeUriComponentSafe(wikipediaMatch[2]);
  }

  var incubatorMatch = value.match(/^https?:\/\/incubator\.wikimedia\.org\/wiki\/([^?#]+)$/i);
  if (incubatorMatch) {
    return "incubator:" + decodeUriComponentSafe(incubatorMatch[1]);
  }

  return value;
}

export function formatFieldType(field) {
  if (!field) {
    return "";
  }

  var normalizeType = function (value) {
    var normalized = String(value || "");
    if (!normalized) {
      return "";
    }
    if (normalized === "string") {
      return "xsd:string";
    }
    if (normalized === "integer") {
      return "xsd:integer";
    }
    if (normalized === "double") {
      return "xsd:double";
    }
    if (normalized === "boolean") {
      return "xsd:boolean";
    }
    return normalized;
  };

  var primary = normalizeType(field.primary_type);
  var observedRaw = Array.isArray(field.observed_types) ? field.observed_types : [];
  var observed = observedRaw.map(normalizeType).filter(Boolean);
  if (!observed.length || (observed.length === 1 && observed[0] === primary)) {
    return primary;
  }
  return primary + " (" + observed.join(", ") + ")";
}

export function normalizeFieldVariableName(fieldKey) {
  var normalized = String(fieldKey || "")
    .trim()
    .replace(/[^A-Za-z0-9_]+/g, "_");
  if (!normalized) {
    return "value";
  }
  if (/^[0-9]/.test(normalized)) {
    return "field_" + normalized;
  }
  return normalized;
}

function arraysEqual(left, right) {
  if (left.length !== right.length) {
    return false;
  }
  for (var index = 0; index < left.length; index += 1) {
    if (left[index] !== right[index]) {
      return false;
    }
  }
  return true;
}

function firstSelectableStructureFieldKeys(structureFields, limit) {
  var normalizedLimit = typeof limit === "number" && Number.isFinite(limit) ? Math.max(0, limit) : 5;
  var next = [];
  (Array.isArray(structureFields) ? structureFields : []).forEach(function (field) {
    if (next.length >= normalizedLimit) {
      return;
    }
    var key = String((field && field.source_key) || "").trim();
    if (!key || next.indexOf(key) !== -1) {
      return;
    }
    next.push(key);
  });
  return next;
}

export function normalizeSelectedQueryFieldKeys(structureFields, selectedQueryFieldKeys, fallbackLimit) {
  var structure = Array.isArray(structureFields) ? structureFields : [];
  var selected = Array.isArray(selectedQueryFieldKeys) ? selectedQueryFieldKeys : [];
  var allowed = {};
  structure.forEach(function (field) {
    var key = String((field && field.source_key) || "").trim();
    if (key) {
      allowed[key] = true;
    }
  });

  var next = [];
  selected.forEach(function (key) {
    var normalized = String(key || "").trim();
    if (!normalized || !allowed[normalized] || next.indexOf(normalized) !== -1) {
      return;
    }
    next.push(normalized);
  });

  var shouldFallbackToFirstFields = arraysEqual(selected, DEFAULT_SELECTED_QUERY_FIELDS);
  var usedFallback = false;
  if (!next.length && shouldFallbackToFirstFields) {
    next = firstSelectableStructureFieldKeys(structure, fallbackLimit);
    usedFallback = next.length > 0;
  }

  return {
    keys: next,
    changed: !arraysEqual(selected, next),
    usedFallback: usedFallback,
  };
}

export function buildWizardQueryWithOntology(
  structureFields,
  selectedQueryFieldKeys,
  prefixName,
  ontologyBase,
  subjectVariableName,
  extraPrefixEntries
) {
  var normalizedStructureFields = Array.isArray(structureFields) ? structureFields : [];
  var normalizedPrefix = String(prefixName || "").trim() || PETSCAN_ONTOLOGY_PREFIX;
  var normalizedBase = String(ontologyBase || "").trim() || PETSCAN_ONTOLOGY_BASE;
  var normalizedSubjectVariableName = String(subjectVariableName || "").trim() || "item";
  var subjectVariable = "?" + normalizedSubjectVariableName;
  var selected = {};
  (Array.isArray(selectedQueryFieldKeys) ? selectedQueryFieldKeys : []).forEach(function (key) {
    selected[key] = true;
  });

  var orderedKeys = normalizedStructureFields
    .map(function (field) {
      return String((field && field.source_key) || "").trim();
    })
    .filter(function (key) {
      return key && selected[key];
    });

  (Array.isArray(selectedQueryFieldKeys) ? selectedQueryFieldKeys : []).forEach(function (key) {
    if (orderedKeys.indexOf(key) === -1) {
      orderedKeys.push(key);
    }
  });

  var selectVars = [];
  var selectSeen = {};
  var pushSelectVar = function (varName) {
    if (!selectSeen[varName]) {
      selectSeen[varName] = true;
      selectVars.push(varName);
    }
  };
  pushSelectVar(subjectVariable);

  var whereLines = ["  " + subjectVariable + " a " + normalizedPrefix + ":Page ."];
  orderedKeys.forEach(function (key) {
    if (key === "gil_link" || key.indexOf("gil_link_") === 0) {
      return;
    }
    var variableName = "?" + normalizeFieldVariableName(key);
    pushSelectVar(variableName);
    whereLines.push(
      "  OPTIONAL { " + subjectVariable + " " + normalizedPrefix + ":" + key + " " + variableName + " . }"
    );
  });

  var selectedGilLinkFields = [
    "gil_link",
    "gil_link_wikidata_id",
    "gil_link_wikidata_entity",
    "gil_link_page_len",
    "gil_link_rev_timestamp",
  ];
  var includeGilLinkBlock = selectedGilLinkFields.some(function (key) {
    return Boolean(selected[key]);
  });
  if (includeGilLinkBlock) {
    whereLines.push("  OPTIONAL {");
    whereLines.push("    " + subjectVariable + " " + normalizedPrefix + ":gil_link ?gil_link .");
    if (selected.gil_link) {
      pushSelectVar("?gil_link");
    }
    if (selected.gil_link_wikidata_id) {
      pushSelectVar("?gil_link_wikidata_id");
      whereLines.push(
        "    OPTIONAL { ?gil_link "
        + normalizedPrefix
        + ":gil_link_wikidata_id ?gil_link_wikidata_id . }"
      );
    }
    if (selected.gil_link_wikidata_entity) {
      pushSelectVar("?gil_link_wikidata_entity");
      whereLines.push(
        "    OPTIONAL { ?gil_link "
        + normalizedPrefix
        + ":gil_link_wikidata_entity ?gil_link_wikidata_entity . }"
      );
    }
    if (selected.gil_link_page_len) {
      pushSelectVar("?gil_link_page_len");
      whereLines.push(
        "    OPTIONAL { ?gil_link " + normalizedPrefix + ":gil_link_page_len ?gil_link_page_len . }"
      );
    }
    if (selected.gil_link_rev_timestamp) {
      pushSelectVar("?gil_link_rev_timestamp");
      whereLines.push(
        "    OPTIONAL { ?gil_link "
        + normalizedPrefix
        + ":gil_link_rev_timestamp ?gil_link_rev_timestamp . }"
      );
    }
    whereLines.push("  }");
  }

  var lines = [
    "PREFIX " + normalizedPrefix + ": <" + normalizedBase + ">",
  ];
  (Array.isArray(extraPrefixEntries) ? extraPrefixEntries : []).forEach(function (entry) {
    if (!Array.isArray(entry) || entry.length < 2) {
      return;
    }
    var extraPrefixName = String(entry[0] || "").trim();
    var extraPrefixBase = String(entry[1] || "").trim();
    if (!extraPrefixName || !extraPrefixBase) {
      return;
    }
    lines.push("PREFIX " + extraPrefixName + ": <" + extraPrefixBase + ">");
  });
  lines.push("SELECT " + selectVars.join(" "));
  lines.push("WHERE {");
  whereLines.forEach(function (line) {
    lines.push(line);
  });
  lines.push("}");
  lines.push("LIMIT 50");
  return lines.join("\n");
}

export function buildWizardQuery(structureFields, selectedQueryFieldKeys) {
  return buildWizardQueryWithOntology(
    structureFields,
    selectedQueryFieldKeys,
    PETSCAN_ONTOLOGY_PREFIX,
    PETSCAN_ONTOLOGY_BASE
  );
}
