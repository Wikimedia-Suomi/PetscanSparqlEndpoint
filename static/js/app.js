(function () {
  if (!window.Vue) {
    return;
  }

  var createApp = window.Vue.createApp;
  var hostname = window.location && window.location.hostname ? window.location.hostname.toLowerCase() : "";
  var isLocalDevHost = hostname === "localhost" || hostname === "127.0.0.1";
  var defaultPsid = isLocalDevHost ? "43641756" : "";
  var defaultSelectedQueryFields = ["title", "namespace"];
  var openQueryTargets = [
    { value: "wdqs", label: "Wikidata Query Service (via Sophox)" },
    { value: "sophox", label: "Sophox" },
    { value: "qlever", label: "QLever endpoint" },
  ];

  var app = createApp({
    data: function () {
      return {
        psid: defaultPsid,
        query: [
          "PREFIX petscan: <https://petscan.wmcloud.org/ontology/>",
          "SELECT ?item ?title ?ns",
          "WHERE {",
          "  ?item a petscan:Page .",
          "  OPTIONAL { ?item petscan:title ?title }",
          "  OPTIONAL { ?item petscan:namespace ?ns }",
          "}",
          "LIMIT 50",
        ].join("\n"),
        refreshBeforeQuery: false,
        petscanGetParams: "",
        petscanLimit: "10",
        isBusy: false,
        statusMessage: "Ready.",
        loadStatusMessage: "Ready.",
        queryType: "",
        resultFormat: "",
        result: null,
        resultViewMode: "table",
        queryExecutionMs: null,
        meta: {},
        loadedPsid: "",
        selectedQueryFieldKeys: defaultSelectedQueryFields.slice(),
        hasLoadedData: false,
        openQueryTarget: "wdqs",
        openQueryTargets: openQueryTargets,
      };
    },
    computed: {
      resultVisible: function () {
        return this.result !== null;
      },
      selectVars: function () {
        if (this.queryType !== "SELECT" || !this.result || !this.result.head) {
          return [];
        }
        return this.result.head.vars || [];
      },
      selectRows: function () {
        if (this.queryType !== "SELECT" || !this.result || !this.result.results) {
          return [];
        }
        return this.result.results.bindings || [];
      },
      askValue: function () {
        if (this.queryType !== "ASK" || !this.result) {
          return false;
        }
        return Boolean(this.result.boolean);
      },
      textResult: function () {
        if (typeof this.result === "string") {
          return this.result;
        }
        if (!this.result) {
          return "";
        }
        return JSON.stringify(this.result, null, 2);
      },
      queryExecutionLabel: function () {
        var value = this.queryExecutionMs;
        if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
          return "";
        }
        if (value >= 1000) {
          return (value / 1000).toFixed(2) + " s";
        }
        return value.toFixed(1) + " ms";
      },
      forwardedPetscanParams: function () {
        var raw = String(this.petscanGetParams || "").trim();
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
      },
      petscanLimitValue: function () {
        return String(this.petscanLimit || "").trim();
      },
      effectivePetscanParams: function () {
        var entries = this.forwardedPetscanParams.slice();
        var limitValue = this.petscanLimitValue;
        if (limitValue) {
          entries.push(["output_limit", limitValue]);
        }
        return entries;
      },
      serviceParamPath: function () {
        var psid = String(this.psid || "").trim();
        var entries = [];

        if (psid) {
          entries.push(["psid", psid]);
        }
        this.effectivePetscanParams.forEach(function (entry) {
          entries.push([entry[0], entry[1]]);
        });

        if (!entries.length) {
          return "";
        }

        return entries
          .map(function (entry) {
            return encodeURIComponent(entry[0]) + "=" + encodeURIComponent(entry[1]);
          })
          .join("&");
      },
      endpointPreview: function () {
        var base = window.location.origin + "/sparql/";
        if (!this.serviceParamPath) {
          return base + "psid=<psid>";
        }
        return base + this.serviceParamPath;
      },
      petscanQueryUrl: function () {
        var psid = String(this.psid || "").trim();
        if (!psid) {
          return "https://petscan.wmcloud.org/";
        }

        var params = new URLSearchParams();
        params.set("psid", psid);
        this.effectivePetscanParams.forEach(function (entry) {
          params.append(entry[0], entry[1]);
        });
        return "https://petscan.wmcloud.org/?" + params.toString();
      },
      petscanJsonUrl: function () {
        var psid = String(this.psid || "").trim();
        if (!psid) {
          return "https://petscan.wmcloud.org/";
        }

        var params = new URLSearchParams();
        params.set("psid", psid);
        params.set("format", "json");
        this.effectivePetscanParams.forEach(function (entry) {
          params.append(entry[0], entry[1]);
        });
        return "https://petscan.wmcloud.org/?" + params.toString();
      },
      jsonResultCount: function () {
        var currentPsid = String(this.psid || "").trim();
        if (!currentPsid || this.loadedPsid !== currentPsid || !this.meta) {
          return null;
        }
        var records = this.meta.records;
        if (typeof records === "number" && Number.isFinite(records)) {
          return records;
        }
        if (typeof records === "string" && records.trim() !== "") {
          var parsed = Number(records);
          if (Number.isFinite(parsed)) {
            return parsed;
          }
        }
        return null;
      },
      activeStructure: function () {
        var currentPsid = String(this.psid || "").trim();
        if (!currentPsid || this.loadedPsid !== currentPsid || !this.meta) {
          return null;
        }
        if (!this.meta.structure || typeof this.meta.structure !== "object") {
          return null;
        }
        return this.meta.structure;
      },
      canShowStructure: function () {
        var structure = this.activeStructure;
        return Boolean(structure && Array.isArray(structure.fields) && structure.fields.length);
      },
      structureFields: function () {
        if (!this.canShowStructure) {
          return [];
        }
        return this.activeStructure.fields;
      },
      structureRowCount: function () {
        if (!this.canShowStructure) {
          return 0;
        }
        return Number(this.activeStructure.row_count || this.meta.records || 0);
      },
      structureFieldCount: function () {
        if (!this.canShowStructure) {
          return 0;
        }
        return Number(this.activeStructure.field_count || this.structureFields.length);
      },
      querySectionReady: function () {
        var currentPsid = String(this.psid || "").trim();
        return Boolean(this.hasLoadedData && currentPsid && this.loadedPsid === currentPsid);
      },
    },
    watch: {
      psid: function () {
        this.hasLoadedData = false;
      },
      petscanGetParams: function () {
        this.hasLoadedData = false;
      },
      petscanLimit: function () {
        this.hasLoadedData = false;
      },
    },
    methods: {
      nowMs: function () {
        if (window.performance && typeof window.performance.now === "function") {
          return window.performance.now();
        }
        return Date.now();
      },
      inferQueryType: function (query) {
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
      },
      structureRequest: async function (psid, refresh) {
        var params = new URLSearchParams();
        params.set("psid", String(psid || "").trim());
        if (refresh) {
          params.set("refresh", "1");
        }
        this.effectivePetscanParams.forEach(function (entry) {
          params.append(entry[0], entry[1]);
        });

        var response = await fetch("/api/structure?" + params.toString(), {
          method: "GET",
          headers: {
            Accept: "application/json",
          },
        });

        var data;
        try {
          data = await response.json();
        } catch (_err) {
          data = { error: "Server returned a non-JSON response." };
        }

        if (!response.ok) {
          throw new Error(data.error || "Request failed with status " + response.status + ".");
        }

        return data;
      },
      sparqlRequest: async function (psid, query, refresh) {
        var pathEntries = [];
        var normalizedPsid = String(psid || "").trim();
        if (normalizedPsid) {
          pathEntries.push(["psid", normalizedPsid]);
        }
        if (refresh) {
          pathEntries.push(["refresh", "1"]);
        }
        this.effectivePetscanParams.forEach(function (entry) {
          pathEntries.push([entry[0], entry[1]]);
        });

        var servicePath = pathEntries
          .map(function (entry) {
            return encodeURIComponent(entry[0]) + "=" + encodeURIComponent(entry[1]);
          })
          .join("&");

        var response = await fetch("/sparql/" + servicePath, {
          method: "POST",
          headers: {
            "Content-Type": "application/sparql-query",
            Accept: "application/sparql-results+json, application/n-triples, text/plain",
          },
          body: String(query || ""),
        });
        var responseReceivedMs = this.nowMs();

        var contentType = String(response.headers.get("Content-Type") || "").toLowerCase();
        var bodyText = await response.text();

        if (!response.ok) {
          throw new Error(bodyText || "Request failed with status " + response.status + ".");
        }

        if (contentType.indexOf("application/sparql-results+json") !== -1) {
          try {
            return {
              resultFormat: "sparql-json",
              sparqlJson: JSON.parse(bodyText),
              responseReceivedMs: responseReceivedMs,
            };
          } catch (_err) {
            throw new Error("SPARQL endpoint returned invalid JSON.");
          }
        }

        return {
          resultFormat: "n-triples",
          ntriples: bodyText,
          responseReceivedMs: responseReceivedMs,
        };
      },
      loadStructure: async function () {
        this.hasLoadedData = false;
        this.isBusy = true;
        this.loadStatusMessage = "Loading data structure...";

        try {
          var data = await this.structureRequest(this.psid, this.refreshBeforeQuery);
          this.meta = data.meta || {};
          this.loadedPsid = String(data.psid || this.psid || "").trim();
          this.hasLoadedData = true;
          if (this.normalizeWizardSelections()) {
            this.updateQueryFromWizardSelections();
          }
          this.loadStatusMessage =
            "Data structure loaded (" +
            this.structureRowCount +
            " rows, " +
            this.structureFieldCount +
            " fields).";
        } catch (err) {
          this.loadStatusMessage = err.message;
        } finally {
          this.isBusy = false;
        }
      },
      runQuery: async function () {
        var detailsRef = this.$refs.structureWizardDetails;
        var details = Array.isArray(detailsRef) ? detailsRef[0] : detailsRef;
        if (details && typeof details.open === "boolean") {
          details.open = false;
        }

        this.isBusy = true;
        this.queryExecutionMs = null;
        var queryStartedMs = this.nowMs();

        try {
          this.queryType = this.inferQueryType(this.query);
          var execution = await this.sparqlRequest(this.psid, this.query, this.refreshBeforeQuery);
          this.resultFormat = execution.resultFormat;
          var responseReceivedMs =
            typeof execution.responseReceivedMs === "number" ? execution.responseReceivedMs : this.nowMs();
          this.queryExecutionMs = Math.max(responseReceivedMs - queryStartedMs, 0);

          if (execution.resultFormat === "sparql-json") {
            this.result = execution.sparqlJson;
            if (!this.queryType) {
              this.queryType =
                this.result && Object.prototype.hasOwnProperty.call(this.result, "boolean")
                  ? "ASK"
                  : "SELECT";
            }
          } else {
            this.result = execution.ntriples;
            if (!this.queryType) {
              this.queryType = "CONSTRUCT";
            }
          }

          try {
            var metaData = await this.structureRequest(this.psid, false);
            this.meta = metaData.meta || {};
            this.loadedPsid = String(metaData.psid || this.psid || "").trim();
            if (this.normalizeWizardSelections()) {
              this.updateQueryFromWizardSelections();
            }
          } catch (_metaErr) {
            // Keep query results even if metadata refresh fails.
          }

        } catch (err) {
          this.result = null;
          this.queryExecutionMs = null;
        } finally {
          this.isBusy = false;
        }
      },
      splitSparqlPrologue: function (queryText) {
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
      },
      buildSparqlServicePath: function (refresh) {
        var pathEntries = [];
        var normalizedPsid = String(this.psid || "").trim();
        if (normalizedPsid) {
          pathEntries.push(["psid", normalizedPsid]);
        }
        if (refresh) {
          pathEntries.push(["refresh", "1"]);
        }
        this.effectivePetscanParams.forEach(function (entry) {
          pathEntries.push([entry[0], entry[1]]);
        });
        return pathEntries
          .map(function (entry) {
            return encodeURIComponent(entry[0]) + "=" + encodeURIComponent(entry[1]);
          })
          .join("&");
      },
      buildPetscanServiceUrl: function (refresh) {
        var servicePath = this.buildSparqlServicePath(refresh);
        if (!servicePath) {
          return window.location.origin + "/sparql/";
        }
        return window.location.origin + "/sparql/" + servicePath;
      },
      buildFederatedQueryText: function () {
        var serviceUrl = this.buildPetscanServiceUrl(this.refreshBeforeQuery);
        if (!serviceUrl || /\/sparql\/$/.test(serviceUrl)) {
          return String(this.query || "");
        }

        var split = this.splitSparqlPrologue(this.query);
        var prologueLines = split.prologueLines;
        var queryBody = split.body;
        if (!queryBody) {
          queryBody = "SELECT * WHERE { ?item ?p ?o . } LIMIT 50";
        }

        var queryType = this.inferQueryType(queryBody);
        var lines = [];
        prologueLines.forEach(function (line) {
          lines.push(line);
        });

        lines.push("SELECT * WHERE {");
        lines.push("  SERVICE <" + serviceUrl + "> {");
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
      },
      buildWdqsFederatedQueryViaSophox: function () {
        var serviceUrl = this.buildPetscanServiceUrl(this.refreshBeforeQuery);
        if (!serviceUrl || /\/sparql\/$/.test(serviceUrl)) {
          return String(this.query || "");
        }

        var split = this.splitSparqlPrologue(this.query);
        var prologueLines = split.prologueLines;
        var queryBody = split.body;
        if (!queryBody) {
          queryBody = "SELECT * WHERE { ?item ?p ?o . } LIMIT 50";
        }

        var queryType = this.inferQueryType(queryBody);
        var lines = [];
        prologueLines.forEach(function (line) {
          lines.push(line);
        });

        lines.push("SELECT * WHERE {");
        lines.push("  SERVICE <https://sophox.org/sparql> {");
        lines.push("    SERVICE <" + serviceUrl + "> {");
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
      },
      buildOpenQueryUrl: function (target) {
        var queryText = target === "wdqs" ? this.buildWdqsFederatedQueryViaSophox() : this.buildFederatedQueryText();
        var encodedQuery = encodeURIComponent(queryText);

        if (target === "wdqs") {
          return "https://query.wikidata.org/#" + encodedQuery;
        }
        if (target === "sophox") {
          return "https://sophox.org/#" + encodedQuery;
        }
        if (target === "qlever") {
          return "https://qlever.wikidata.dbis.rwth-aachen.de/wikidata/?query=" + encodedQuery;
        }
        return "";
      },
      openQueryTargetDialog: function () {
        var dialogRef = this.$refs.openQueryDialog;
        var dialog = Array.isArray(dialogRef) ? dialogRef[0] : dialogRef;
        if (!dialog) {
          return;
        }
        if (dialog.open) {
          return;
        }
        if (typeof dialog.showModal === "function") {
          dialog.showModal();
          return;
        }
        dialog.setAttribute("open", "open");
      },
      closeQueryTargetDialog: function () {
        var dialogRef = this.$refs.openQueryDialog;
        var dialog = Array.isArray(dialogRef) ? dialogRef[0] : dialogRef;
        if (!dialog) {
          return;
        }
        if (typeof dialog.close === "function" && dialog.open) {
          dialog.close();
          return;
        }
        dialog.removeAttribute("open");
      },
      onOpenQueryDialogClose: function () {
        // No-op hook for future dialog state sync.
      },
      openFederatedQueryInTarget: function () {
        var target = String(this.openQueryTarget || "").trim();
        if (!target) {
          this.statusMessage = "Choose a target from Open query in.";
          return;
        }
        var targetUrl = this.buildOpenQueryUrl(target);
        if (!targetUrl) {
          this.statusMessage = "Unsupported Open query in target.";
          return;
        }
        var opened = window.open(targetUrl, "_blank", "noopener,noreferrer");
        if (!opened) {
          this.statusMessage = "Unable to open new tab. Check browser popup settings.";
          return;
        }
        this.closeQueryTargetDialog();
      },
      formatCell: function (binding) {
        if (!binding) {
          return "";
        }

        if (binding.type === "uri") {
          return this.formatUriText(binding.value);
        }

        if (binding.type === "bnode") {
          return "_:" + binding.value;
        }

        if (binding.type === "literal") {
          if (binding["xml:lang"]) {
            return binding.value + "@" + binding["xml:lang"];
          }
          return binding.value;
        }

        return String(binding.value || "");
      },
      formatCellHref: function (binding) {
        if (!binding || binding.type !== "uri") {
          return "";
        }
        return String(binding.value || "").trim();
      },
      decodeUriComponentSafe: function (value) {
        try {
          return decodeURIComponent(String(value || ""));
        } catch (_err) {
          return String(value || "");
        }
      },
      formatUriText: function (uriValue) {
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
          return "d:" + this.decodeUriComponentSafe(wikidataWikiMatch[1]);
        }

        var wikipediaMatch = value.match(/^https?:\/\/([a-z0-9-]+)\.wikipedia\.org\/wiki\/([^?#]+)$/i);
        if (wikipediaMatch) {
          var languageCode = wikipediaMatch[1].toLowerCase();
          var wikiTitle = this.decodeUriComponentSafe(wikipediaMatch[2]);
          return "w:" + languageCode + ":" + wikiTitle;
        }

        var incubatorMatch = value.match(/^https?:\/\/incubator\.wikimedia\.org\/wiki\/([^?#]+)$/i);
        if (incubatorMatch) {
          return "incubator:" + this.decodeUriComponentSafe(incubatorMatch[1]);
        }

        return value;
      },
      formatFieldType: function (field) {
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
      },
      normalizeFieldVariableName: function (fieldKey) {
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
      },
      isWizardFieldSelected: function (fieldKey) {
        var key = String(fieldKey || "").trim();
        return this.selectedQueryFieldKeys.indexOf(key) !== -1;
      },
      toggleWizardField: function (fieldKey, isSelected) {
        var key = String(fieldKey || "").trim();
        if (!key) {
          return;
        }
        var next = this.selectedQueryFieldKeys.slice();
        var index = next.indexOf(key);
        if (isSelected && index === -1) {
          next.push(key);
        }
        if (!isSelected && index !== -1) {
          next.splice(index, 1);
        }
        this.selectedQueryFieldKeys = next;
        this.updateQueryFromWizardSelections();
      },
      selectAllWizardFields: function () {
        if (!this.canShowStructure) {
          return;
        }
        this.selectedQueryFieldKeys = this.structureFields.map(function (field) {
          return field.source_key;
        });
        this.updateQueryFromWizardSelections();
      },
      clearWizardSelections: function () {
        this.selectedQueryFieldKeys = [];
        this.updateQueryFromWizardSelections();
      },
      normalizeWizardSelections: function () {
        if (!this.selectedQueryFieldKeys.length) {
          return false;
        }
        var allowed = {};
        this.structureFields.forEach(function (field) {
          var key = String(field.source_key || "").trim();
          if (key) {
            allowed[key] = true;
          }
        });
        var next = [];
        this.selectedQueryFieldKeys.forEach(function (key) {
          var normalized = String(key || "").trim();
          if (!normalized || !allowed[normalized] || next.indexOf(normalized) !== -1) {
            return;
          }
          next.push(normalized);
        });
        var changed = next.length !== this.selectedQueryFieldKeys.length;
        this.selectedQueryFieldKeys = next;
        return changed;
      },
      buildWizardQuery: function () {
        var self = this;
        var selected = {};
        this.selectedQueryFieldKeys.forEach(function (key) {
          selected[key] = true;
        });

        var orderedKeys = this.structureFields
          .map(function (field) {
            return String(field.source_key || "").trim();
          })
          .filter(function (key) {
            return key && selected[key];
          });

        this.selectedQueryFieldKeys.forEach(function (key) {
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
        pushSelectVar("?item");

        var whereLines = ["  ?item a petscan:Page ."];
        orderedKeys.forEach(function (key) {
          if (key === "gil_link" || key.indexOf("gil_link_") === 0) {
            return;
          }
          var variableName = "?" + self.normalizeFieldVariableName(key);
          pushSelectVar(variableName);
          whereLines.push("  OPTIONAL { ?item petscan:" + key + " " + variableName + " . }");
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
          whereLines.push("    ?item petscan:gil_link ?gil_link .");
          if (selected.gil_link) {
            pushSelectVar("?gil_link");
          }
          if (selected.gil_link_wikidata_id) {
            pushSelectVar("?gil_link_wikidata_id");
            whereLines.push(
              "    OPTIONAL { ?gil_link petscan:gil_link_wikidata_id ?gil_link_wikidata_id . }"
            );
          }
          if (selected.gil_link_wikidata_entity) {
            pushSelectVar("?gil_link_wikidata_entity");
            whereLines.push(
              "    OPTIONAL { ?gil_link petscan:gil_link_wikidata_entity ?gil_link_wikidata_entity . }"
            );
          }
          if (selected.gil_link_page_len) {
            pushSelectVar("?gil_link_page_len");
            whereLines.push("    OPTIONAL { ?gil_link petscan:gil_link_page_len ?gil_link_page_len . }");
          }
          if (selected.gil_link_rev_timestamp) {
            pushSelectVar("?gil_link_rev_timestamp");
            whereLines.push(
              "    OPTIONAL { ?gil_link petscan:gil_link_rev_timestamp ?gil_link_rev_timestamp . }"
            );
          }
          whereLines.push("  }");
        }

        var lines = [
          "PREFIX petscan: <https://petscan.wmcloud.org/ontology/>",
          "SELECT " + selectVars.join(" "),
          "WHERE {",
        ];
        whereLines.forEach(function (line) {
          lines.push(line);
        });
        lines.push("}");
        lines.push("LIMIT 50");
        return lines.join("\n");
      },
      updateQueryFromWizardSelections: function () {
        this.query = this.buildWizardQuery();
      },
    },
  });

  app.config.compilerOptions.delimiters = ["[[", "]]"];
  app.mount("#app");
})();
