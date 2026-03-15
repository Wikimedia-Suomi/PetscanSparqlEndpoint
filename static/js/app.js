(function () {
  if (!window.Vue) {
    return;
  }

  var createApp = window.Vue.createApp;
  var hostname = window.location && window.location.hostname ? window.location.hostname.toLowerCase() : "";
  var isLocalDevHost = hostname === "localhost" || hostname === "127.0.0.1";
  var defaultPsid = isLocalDevHost ? "43641756" : "";

  var app = createApp({
    data: function () {
      return {
        psid: defaultPsid,
        query: [
          "PREFIX ps: <https://petscan.wmcloud.org/ontology/>",
          "SELECT ?item ?title ?ns",
          "WHERE {",
          "  ?item a ps:Page .",
          "  OPTIONAL { ?item ps:title ?title }",
          "  OPTIONAL { ?item ps:namespace ?ns }",
          "}",
          "LIMIT 50",
        ].join("\n"),
        refreshBeforeQuery: false,
        petscanGetParams: "",
        isBusy: false,
        statusMessage: "Ready.",
        queryType: "",
        resultFormat: "",
        result: null,
        queryExecutionMs: null,
        meta: {},
        loadedPsid: "",
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
      serviceParamPath: function () {
        var psid = String(this.psid || "").trim();
        var entries = [];

        if (psid) {
          entries.push(["psid", psid]);
        }
        this.forwardedPetscanParams.forEach(function (entry) {
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
        this.forwardedPetscanParams.forEach(function (entry) {
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
        this.forwardedPetscanParams.forEach(function (entry) {
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
      structurePatterns: function () {
        if (!this.canShowStructure) {
          return [];
        }

        var byField = {};
        this.structureFields.forEach(function (field) {
          byField[field.source_key] = field;
        });

        var lines = [];
        if (byField.gil_link) {
          lines.push("?item ps:gil_link ?gil_link .");
          lines.push("?gil_link ps:gil_link_wikidata_id ?gil_link_wikidata_id .");
          lines.push("?gil_link ps:gil_link_wikidata_entity ?gil_link_wikidata_entity .");
          lines.push("?gil_link ps:gil_link_page_len ?gil_link_page_len .");
          lines.push("?gil_link ps:gil_link_rev_timestamp ?gil_link_rev_timestamp");
          return lines;
        }
        if (byField.id || byField.page_id) {
          lines.push("?item ps:page_id ?page_id .");
        }

        return lines;
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
        this.forwardedPetscanParams.forEach(function (entry) {
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
        this.forwardedPetscanParams.forEach(function (entry) {
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
        this.isBusy = true;
        this.statusMessage = "Loading data structure...";

        try {
          var data = await this.structureRequest(this.psid, this.refreshBeforeQuery);
          this.meta = data.meta || {};
          this.loadedPsid = String(data.psid || this.psid || "").trim();
          this.statusMessage =
            "Data structure loaded (" +
            this.structureRowCount +
            " rows, " +
            this.structureFieldCount +
            " fields).";
        } catch (err) {
          this.statusMessage = err.message;
        } finally {
          this.isBusy = false;
        }
      },
      runQuery: async function () {
        this.isBusy = true;
        this.statusMessage = "Running query...";
        this.queryExecutionMs = null;
        var queryStartedMs = this.nowMs();

        try {
          this.queryType = this.inferQueryType(this.query);
          var execution = await this.sparqlRequest(this.psid, this.query, this.refreshBeforeQuery);
          this.resultFormat = execution.resultFormat;
          var responseReceivedMs =
            typeof execution.responseReceivedMs === "number" ? execution.responseReceivedMs : this.nowMs();
          this.queryExecutionMs = Math.max(responseReceivedMs - queryStartedMs, 0);
          var timingSuffix = this.queryExecutionLabel ? " in " + this.queryExecutionLabel : "";

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
          } catch (_metaErr) {
            // Keep query results even if metadata refresh fails.
          }

          if (this.queryType === "SELECT") {
            this.statusMessage = "Query finished (" + this.selectRows.length + " rows" + timingSuffix + ").";
          } else if (this.queryType === "ASK") {
            this.statusMessage = "ASK query finished" + timingSuffix + ".";
          } else {
            this.statusMessage = "Graph query finished" + timingSuffix + ".";
          }
        } catch (err) {
          this.statusMessage = err.message;
          this.result = null;
          this.queryExecutionMs = null;
        } finally {
          this.isBusy = false;
        }
      },
      formatCell: function (binding) {
        if (!binding) {
          return "";
        }

        if (binding.type === "uri") {
          return binding.value;
        }

        if (binding.type === "bnode") {
          return "_:" + binding.value;
        }

        if (binding.type === "literal") {
          if (binding["xml:lang"]) {
            return binding.value + "@" + binding["xml:lang"];
          }
          if (binding.datatype) {
            return binding.value + "^^" + binding.datatype;
          }
          return binding.value;
        }

        return String(binding.value || "");
      },
      formatFieldType: function (field) {
        if (!field) {
          return "";
        }
        var primary = String(field.primary_type || "");
        var observed = Array.isArray(field.observed_types) ? field.observed_types : [];
        if (!observed.length || (observed.length === 1 && observed[0] === primary)) {
          return primary;
        }
        return primary + " (" + observed.join(", ") + ")";
      },
      openStructureDialog: function () {
        if (!this.canShowStructure) {
          return;
        }
        var dialog = this.$refs.structureDialog;
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
      closeStructureDialog: function () {
        var dialog = this.$refs.structureDialog;
        if (!dialog) {
          return;
        }
        if (typeof dialog.close === "function" && dialog.open) {
          dialog.close();
          return;
        }
        dialog.removeAttribute("open");
      },
      onStructureDialogClose: function () {
        // No-op hook for future state sync. Kept for accessibility event handling.
      },
    },
  });

  app.config.compilerOptions.delimiters = ["[[", "]]"];
  app.mount("#app");
})();
