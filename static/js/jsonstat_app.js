import {
  formatFieldType as formatFieldTypeHelper,
  inferQueryType as inferQueryTypeHelper,
  normalizeFieldVariableName as normalizeFieldVariableNameHelper,
  normalizeSelectedQueryFieldKeys as normalizeSelectedQueryFieldKeysHelper,
} from "./app_logic.js?v=20260825-06";

(function () {
  if (!window.Vue) {
    return;
  }

  var JSONSTAT_ONTOLOGY_BASE = "https://sparqlbridge.toolforge.org/ontology/jsonstat/";
  var DATA_CUBE_ONTOLOGY_BASE = "http://purl.org/linked-data/cube#";
  var structurePath = "/jsonstat/api/structure";
  var sparqlBasePath = "/sparql?dataset=jsonstat&";

  function defaultQuery() {
    return [
      "PREFIX qb: <" + DATA_CUBE_ONTOLOGY_BASE + ">",
      "PREFIX jsonstat: <" + JSONSTAT_ONTOLOGY_BASE + ">",
      "SELECT ?observation ?value",
      "WHERE {",
      "  ?observation a qb:Observation .",
      "  OPTIONAL { ?observation jsonstat:value ?value . }",
      "}",
      "LIMIT 50",
    ].join("\n");
  }

  var app = window.Vue.createApp({
    data: function () {
      return {
        sourceUrl: "",
        loadedSourceUrl: "",
        loadedSourceToken: "",
        query: defaultQuery(),
        refreshBeforeQuery: false,
        isBusy: false,
        loadStatusMessage: "Ready.",
        loadStatusLevel: "neutral",
        statusMessage: "Ready.",
        statusLevel: "neutral",
        queryType: "",
        resultFormat: "",
        result: null,
        meta: {},
        selectedQueryFieldKeys: ["value"],
        hasLoadedData: false,
      };
    },
    computed: {
      resultVisible: function () {
        return this.result !== null;
      },
      selectVars: function () {
        return this.queryType === "SELECT" && this.result && this.result.head
          ? this.result.head.vars || []
          : [];
      },
      selectRows: function () {
        return this.queryType === "SELECT" && this.result && this.result.results
          ? this.result.results.bindings || []
          : [];
      },
      askValue: function () {
        return Boolean(this.queryType === "ASK" && this.result && this.result.boolean);
      },
      textResult: function () {
        if (typeof this.result === "string") {
          return this.result;
        }
        return this.result ? JSON.stringify(this.result, null, 2) : "";
      },
      structureFields: function () {
        var structure = this.meta && this.meta.structure;
        return structure && Array.isArray(structure.fields) ? structure.fields : [];
      },
      structureRowCount: function () {
        var structure = this.meta && this.meta.structure;
        return Number((structure && structure.row_count) || this.meta.records || 0);
      },
      canShowStructure: function () {
        return this.structureFields.length > 0;
      },
      querySectionReady: function () {
        return Boolean(this.hasLoadedData && this.loadedSourceToken);
      },
      endpointUrl: function () {
        return this.loadedSourceToken
          ? window.location.origin + sparqlBasePath + "source=" + this.loadedSourceToken
          : "";
      },
    },
    watch: {
      sourceUrl: function () {
        if (String(this.sourceUrl || "").trim() !== this.loadedSourceUrl) {
          this.hasLoadedData = false;
        }
      },
    },
    methods: {
      structureRequest: async function (refresh) {
        var params = new URLSearchParams();
        params.set("url", String(this.sourceUrl || "").trim());
        if (refresh) {
          params.set("refresh", "1");
        }
        var response = await fetch(structurePath + "?" + params.toString(), {
          headers: { Accept: "application/json" },
        });
        var data;
        try {
          data = await response.json();
        } catch (_error) {
          data = { error: "Server returned a non-JSON response." };
        }
        if (!response.ok) {
          throw new Error(data.error || "Request failed with status " + response.status + ".");
        }
        return data;
      },
      loadStructure: async function () {
        this.isBusy = true;
        this.hasLoadedData = false;
        this.loadStatusMessage = "Loading JSON-stat data...";
        this.loadStatusLevel = "neutral";
        try {
          var data = await this.structureRequest(true);
          this.meta = data.meta || {};
          this.loadedSourceUrl = String(data.url || this.sourceUrl || "").trim();
          this.loadedSourceToken = String(data.source_token || "").trim();
          this.sourceUrl = this.loadedSourceUrl;
          this.hasLoadedData = true;
          this.normalizeWizardSelections();
          this.updateQueryFromWizardSelections();
          this.loadStatusMessage =
            "JSON-stat data loaded (" +
            this.structureRowCount +
            " cells, " +
            this.structureFields.length +
            " fields).";
          this.loadStatusLevel = "success";
        } catch (error) {
          this.loadStatusMessage = error && error.message ? error.message : "JSON-stat load failed.";
          this.loadStatusLevel = "error";
        } finally {
          this.isBusy = false;
        }
      },
      runQuery: async function () {
        this.isBusy = true;
        this.statusMessage = "Running SPARQL query...";
        this.statusLevel = "neutral";
        try {
          var servicePath = "source=" + this.loadedSourceToken;
          if (this.refreshBeforeQuery) {
            servicePath += "&refresh=1";
          }
          var response = await fetch(sparqlBasePath + servicePath, {
            method: "POST",
            headers: {
              "Content-Type": "application/sparql-query",
              Accept: "application/sparql-results+json, application/n-triples, text/plain",
            },
            body: String(this.query || ""),
          });
          var contentType = String(response.headers.get("Content-Type") || "").toLowerCase();
          var body = await response.text();
          if (!response.ok) {
            throw new Error(body || "Request failed with status " + response.status + ".");
          }

          this.queryType = inferQueryTypeHelper(this.query) || "CONSTRUCT";
          var returnedRowCount = 0;
          if (contentType.indexOf("application/sparql-results+json") !== -1) {
            this.result = JSON.parse(body);
            if (!inferQueryTypeHelper(this.query)) {
              this.queryType = Object.prototype.hasOwnProperty.call(this.result, "boolean")
                ? "ASK"
                : "SELECT";
            }
            if (
              this.queryType === "SELECT" &&
              this.result &&
              this.result.results &&
              Array.isArray(this.result.results.bindings)
            ) {
              returnedRowCount = this.result.results.bindings.length;
            }
          } else {
            this.result = body;
          }
          this.statusMessage =
            this.queryType === "SELECT"
              ? "Query completed (" + returnedRowCount + " rows returned)."
              : "Query completed.";
          this.statusLevel = "success";
        } catch (error) {
          this.result = null;
          this.queryType = "";
          this.statusMessage = error && error.message ? error.message : "SPARQL query failed.";
          this.statusLevel = "error";
        } finally {
          this.isBusy = false;
        }
      },
      formatFieldType: function (field) {
        return formatFieldTypeHelper(field);
      },
      bindingText: function (binding) {
        return binding && Object.prototype.hasOwnProperty.call(binding, "value") ? binding.value : "";
      },
      isWizardFieldSelected: function (fieldKey) {
        return this.selectedQueryFieldKeys.indexOf(String(fieldKey || "").trim()) !== -1;
      },
      toggleWizardField: function (fieldKey, selected) {
        var key = String(fieldKey || "").trim();
        if (!key) {
          return;
        }
        var next = this.selectedQueryFieldKeys.slice();
        var index = next.indexOf(key);
        if (selected && index === -1) {
          next.push(key);
        } else if (!selected && index !== -1) {
          next.splice(index, 1);
        }
        this.selectedQueryFieldKeys = next;
        this.updateQueryFromWizardSelections();
      },
      selectAllWizardFields: function () {
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
        var normalized = normalizeSelectedQueryFieldKeysHelper(
          this.structureFields,
          this.selectedQueryFieldKeys,
          5,
          ["value"]
        );
        this.selectedQueryFieldKeys = normalized.keys;
      },
      buildWizardQuery: function () {
        var selected = {};
        this.selectedQueryFieldKeys.forEach(function (key) {
          selected[key] = true;
        });
        var selectVariables = ["?observation"];
        var whereLines = ["  ?observation a qb:Observation ."];
        this.structureFields.forEach(function (field) {
          var key = String(field.source_key || "").trim();
          var predicate = String(field.predicate || "").trim();
          if (!key || !predicate || !selected[key]) {
            return;
          }
          var variable = "?" + normalizeFieldVariableNameHelper(key);
          selectVariables.push(variable);
          whereLines.push("  OPTIONAL { ?observation <" + predicate + "> " + variable + " . }");
        });
        return [
          "PREFIX qb: <" + DATA_CUBE_ONTOLOGY_BASE + ">",
          "PREFIX jsonstat: <" + JSONSTAT_ONTOLOGY_BASE + ">",
          "SELECT " + selectVariables.join(" "),
          "WHERE {",
        ].concat(whereLines, ["}", "LIMIT 50"]).join("\n");
      },
      updateQueryFromWizardSelections: function () {
        this.query = this.buildWizardQuery();
      },
    },
  });

  app.config.compilerOptions.delimiters = ["[[", "]]" ];
  app.mount("#app");
})();
