import {
  OPEN_QUERY_TARGETS,
  buildOpenQueryUrl as buildOpenQueryUrlHelper,
  buildPlacenamesWizardQuery as buildPlacenamesWizardQueryHelper,
  formatFieldType as formatFieldTypeHelper,
  formatUriText as formatUriTextHelper,
  inferQueryType as inferQueryTypeHelper,
  normalizeSelectedQueryFieldKeys as normalizeSelectedQueryFieldKeysHelper,
  safeExternalHref as safeExternalHrefHelper,
} from "./app_logic.js?v=20260825-06";

(function () {
  if (!window.Vue) {
    return;
  }

  var createApp = window.Vue.createApp;
  var placenamesDataset = "saami";
  var placenamesBasePath = "/placenames";
  var placenamesStructurePath = placenamesBasePath + "/api/structure";
  var placenamesSparqlPath = placenamesBasePath + "/sparql/dataset=" + placenamesDataset;
  var defaultPlacenamesSelectedQueryFields = [
    "place",
    "spelling",
    "municipality",
    "wgs84WKT",
  ];

  var app = createApp({
    data: function () {
      return {
        query: buildPlacenamesWizardQueryHelper([], []),
        isBusy: false,
        statusMessage: "Ready.",
        statusLevel: "neutral",
        loadStatusMessage: "Ready.",
        loadStatusLevel: "neutral",
        loadExecutionMs: null,
        queryType: "",
        resultFormat: "",
        result: null,
        resultViewMode: "table",
        queryExecutionMs: null,
        meta: {},
        selectedQueryFieldKeys: defaultPlacenamesSelectedQueryFields.slice(),
        hasLoadedData: false,
        openQueryTarget: "wdqs",
        openQueryTargets: OPEN_QUERY_TARGETS,
      };
    },
    computed: {
      endpointPreview: function () {
        return window.location.origin + placenamesSparqlPath;
      },
      querySectionReady: function () {
        return this.hasLoadedData;
      },
      resultVisible: function () {
        return this.result !== null;
      },
      selectVars: function () {
        if (this.queryType !== "SELECT" || !this.result || !this.result.head) {
          return [];
        }
        return Array.isArray(this.result.head.vars) ? this.result.head.vars : [];
      },
      selectRows: function () {
        if (this.queryType !== "SELECT" || !this.result || !this.result.results) {
          return [];
        }
        return Array.isArray(this.result.results.bindings) ? this.result.results.bindings : [];
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
        return this.formatDurationMs(this.queryExecutionMs);
      },
      loadExecutionLabel: function () {
        return this.formatDurationMs(this.loadExecutionMs);
      },
      activeStructure: function () {
        if (!this.hasLoadedData || !this.meta || typeof this.meta.structure !== "object") {
          return null;
        }
        return this.meta.structure;
      },
      canShowStructure: function () {
        var structure = this.activeStructure;
        return Boolean(structure && Array.isArray(structure.fields) && structure.fields.length);
      },
      structureFields: function () {
        return this.canShowStructure ? this.activeStructure.fields : [];
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
    },
    methods: {
      nowMs: function () {
        if (window.performance && typeof window.performance.now === "function") {
          return window.performance.now();
        }
        return Date.now();
      },
      formatDurationMs: function (value) {
        if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
          return "";
        }
        if (value >= 1000) {
          return (value / 1000).toFixed(2) + " s";
        }
        return value.toFixed(1) + " ms";
      },
      formatNumber: function (value) {
        var number = Number(value || 0);
        return Number.isFinite(number) ? number.toLocaleString() : "0";
      },
      inferQueryType: function (query) {
        return inferQueryTypeHelper(query);
      },
      structureRequest: async function () {
        var params = new URLSearchParams({ dataset: placenamesDataset });
        var response = await fetch(placenamesStructurePath + "?" + params.toString(), {
          method: "GET",
          headers: { Accept: "application/json" },
        });
        var responseReceivedMs = this.nowMs();

        var data;
        try {
          data = await response.json();
        } catch (_error) {
          data = { error: "Server returned a non-JSON response." };
        }
        if (!response.ok) {
          throw new Error(data.error || "Request failed with status " + response.status + ".");
        }
        if (data && typeof data === "object") {
          data._responseReceivedMs = responseReceivedMs;
        }
        return data;
      },
      sparqlRequest: async function (query) {
        var response = await fetch(placenamesSparqlPath, {
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
          var errorMessage = bodyText;
          if (contentType.indexOf("json") !== -1) {
            try {
              var errorPayload = JSON.parse(bodyText);
              errorMessage = errorPayload.error || bodyText;
            } catch (_error) {
              // Preserve the original response text.
            }
          }
          throw new Error(errorMessage || "Request failed with status " + response.status + ".");
        }

        if (contentType.indexOf("json") !== -1) {
          try {
            return {
              resultFormat: "sparql-json",
              sparqlJson: JSON.parse(bodyText),
              responseReceivedMs: responseReceivedMs,
            };
          } catch (_error) {
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
        this.result = null;
        this.isBusy = true;
        this.loadExecutionMs = null;
        this.loadStatusMessage = "Loading the local dataset...";
        this.loadStatusLevel = "neutral";
        var loadStartedMs = this.nowMs();

        try {
          var data = await this.structureRequest();
          var responseReceivedMs =
            typeof data._responseReceivedMs === "number" ? data._responseReceivedMs : this.nowMs();
          this.loadExecutionMs = Math.max(responseReceivedMs - loadStartedMs, 0);
          this.meta = data.meta || {};
          this.hasLoadedData = true;
          this.statusMessage = "Ready to run SPARQL query.";
          this.statusLevel = "neutral";
          this.normalizeWizardSelections();
          this.updateQueryFromWizardSelections();
          this.loadStatusMessage =
            "Data structure loaded (" +
            this.formatNumber(this.structureRowCount) +
            " rows, " +
            this.structureFieldCount +
            " fields" +
            (this.loadExecutionLabel ? ", load time " + this.loadExecutionLabel : "") +
            ").";
          this.loadStatusLevel = "success";
        } catch (error) {
          this.meta = {};
          this.loadExecutionMs = null;
          this.loadStatusMessage = error && error.message ? error.message : "Dataset loading failed.";
          this.loadStatusLevel = "error";
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
        this.result = null;
        this.resultFormat = "";
        this.resultViewMode = "table";
        this.queryExecutionMs = null;
        this.statusMessage = "Running SPARQL query...";
        this.statusLevel = "neutral";
        var queryStartedMs = this.nowMs();

        try {
          this.queryType = this.inferQueryType(this.query);
          var execution = await this.sparqlRequest(this.query);
          this.resultFormat = execution.resultFormat;
          var responseReceivedMs =
            typeof execution.responseReceivedMs === "number"
              ? execution.responseReceivedMs
              : this.nowMs();
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

          if (this.queryType === "SELECT") {
            var rowCount = this.selectRows.length;
            this.statusMessage =
              "Query completed (" +
              rowCount +
              (rowCount === 1 ? " row returned)." : " rows returned).");
          } else if (this.queryType === "ASK") {
            this.statusMessage =
              "Query completed (ASK result: " + (this.askValue ? "true" : "false") + ").";
          } else {
            this.statusMessage = "Query completed.";
          }
          this.statusLevel = "success";
        } catch (error) {
          this.result = null;
          this.resultFormat = "";
          this.queryType = "";
          this.queryExecutionMs = null;
          this.statusMessage = error && error.message ? error.message : "SPARQL query failed.";
          this.statusLevel = "error";
        } finally {
          this.isBusy = false;
        }
      },
      buildPlacenamesServiceUrl: function () {
        return window.location.origin + placenamesSparqlPath;
      },
      buildOpenQueryUrl: function (target) {
        return buildOpenQueryUrlHelper(target, this.query, this.buildPlacenamesServiceUrl());
      },
      openQueryTargetDialog: function () {
        var dialogRef = this.$refs.openQueryDialog;
        var dialog = Array.isArray(dialogRef) ? dialogRef[0] : dialogRef;
        if (!dialog || dialog.open) {
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
        // No-op hook for the shared data-source page structure.
      },
      openFederatedQueryInTarget: function () {
        var target = String(this.openQueryTarget || "").trim();
        if (!target) {
          this.statusMessage = "Choose a target from Open query in.";
          this.statusLevel = "error";
          return;
        }
        var targetUrl = this.buildOpenQueryUrl(target);
        if (!targetUrl) {
          this.statusMessage = "Unsupported Open query in target.";
          this.statusLevel = "error";
          return;
        }
        var opened = window.open(targetUrl, "_blank", "noopener,noreferrer");
        if (!opened) {
          this.statusMessage = "Unable to open new tab. Check browser popup settings.";
          this.statusLevel = "error";
          return;
        }
        this.closeQueryTargetDialog();
      },
      formatCell: function (binding) {
        if (!binding || typeof binding !== "object") {
          return "";
        }
        if (binding.type === "uri") {
          return formatUriTextHelper(binding.value);
        }
        if (binding.type === "bnode") {
          return "_:" + String(binding.value || "");
        }
        if (binding.type === "literal" && binding["xml:lang"]) {
          return String(binding.value || "") + "@" + String(binding["xml:lang"]);
        }
        return String(binding.value || "");
      },
      formatCellHref: function (binding) {
        if (!binding || binding.type !== "uri") {
          return "";
        }
        return safeExternalHrefHelper(binding.value);
      },
      formatFieldType: function (field) {
        return formatFieldTypeHelper(field);
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
        var normalizedSelection = normalizeSelectedQueryFieldKeysHelper(
          this.structureFields,
          this.selectedQueryFieldKeys,
          5,
          defaultPlacenamesSelectedQueryFields
        );
        this.selectedQueryFieldKeys = normalizedSelection.keys;
        return normalizedSelection.changed;
      },
      buildWizardQuery: function () {
        return buildPlacenamesWizardQueryHelper(
          this.structureFields,
          this.selectedQueryFieldKeys
        );
      },
      updateQueryFromWizardSelections: function () {
        this.query = this.buildWizardQuery();
      },
    },
  });

  app.config.compilerOptions.delimiters = ["[[", "]]"];
  app.mount("#app");
})();
