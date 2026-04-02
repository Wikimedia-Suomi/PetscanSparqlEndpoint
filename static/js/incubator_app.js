import {
  OPEN_QUERY_TARGETS,
  buildIncubatorCategoryUrl as buildIncubatorCategoryUrlHelper,
  buildIncubatorDefaultQueryText as buildIncubatorDefaultQueryTextHelper,
  buildIncubatorWizardQuery as buildIncubatorWizardQueryHelper,
  buildOpenQueryUrl as buildOpenQueryUrlHelper,
  decodeUriComponentSafe as decodeUriComponentSafeHelper,
  formatFieldType as formatFieldTypeHelper,
  formatUriText as formatUriTextHelper,
  inferQueryType as inferQueryTypeHelper,
  normalizeFieldVariableName as normalizeFieldVariableNameHelper,
  normalizeSelectedQueryFieldKeys as normalizeSelectedQueryFieldKeysHelper,
  safeExternalHref as safeExternalHrefHelper,
  splitSparqlPrologue as splitSparqlPrologueHelper,
} from "./app_logic.js";

(function () {
  if (!window.Vue) {
    return;
  }

  var createApp = window.Vue.createApp;
  var incubatorBasePath = "/incubator";
  var incubatorStructurePath = incubatorBasePath + "/api/structure";
  var incubatorSparqlBasePath = incubatorBasePath + "/sparql";
  var incubatorRowIdVariableName = "incubator_page";
  var defaultIncubatorSelectedQueryFields = [
    "wikidata_entity",
    "lang_code",
    "page_label",
    "site_url",
    "wiki_group",
  ];

  var app = createApp({
    data: function () {
      return {
        query: buildIncubatorDefaultQueryTextHelper(incubatorRowIdVariableName),
        refreshBeforeQuery: false,
        incubatorLimit: "10",
        incubatorRecentChangesOnly: false,
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
        loadedFilterSignature: "",
        selectedQueryFieldKeys: defaultIncubatorSelectedQueryFields.slice(),
        hasLoadedData: false,
        openQueryTarget: "wdqs",
        openQueryTargets: OPEN_QUERY_TARGETS,
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
        return this.formatDurationMs(this.queryExecutionMs);
      },
      incubatorLimitValue: function () {
        return String(this.incubatorLimit || "").trim();
      },
      effectiveIncubatorParams: function () {
        var entries = [];
        if (this.incubatorLimitValue) {
          entries.push(["limit", this.incubatorLimitValue]);
        }
        if (this.incubatorRecentChangesOnly) {
          entries.push(["recentchanges_only", "1"]);
        }
        return entries;
      },
      currentFilterSignature: function () {
        return this.effectiveIncubatorParams
          .map(function (entry) {
            return entry[0] + "=" + entry[1];
          })
          .join("&");
      },
      endpointPreview: function () {
        var base = window.location.origin + incubatorSparqlBasePath;
        if (!this.currentFilterSignature) {
          return base;
        }
        return base + "/" + this.currentFilterSignature;
      },
      incubatorCategoryUrl: function () {
        return buildIncubatorCategoryUrlHelper();
      },
      jsonResultCount: function () {
        if (!this.meta || this.loadedFilterSignature !== this.currentFilterSignature) {
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
        if (!this.meta || this.loadedFilterSignature !== this.currentFilterSignature) {
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
        return Boolean(this.hasLoadedData && this.loadedFilterSignature === this.currentFilterSignature);
      },
    },
    watch: {
      incubatorLimit: function () {
        this.hasLoadedData = false;
        this.loadExecutionMs = null;
      },
      incubatorRecentChangesOnly: function () {
        this.hasLoadedData = false;
        this.loadExecutionMs = null;
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
      inferQueryType: function (query) {
        return inferQueryTypeHelper(query);
      },
      structureRequest: async function (refresh) {
        var params = new URLSearchParams();
        if (refresh) {
          params.set("refresh", "1");
        }
        this.effectiveIncubatorParams.forEach(function (entry) {
          params.append(entry[0], entry[1]);
        });

        var requestUrl = incubatorStructurePath;
        if (params.toString()) {
          requestUrl += "?" + params.toString();
        }

        var response = await fetch(requestUrl, {
          method: "GET",
          headers: {
            Accept: "application/json",
          },
        });
        var responseReceivedMs = this.nowMs();

        var data;
        try {
          data = await response.json();
        } catch (_err) {
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
      buildSparqlServicePath: function (refresh) {
        var entries = [];
        if (refresh) {
          entries.push(["refresh", "1"]);
        }
        this.effectiveIncubatorParams.forEach(function (entry) {
          entries.push(entry);
        });
        return entries
          .map(function (entry) {
            return encodeURIComponent(entry[0]) + "=" + encodeURIComponent(entry[1]);
          })
          .join("&");
      },
      buildIncubatorServiceUrl: function (refresh) {
        var servicePath = this.buildSparqlServicePath(refresh);
        if (!servicePath) {
          return window.location.origin + incubatorSparqlBasePath;
        }
        return window.location.origin + incubatorSparqlBasePath + "/" + servicePath;
      },
      sparqlRequest: async function (query, refresh) {
        var servicePath = this.buildSparqlServicePath(refresh);
        var requestUrl = incubatorSparqlBasePath + (servicePath ? "/" + servicePath : "");

        var response = await fetch(requestUrl, {
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
        this.loadExecutionMs = null;
        this.loadStatusMessage = "Loading Incubator data...";
        this.loadStatusLevel = "neutral";
        var loadStartedMs = this.nowMs();

        try {
          var data = await this.structureRequest(true);
          var responseReceivedMs =
            typeof data._responseReceivedMs === "number" ? data._responseReceivedMs : this.nowMs();
          this.loadExecutionMs = Math.max(responseReceivedMs - loadStartedMs, 0);
          this.meta = data.meta || {};
          this.loadedFilterSignature = this.currentFilterSignature;
          this.hasLoadedData = true;
          this.statusMessage = "Ready to run SPARQL query.";
          this.statusLevel = "neutral";
          if (this.normalizeWizardSelections()) {
            this.updateQueryFromWizardSelections();
          }
          var loadTimeLabel = this.formatDurationMs(this.loadExecutionMs);
          this.loadStatusMessage =
            "Incubator data loaded (" +
            this.structureRowCount +
            " rows, " +
            this.structureFieldCount +
            " fields" +
            (loadTimeLabel ? ", load time " + loadTimeLabel : "") +
            ").";
          this.loadStatusLevel = "success";
        } catch (err) {
          this.loadExecutionMs = null;
          this.loadStatusMessage = err.message;
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
        this.queryExecutionMs = null;
        this.statusMessage = "Running SPARQL query...";
        this.statusLevel = "neutral";
        var queryStartedMs = this.nowMs();

        try {
          this.queryType = this.inferQueryType(this.query);
          var execution = await this.sparqlRequest(this.query, this.refreshBeforeQuery);
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

          if (this.queryType === "SELECT") {
            this.statusMessage = "Query completed (" + this.selectRows.length + " rows returned).";
          } else if (this.queryType === "ASK") {
            this.statusMessage = "Query completed (ASK result: " + (this.askValue ? "true" : "false") + ").";
          } else {
            this.statusMessage = "Query completed.";
          }
          this.statusLevel = "success";

          try {
            var metaData = await this.structureRequest(false);
            this.meta = metaData.meta || {};
            this.loadedFilterSignature = this.currentFilterSignature;
            if (this.normalizeWizardSelections()) {
              this.updateQueryFromWizardSelections();
            }
          } catch (_metaErr) {
            // Keep query results even if metadata refresh fails.
          }
        } catch (err) {
          this.result = null;
          this.resultFormat = "";
          this.queryType = "";
          this.queryExecutionMs = null;
          this.statusMessage = err && err.message ? err.message : "SPARQL query failed.";
          this.statusLevel = "error";
        } finally {
          this.isBusy = false;
        }
      },
      splitSparqlPrologue: function (queryText) {
        return splitSparqlPrologueHelper(queryText);
      },
      buildOpenQueryUrl: function (target) {
        return buildOpenQueryUrlHelper(target, this.query, this.buildIncubatorServiceUrl(this.refreshBeforeQuery));
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
          this.statusLevel = "error";
          return;
        }
        var targetUrl = this.buildOpenQueryUrl(target);
        if (!targetUrl) {
          this.statusMessage = "Unsupported Open query in target.";
          this.statusLevel = "error";
          return;
        }
        window.open(targetUrl, "_blank", "noopener");
        this.closeQueryTargetDialog();
      },
      formatCell: function (variableName, binding) {
        if (!binding) {
          return "";
        }
        if (binding.type === "uri") {
          return this.formatUriText(binding.value);
        }
        return String(binding.value || "");
      },
      formatCellHref: function (binding) {
        if (!binding || binding.type !== "uri") {
          return "";
        }
        return safeExternalHrefHelper(binding.value);
      },
      decodeUriComponentSafe: function (value) {
        return decodeUriComponentSafeHelper(value);
      },
      formatUriText: function (uriValue) {
        return formatUriTextHelper(uriValue);
      },
      formatFieldType: function (field) {
        return formatFieldTypeHelper(field);
      },
      normalizeFieldVariableName: function (fieldKey) {
        return normalizeFieldVariableNameHelper(fieldKey);
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
          5
        );
        this.selectedQueryFieldKeys = normalizedSelection.keys;
        return normalizedSelection.changed;
      },
      buildWizardQuery: function () {
        return buildIncubatorWizardQueryHelper(
          this.structureFields,
          this.selectedQueryFieldKeys,
          incubatorRowIdVariableName
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
