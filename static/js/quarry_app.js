import {
  DEFAULT_SELECTED_QUERY_FIELDS,
  OPEN_QUERY_TARGETS,
  QUARRY_ONTOLOGY_BASE,
  QUARRY_ONTOLOGY_PREFIX,
  QUARRY_QUERY_BASE,
  QUARRY_QUERY_PREFIX,
  buildFederatedQueryText as buildFederatedQueryTextHelper,
  buildDefaultQueryText as buildDefaultQueryTextHelper,
  buildNamedServiceParamPath as buildNamedServiceParamPathHelper,
  buildOpenQueryUrl as buildOpenQueryUrlHelper,
  buildPetscanServiceUrl as buildPetscanServiceUrlHelper,
  buildQuarryJsonUrl as buildQuarryJsonUrlHelper,
  buildQuarryQueryUrl as buildQuarryQueryUrlHelper,
  buildWizardQueryWithOntology as buildWizardQueryWithOntologyHelper,
  decodeUriComponentSafe as decodeUriComponentSafeHelper,
  formatFieldType as formatFieldTypeHelper,
  formatUriText as formatUriTextHelper,
  inferQueryType as inferQueryTypeHelper,
  normalizeSelectedQueryFieldKeys as normalizeSelectedQueryFieldKeysHelper,
  normalizeFieldVariableName as normalizeFieldVariableNameHelper,
  safeExternalHref as safeExternalHrefHelper,
  splitSparqlPrologue as splitSparqlPrologueHelper,
  buildWdqsFederatedQueryViaSophox as buildWdqsFederatedQueryViaSophoxHelper,
} from "./app_logic.js";

(function () {
  if (!window.Vue) {
    return;
  }

  var createApp = window.Vue.createApp;
  var hostname = window.location && window.location.hostname ? window.location.hostname.toLowerCase() : "";
  var isLocalDevHost = hostname === "localhost" || hostname === "127.0.0.1";
  var quarryBasePath = "/quarry";
  var quarryStructurePath = quarryBasePath + "/api/structure";
  var quarrySparqlBasePath = quarryBasePath + "/sparql/";
  var defaultQuarryId = isLocalDevHost ? "103479" : "";
  var quarryRowIdVariableName = "quarry_row_id";
  var quarryExtraPrefixEntries = [[QUARRY_QUERY_PREFIX, QUARRY_QUERY_BASE]];

  var app = createApp({
    data: function () {
      return {
        quarryId: defaultQuarryId,
        query: buildDefaultQueryTextHelper(
          QUARRY_ONTOLOGY_PREFIX,
          QUARRY_ONTOLOGY_BASE,
          quarryRowIdVariableName,
          quarryExtraPrefixEntries
        ),
        refreshBeforeQuery: false,
        quarryLimit: "10",
        resolvedQrunId: "",
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
        loadedQuarryId: "",
        selectedQueryFieldKeys: DEFAULT_SELECTED_QUERY_FIELDS.slice(),
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
      loadExecutionLabel: function () {
        return this.formatDurationMs(this.loadExecutionMs);
      },
      quarryLimitValue: function () {
        return String(this.quarryLimit || "").trim();
      },
      effectiveQuarryParams: function () {
        var entries = [];
        if (this.quarryLimitValue) {
          entries.push(["limit", this.quarryLimitValue]);
        }
        return entries;
      },
      serviceParamPath: function () {
        return buildNamedServiceParamPathHelper("quarry_id", this.quarryId, this.effectiveQuarryParams, false);
      },
      endpointPreview: function () {
        var base = window.location.origin + quarrySparqlBasePath;
        if (!this.serviceParamPath) {
          return base + "quarry_id=<quarry_id>";
        }
        return base + this.serviceParamPath;
      },
      quarryQueryUrl: function () {
        return buildQuarryQueryUrlHelper(this.quarryId);
      },
      quarryJsonUrl: function () {
        return buildQuarryJsonUrlHelper(this.resolvedQrunId);
      },
      jsonResultCount: function () {
        var currentQuarryId = String(this.quarryId || "").trim();
        if (!currentQuarryId || this.loadedQuarryId !== currentQuarryId || !this.meta) {
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
        var currentQuarryId = String(this.quarryId || "").trim();
        if (!currentQuarryId || this.loadedQuarryId !== currentQuarryId || !this.meta) {
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
        var currentQuarryId = String(this.quarryId || "").trim();
        return Boolean(this.hasLoadedData && currentQuarryId && this.loadedQuarryId === currentQuarryId);
      },
    },
    watch: {
      quarryId: function () {
        this.hasLoadedData = false;
        this.loadExecutionMs = null;
        this.resolvedQrunId = "";
      },
      quarryLimit: function () {
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
      structureRequest: async function (quarryId, refresh) {
        var params = new URLSearchParams();
        params.set("quarry_id", String(quarryId || "").trim());
        if (refresh) {
          params.set("refresh", "1");
        }
        this.effectiveQuarryParams.forEach(function (entry) {
          params.append(entry[0], entry[1]);
        });

        var response = await fetch(quarryStructurePath + "?" + params.toString(), {
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
      sparqlRequest: async function (quarryId, query, refresh) {
        var servicePath = buildNamedServiceParamPathHelper(
          "quarry_id",
          quarryId,
          this.effectiveQuarryParams,
          refresh
        );

        var response = await fetch(quarrySparqlBasePath + servicePath, {
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
        this.loadStatusMessage = "Loading Quarry data...";
        this.loadStatusLevel = "neutral";
        var loadStartedMs = this.nowMs();

        try {
          var data = await this.structureRequest(this.quarryId, true);
          var responseReceivedMs =
            typeof data._responseReceivedMs === "number" ? data._responseReceivedMs : this.nowMs();
          this.loadExecutionMs = Math.max(responseReceivedMs - loadStartedMs, 0);
          this.meta = data.meta || {};
          this.loadedQuarryId = String(data.quarry_id || this.quarryId || "").trim();
          this.resolvedQrunId = String(data.qrun_id || "").trim();
          this.hasLoadedData = true;
          this.statusMessage = "Ready to run SPARQL query.";
          this.statusLevel = "neutral";
          if (this.normalizeWizardSelections()) {
            this.updateQueryFromWizardSelections();
          }
          var loadTimeLabel = this.formatDurationMs(this.loadExecutionMs);
          this.loadStatusMessage =
            "Quarry data loaded (" +
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
          var execution = await this.sparqlRequest(this.quarryId, this.query, this.refreshBeforeQuery);
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
            var metaData = await this.structureRequest(this.quarryId, false);
            this.meta = metaData.meta || {};
            this.loadedQuarryId = String(metaData.quarry_id || this.quarryId || "").trim();
            this.resolvedQrunId = String(metaData.qrun_id || this.resolvedQrunId || "").trim();
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
      buildSparqlServicePath: function (refresh) {
        return buildNamedServiceParamPathHelper("quarry_id", this.quarryId, this.effectiveQuarryParams, refresh);
      },
      buildQuarryServiceUrl: function (refresh) {
        var servicePath = this.buildSparqlServicePath(refresh);
        return buildPetscanServiceUrlHelper(window.location.origin, quarrySparqlBasePath, servicePath);
      },
      buildFederatedQueryText: function () {
        return buildFederatedQueryTextHelper(this.buildQuarryServiceUrl(this.refreshBeforeQuery), this.query);
      },
      buildWdqsFederatedQueryViaSophox: function () {
        return buildWdqsFederatedQueryViaSophoxHelper(
          this.buildQuarryServiceUrl(this.refreshBeforeQuery),
          this.query
        );
      },
      buildOpenQueryUrl: function (target) {
        return buildOpenQueryUrlHelper(target, this.query, this.buildQuarryServiceUrl(this.refreshBeforeQuery));
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
        var opened = window.open(targetUrl, "_blank", "noopener,noreferrer");
        if (!opened) {
          this.statusMessage = "Unable to open new tab. Check browser popup settings.";
          this.statusLevel = "error";
          return;
        }
        this.closeQueryTargetDialog();
      },
      formatCell: function (fieldKey, binding) {
        if (!binding) {
          return "";
        }

        if (binding.type === "uri") {
          var normalizedFieldKey = String(fieldKey || "").trim();
          var value = String(binding.value || "").trim();
          if (normalizedFieldKey === quarryRowIdVariableName) {
            var quarryRowIdMatch = value.match(/^https?:\/\/quarry\.wmcloud\.org\/query\/(\d+)#(?:row\/)?(\d+)$/i);
            if (quarryRowIdMatch) {
              return quarryRowIdMatch[1] + "#" + quarryRowIdMatch[2];
            }
          }
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
        return buildWizardQueryWithOntologyHelper(
          this.structureFields,
          this.selectedQueryFieldKeys,
          QUARRY_ONTOLOGY_PREFIX,
          QUARRY_ONTOLOGY_BASE,
          quarryRowIdVariableName,
          quarryExtraPrefixEntries
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
