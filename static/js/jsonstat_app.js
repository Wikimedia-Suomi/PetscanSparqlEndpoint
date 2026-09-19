import {
  OPEN_QUERY_TARGETS,
  buildJsonstatDefaultQueryText as buildJsonstatDefaultQueryTextHelper,
  buildJsonstatWizardQuery as buildJsonstatWizardQueryHelper,
  buildNamedServiceParamPath as buildNamedServiceParamPathHelper,
  buildOpenQueryUrl as buildOpenQueryUrlHelper,
  buildPetscanServiceUrl as buildPetscanServiceUrlHelper,
  formatFieldType as formatFieldTypeHelper,
  inferQueryType as inferQueryTypeHelper,
  normalizeSelectedQueryFieldKeys as normalizeSelectedQueryFieldKeysHelper,
} from "./app_logic.js?v=20260919-02";

(function () {
  if (!window.Vue) {
    return;
  }

  var structurePath = "/jsonstat/api/structure";
  var sparqlBasePath = "/sparql?dataset=jsonstat&";

  var app = window.Vue.createApp({
    data: function () {
      return {
        sourceUrl: "",
        loadedSourceUrl: "",
        loadedSourceToken: "",
        query: buildJsonstatDefaultQueryTextHelper(),
        refreshBeforeQuery: false,
        isBusy: false,
        loadStatusMessage: "Ready.",
        loadStatusLevel: "neutral",
        statusMessage: "Ready.",
        statusLevel: "neutral",
        queryType: "",
        result: null,
        meta: {},
        selectedQueryFieldKeys: ["value"],
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
        return this.loadedSourceToken ? this.buildJsonstatServiceUrl(false) : "";
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
          var servicePath = this.buildSparqlServicePath(this.refreshBeforeQuery);
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
      buildSparqlServicePath: function (refresh) {
        return buildNamedServiceParamPathHelper("source", this.loadedSourceToken, [], refresh);
      },
      buildJsonstatServiceUrl: function (refresh) {
        return buildPetscanServiceUrlHelper(
          window.location.origin,
          sparqlBasePath,
          this.buildSparqlServicePath(refresh)
        );
      },
      buildOpenQueryUrl: function (target) {
        return buildOpenQueryUrlHelper(
          target,
          this.query,
          this.buildJsonstatServiceUrl(false)
        );
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
          ["value"]
        );
        this.selectedQueryFieldKeys = normalizedSelection.keys;
        return normalizedSelection.changed;
      },
      buildWizardQuery: function () {
        return buildJsonstatWizardQueryHelper(
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
