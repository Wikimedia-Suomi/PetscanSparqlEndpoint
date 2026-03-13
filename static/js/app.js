(function () {
  if (!window.Vue) {
    return;
  }

  var createApp = window.Vue.createApp;

  var app = createApp({
    data: function () {
      return {
        psid: "43641756",
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
        isBusy: false,
        statusMessage: "Ready.",
        queryType: "",
        resultFormat: "",
        result: null,
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
      endpointPreview: function () {
        var base = window.location.origin + "/sparql";
        if (!this.psid) {
          return base + "?psid=<psid>";
        }
        return base + "?psid=" + encodeURIComponent(this.psid);
      },
      petscanQueryUrl: function () {
        var psid = String(this.psid || "").trim();
        if (!psid) {
          return "https://petscan.wmcloud.org/";
        }
        return "https://petscan.wmcloud.org/?psid=" + encodeURIComponent(psid);
      },
      petscanJsonUrl: function () {
        var psid = String(this.psid || "").trim();
        if (!psid) {
          return "https://petscan.wmcloud.org/";
        }
        return (
          "https://petscan.wmcloud.org/?psid=" +
          encodeURIComponent(psid) +
          "&format=json"
        );
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
        if (byField.id || byField.page_id) {
          lines.push("?item ps:page_id ?page_id .");
        }
        if (byField.gil_link) {
          lines.push("?item ps:gil_link ?gil_link .");
        }
        if (byField.gil_link_wikidata_id) {
          lines.push("?gil_link ps:gil_link_wikidata_id ?gil_link_wikidata_id .");
        }
        if (byField.gil_link_wikidata_entity) {
          lines.push("?gil_link ps:gil_link_wikidata_entity ?gil_link_wikidata_entity .");
        }

        return lines;
      },
    },
    methods: {
      apiPost: async function (path, payload) {
        var response = await fetch(path, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Accept: "application/json",
          },
          body: JSON.stringify(payload),
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
      loadDataset: async function () {
        this.isBusy = true;
        this.statusMessage = "Loading PetScan dataset...";

        try {
          var payload = {
            psid: this.psid,
            refresh: true,
          };
          var data = await this.apiPost("/api/load", payload);
          this.meta = data.meta || {};
          this.loadedPsid = String(data.psid || this.psid || "").trim();
          this.statusMessage = "Dataset loaded (" + (this.meta.records || 0) + " records).";
        } catch (err) {
          this.statusMessage = err.message;
        } finally {
          this.isBusy = false;
        }
      },
      runQuery: async function () {
        this.isBusy = true;
        this.statusMessage = "Running query...";

        try {
          var payload = {
            psid: this.psid,
            query: this.query,
            refresh: this.refreshBeforeQuery,
          };

          var data = await this.apiPost("/api/query", payload);
          this.queryType = data.query_type;
          this.resultFormat = data.result_format;
          this.result = data.result;
          this.meta = data.meta || {};
          this.loadedPsid = String(data.psid || this.psid || "").trim();

          if (this.queryType === "SELECT") {
            this.statusMessage = "Query finished (" + this.selectRows.length + " rows).";
          } else if (this.queryType === "ASK") {
            this.statusMessage = "ASK query finished.";
          } else {
            this.statusMessage = "Graph query finished.";
          }
        } catch (err) {
          this.statusMessage = err.message;
          this.result = null;
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
