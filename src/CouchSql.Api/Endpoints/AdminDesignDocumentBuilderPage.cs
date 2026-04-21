using System.Text.Json;
using System.Text.Json.Serialization;
using CouchSql.Core.Design;

namespace CouchSql.Api.Endpoints;

public static class AdminDesignDocumentBuilderPage
{
    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    public static CouchSqlDesignDocument CreateSampleDocument()
    {
        return new CouchSqlDesignDocument
        {
            Id = "_design/couchsql",
            Revision = null,
            CouchSql = new CouchSqlDesignConfiguration
            {
                SchemaVersion = 1,
                Types =
                [
                    new CouchSqlTypeDefinition
                    {
                        Name = "customer",
                        Table = "customers",
                        Identify = ParseJsonElement("""
                        {
                          "all": [
                            { "path": "meta.entity", "equals": "customer" },
                            { "path": "customer.id", "exists": true }
                          ]
                        }
                        """),
                        Fields =
                        [
                            new CouchSqlFieldDefinition
                            {
                                Column = "customer_id",
                                Path = "customer.id",
                                Type = "text",
                                Required = true
                            },
                            new CouchSqlFieldDefinition
                            {
                                Column = "customer_name",
                                Path = "customer.name",
                                Type = "text",
                              Required = false,
                              Transform = new CouchSqlFieldTransformDefinition
                              {
                                Prefix = "Customer: "
                              }
                            }
                        ],
                        Indexes =
                        [
                            new CouchSqlIndexDefinition
                            {
                                Name = "ix_customers_customer_id",
                                Columns = ["customer_id"],
                                Unique = true
                            }
                        ]
                    }
                ]
            }
        };
    }

    public static string BuildHtml(CouchSqlDesignDocument initialDocument)
    {
        var initialJson = JsonSerializer.Serialize(initialDocument, SerializerOptions);

        const string html = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>couchSQL Design Document Builder</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f3efe6;
      --bg-accent: #e6ebe3;
      --panel: rgba(255, 255, 255, 0.88);
      --panel-strong: #ffffff;
      --text: #102018;
      --muted: #5a6b63;
      --line: rgba(16, 32, 24, 0.12);
      --brand: #125c43;
      --brand-strong: #0d4633;
      --brand-soft: #d9eee6;
      --danger: #8a2f2f;
      --danger-soft: #f8e2e2;
      --shadow: 0 20px 60px rgba(14, 28, 22, 0.12);
      --radius: 20px;
    }

    * { box-sizing: border-box; }

    html, body { margin: 0; min-height: 100%; }

    body {
      font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(18, 92, 67, 0.14), transparent 28%),
        radial-gradient(circle at 85% 0%, rgba(119, 155, 95, 0.18), transparent 24%),
        linear-gradient(180deg, var(--bg) 0%, #f7f5ef 48%, #eef3ee 100%);
    }

    .shell {
      width: min(1440px, calc(100vw - 32px));
      margin: 0 auto;
      padding: 32px 0 48px;
    }

    .hero {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 24px;
      align-items: end;
      margin-bottom: 24px;
    }

    .eyebrow {
      margin: 0 0 10px;
      text-transform: uppercase;
      letter-spacing: 0.16em;
      font-size: 0.78rem;
      color: var(--brand-strong);
      font-weight: 800;
    }

    h1 {
      margin: 0;
      font-size: clamp(2rem, 4vw, 3.6rem);
      line-height: 1.02;
      letter-spacing: -0.04em;
    }

    .lede {
      margin: 14px 0 0;
      max-width: 72ch;
      font-size: 1.02rem;
      line-height: 1.65;
      color: var(--muted);
    }

    .hero-actions {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      justify-content: flex-end;
    }

    button,
    .button {
      appearance: none;
      border: 1px solid transparent;
      border-radius: 999px;
      padding: 11px 16px;
      font: inherit;
      font-weight: 700;
      cursor: pointer;
      transition: transform 120ms ease, box-shadow 120ms ease, background 120ms ease, border-color 120ms ease;
      text-decoration: none;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 8px;
      white-space: nowrap;
    }

    button:hover,
    .button:hover {
      transform: translateY(-1px);
    }

    .primary {
      background: linear-gradient(180deg, #1f7a58 0%, var(--brand) 100%);
      color: #fff;
      box-shadow: 0 10px 24px rgba(18, 92, 67, 0.22);
    }

    .secondary {
      background: var(--panel-strong);
      color: var(--text);
      border-color: var(--line);
    }

    .ghost {
      background: transparent;
      color: var(--brand-strong);
      border-color: rgba(18, 92, 67, 0.14);
    }

    .danger {
      background: var(--danger-soft);
      color: var(--danger);
      border-color: rgba(138, 47, 47, 0.16);
    }

    .workspace {
      display: grid;
      grid-template-columns: minmax(0, 1.15fr) minmax(360px, 0.85fr);
      gap: 20px;
      align-items: start;
    }

    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      backdrop-filter: blur(14px);
    }

    .panel-inner {
      padding: 22px;
    }

    .section {
      margin-bottom: 18px;
    }

    .section-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 12px;
    }

    .section-title {
      margin: 0;
      font-size: 1.05rem;
    }

    .section-note {
      margin: 4px 0 0;
      color: var(--muted);
      font-size: 0.92rem;
      line-height: 1.5;
    }

    .grid-2 {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
    }

    .grid-3 {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 12px;
    }

    .field,
    .rule-field {
      display: grid;
      gap: 6px;
    }

    label {
      font-size: 0.9rem;
      font-weight: 700;
      color: var(--brand-strong);
    }

    input,
    select,
    textarea {
      width: 100%;
      border-radius: 14px;
      border: 1px solid rgba(16, 32, 24, 0.14);
      background: rgba(255, 255, 255, 0.92);
      color: var(--text);
      padding: 11px 12px;
      font: inherit;
      transition: border-color 120ms ease, box-shadow 120ms ease;
    }

    textarea {
      min-height: 108px;
      resize: vertical;
      font-family: ui-monospace, SFMono-Regular, Consolas, "Liberation Mono", monospace;
      font-size: 0.92rem;
      line-height: 1.5;
    }

    input:focus,
    select:focus,
    textarea:focus {
      outline: none;
      border-color: rgba(18, 92, 67, 0.56);
      box-shadow: 0 0 0 4px rgba(18, 92, 67, 0.12);
    }

    input:disabled,
    select:disabled,
    textarea:disabled {
      background: rgba(16, 32, 24, 0.06);
      color: rgba(16, 32, 24, 0.45);
      cursor: not-allowed;
    }

    .toolbar {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
    }

    .types {
      display: grid;
      gap: 14px;
    }

    .type-card,
    .inline-card {
      border: 1px solid rgba(16, 32, 24, 0.12);
      border-radius: 18px;
      background: rgba(255, 255, 255, 0.84);
      padding: 16px;
    }

    .type-header,
    .inline-head {
      display: flex;
      justify-content: space-between;
      gap: 10px;
      align-items: center;
      margin-bottom: 14px;
    }

    .type-title {
      margin: 0;
      font-size: 1rem;
    }

    .type-stack {
      display: grid;
      gap: 14px;
    }

    .list {
      display: grid;
      gap: 10px;
    }

    .field-row,
    .index-row {
      display: grid;
      grid-template-columns: minmax(130px, 1fr) minmax(150px, 1.3fr) minmax(130px, 0.85fr) minmax(130px, 0.95fr) minmax(130px, 0.95fr) auto auto;
      gap: 10px;
      align-items: end;
    }

    .index-row {
      grid-template-columns: minmax(140px, 1fr) minmax(180px, 1.6fr) auto auto;
    }

    .checkbox {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 10px 0 8px;
      font-weight: 700;
      color: var(--text);
    }

    .checkbox input {
      width: 18px;
      height: 18px;
      margin: 0;
      accent-color: var(--brand);
    }

    .rule-tree {
      display: grid;
      gap: 10px;
    }

    .rule-node {
      border: 1px solid rgba(16, 32, 24, 0.12);
      border-radius: 16px;
      background: linear-gradient(180deg, rgba(245, 248, 246, 0.92), rgba(255, 255, 255, 0.88));
      padding: 12px;
      display: grid;
      gap: 12px;
    }

    .rule-head {
      display: flex;
      gap: 10px;
      align-items: center;
      justify-content: space-between;
    }

    .rule-kinds {
      min-width: 160px;
    }

    .rule-body {
      display: grid;
      gap: 10px;
    }

    .rule-children {
      display: grid;
      gap: 10px;
      padding-left: 14px;
      border-left: 2px solid rgba(18, 92, 67, 0.18);
    }

    .rule-actions {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
    }

    .preview {
      position: sticky;
      top: 16px;
    }

    .preview pre {
      margin: 0;
      padding: 18px;
      border-radius: 18px;
      background: #0c1914;
      color: #d5f0e7;
      overflow: auto;
      max-height: 68vh;
      font-family: ui-monospace, SFMono-Regular, Consolas, "Liberation Mono", monospace;
      font-size: 0.88rem;
      line-height: 1.55;
      white-space: pre-wrap;
      word-break: break-word;
    }

    .status {
      margin-top: 14px;
      border-radius: 16px;
      padding: 14px 16px;
      border: 1px solid transparent;
      font-size: 0.94rem;
      line-height: 1.5;
    }

    .status.success {
      background: rgba(35, 125, 87, 0.1);
      border-color: rgba(35, 125, 87, 0.18);
      color: #16543b;
    }

    .status.error {
      background: rgba(138, 47, 47, 0.1);
      border-color: rgba(138, 47, 47, 0.18);
      color: #7a2828;
    }

    .status.neutral {
      background: rgba(16, 32, 24, 0.04);
      border-color: rgba(16, 32, 24, 0.08);
      color: var(--muted);
    }

    .footer-note {
      margin-top: 12px;
      font-size: 0.86rem;
      color: var(--muted);
    }

    .paste-panel {
      margin-top: 16px;
      display: grid;
      gap: 10px;
    }

    .paste-panel textarea {
      min-height: 160px;
    }

    @media (max-width: 1120px) {
      .workspace,
      .hero {
        grid-template-columns: 1fr;
      }

      .hero-actions {
        justify-content: flex-start;
      }

      .preview {
        position: static;
      }
    }

    @media (max-width: 820px) {
      .shell { width: min(100vw - 20px, 1440px); padding-top: 18px; }
      .panel-inner { padding: 16px; }
      .grid-2,
      .grid-3,
      .field-row,
      .index-row {
        grid-template-columns: 1fr;
      }

      .type-header,
      .rule-head,
      .section-head {
        align-items: flex-start;
        flex-direction: column;
      }
    }
  </style>
</head>
<body>
  <main class="shell">
    <section class="hero">
      <div>
        <p class="eyebrow">Admin design tool</p>
        <h1>Design Document Builder</h1>
        <p class="lede">Create or load a CouchDB design document through a guided form, preview the generated JSON, and validate it against the server-side contract before you copy or download it.</p>
      </div>
      <div class="hero-actions">
        <button type="button" class="secondary" id="loadSampleButton">Load sample</button>
        <label class="button secondary" for="importFileInput">Load JSON</label>
        <input id="importFileInput" type="file" accept="application/json,.json" hidden />
        <button type="button" class="secondary" id="applyPastedJsonButton">Apply pasted JSON</button>
        <button type="button" class="primary" id="validateButton">Validate</button>
        <button type="button" class="secondary" id="copyButton">Copy JSON</button>
        <button type="button" class="secondary" id="downloadButton">Download JSON</button>
      </div>
    </section>

    <section class="workspace">
      <div class="panel">
        <div class="panel-inner">
          <form id="builderForm" autocomplete="off" novalidate>
            <div class="section">
              <div class="section-head">
                <div>
                  <h2 class="section-title">Document basics</h2>
                  <p class="section-note">The builder keeps the required CouchDB document shape in view while you work.</p>
                </div>
              </div>
              <div class="grid-3">
                <div class="field">
                  <label for="documentId">_id</label>
                  <input id="documentId" name="documentId" value="_design/couchsql" spellcheck="false" />
                </div>
                <div class="field">
                  <label for="documentRevision">_rev</label>
                  <input id="documentRevision" name="documentRevision" placeholder="Optional for new documents" spellcheck="false" />
                </div>
                <div class="field">
                  <label for="schemaVersion">schemaVersion</label>
                  <input id="schemaVersion" name="schemaVersion" type="number" min="1" step="1" value="1" />
                </div>
              </div>
            </div>

            <div class="section">
              <div class="section-head">
                <div>
                  <h2 class="section-title">Types</h2>
                  <p class="section-note">Each type describes one logical record shape, its table, fields, indexes, and identify rule.</p>
                </div>
                <button type="button" class="ghost" id="addTypeButton">Add type</button>
              </div>
              <div class="types" id="typesContainer"></div>
            </div>
          </form>
        </div>
      </div>

      <div class="panel preview">
        <div class="panel-inner">
          <div class="section-head">
            <div>
              <h2 class="section-title">JSON preview</h2>
              <p class="section-note">This is the exact document shape that will be copied, downloaded, or validated.</p>
            </div>
          </div>
          <pre id="jsonPreview"></pre>
          <div class="paste-panel">
            <div>
              <h3 class="section-title">Paste JSON</h3>
              <p class="section-note">Paste a design document copied from CouchDB and apply it directly into the builder.</p>
            </div>
            <textarea id="pastedJsonInput" spellcheck="false" placeholder='{\n  "_id": "_design/couchsql",\n  "couchsql": { ... }\n}'></textarea>
          </div>
          <div id="statusBox" class="status neutral">Ready. Edit the form or load a document to begin.</div>
          <div class="footer-note">Validation uses the server contract, so the page stays aligned with the backend rules.</div>
        </div>
      </div>
    </section>
  </main>

  <script>
    const sampleDocument = __INITIAL_DOCUMENT__;
    const validateUrl = "/internal/v1/design-documents/validate";
    const templateUrl = "/internal/v1/design-documents/template";

    const documentIdInput = document.getElementById("documentId");
    const documentRevisionInput = document.getElementById("documentRevision");
    const schemaVersionInput = document.getElementById("schemaVersion");
    const typesContainer = document.getElementById("typesContainer");
    const previewElement = document.getElementById("jsonPreview");
    const statusBox = document.getElementById("statusBox");
    const loadSampleButton = document.getElementById("loadSampleButton");
    const importFileInput = document.getElementById("importFileInput");
    const applyPastedJsonButton = document.getElementById("applyPastedJsonButton");
    const pastedJsonInput = document.getElementById("pastedJsonInput");
    const validateButton = document.getElementById("validateButton");
    const copyButton = document.getElementById("copyButton");
    const downloadButton = document.getElementById("downloadButton");
    const addTypeButton = document.getElementById("addTypeButton");

    const supportedTypes = ["text", "integer", "bigint", "numeric", "boolean", "date", "timestamp", "timestamptz", "jsonb", "uuid", "double precision"];

    function setStatus(message, kind = "neutral") {
      statusBox.className = `status ${kind}`;
      statusBox.textContent = message;
    }

    function clearChildren(element) {
      while (element.firstChild) {
        element.removeChild(element.firstChild);
      }
    }

    function safeJsonParse(text, context) {
      const trimmed = text.trim();
      if (!trimmed) {
        throw new Error(`${context} must not be empty.`);
      }

      return JSON.parse(trimmed);
    }

    function stringifyJson(value) {
      return JSON.stringify(value, null, 2) ?? "";
    }

    function createOption(label, value) {
      const option = document.createElement("option");
      option.value = value;
      option.textContent = label;
      return option;
    }

    function createRuleNode(rule = null) {
      const wrapper = document.createElement("div");
      wrapper.className = "rule-node";
      wrapper.innerHTML = `
        <div class="rule-head">
          <label class="field" style="flex: 1;">
            <span>Rule kind</span>
            <select class="rule-kind rule-kinds">
              <option value="all">all</option>
              <option value="any">any</option>
              <option value="equals">equals</option>
              <option value="exists">exists</option>
              <option value="contains">contains</option>
            </select>
          </label>
          <button type="button" class="danger remove-rule">Remove</button>
        </div>
        <div class="rule-body">
          <div class="grid-2">
            <label class="rule-field rule-path-field">
              <span>Path</span>
              <input class="rule-path" spellcheck="false" placeholder="meta.entity" />
            </label>
            <label class="rule-field rule-expected-field">
              <span>Expected JSON</span>
              <textarea class="rule-expected" spellcheck="false" placeholder='"customer"'></textarea>
            </label>
          </div>
          <div class="rule-actions">
            <button type="button" class="ghost add-child-rule">Add child rule</button>
          </div>
          <div class="rule-children"></div>
        </div>
      `;

      const kindSelect = wrapper.querySelector(".rule-kind");
      const pathField = wrapper.querySelector(".rule-path-field");
      const expectedField = wrapper.querySelector(".rule-expected-field");
      const pathInput = wrapper.querySelector(".rule-path");
      const expectedInput = wrapper.querySelector(".rule-expected");
      const children = wrapper.querySelector(".rule-children");
      const addChildButton = wrapper.querySelector(".add-child-rule");
      const removeButton = wrapper.querySelector(".remove-rule");

      function syncVisibility() {
        const kind = kindSelect.value;
        const grouped = kind === "all" || kind === "any";

        pathField.hidden = grouped;
        expectedField.hidden = grouped || kind === "exists";
        addChildButton.hidden = !grouped;
        children.hidden = !grouped;
      }

      kindSelect.addEventListener("change", () => {
        syncVisibility();
        updatePreview();
      });

      pathInput.addEventListener("input", updatePreview);
      expectedInput.addEventListener("input", updatePreview);

      addChildButton.addEventListener("click", () => {
        children.appendChild(createRuleNode());
        syncVisibility();
        updatePreview();
      });

      removeButton.addEventListener("click", () => {
        wrapper.remove();
        updatePreview();
      });

      if (rule) {
        if (Object.prototype.hasOwnProperty.call(rule, "all")) {
          kindSelect.value = "all";
          rule.all.forEach(childRule => children.appendChild(createRuleNode(childRule)));
        } else if (Object.prototype.hasOwnProperty.call(rule, "any")) {
          kindSelect.value = "any";
          rule.any.forEach(childRule => children.appendChild(createRuleNode(childRule)));
        } else if (Object.prototype.hasOwnProperty.call(rule, "exists")) {
          kindSelect.value = "exists";
          pathInput.value = rule.path ?? "";
        } else if (Object.prototype.hasOwnProperty.call(rule, "contains")) {
          kindSelect.value = "contains";
          pathInput.value = rule.path ?? "";
          expectedInput.value = stringifyJson(rule.contains);
        } else {
          kindSelect.value = "equals";
          pathInput.value = rule.path ?? "";
          expectedInput.value = stringifyJson(rule.equals);
        }
      }

      syncVisibility();
      return wrapper;
    }

    function createFieldRow(field = {}) {
      const row = document.createElement("div");
      row.className = "field-row";
      row.innerHTML = `
        <label class="field">
          <span>Column</span>
          <input class="field-column" spellcheck="false" placeholder="customer_id" />
        </label>
        <label class="field">
          <span>Path</span>
          <input class="field-path" spellcheck="false" placeholder="customer.id" />
        </label>
        <label class="field">
          <span>Type</span>
          <select class="field-type"></select>
        </label>
        <label class="field">
          <span>Prefix</span>
          <input class="field-transform-prefix" spellcheck="false" placeholder="Customer: " />
        </label>
        <label class="field">
          <span>Append</span>
          <input class="field-transform-append" spellcheck="false" placeholder=" Ltd." />
        </label>
        <label class="checkbox">
          <input class="field-required" type="checkbox" />
          <span>Required</span>
        </label>
        <button type="button" class="danger remove-field">Remove</button>
      `;

      const columnInput = row.querySelector(".field-column");
      const pathInput = row.querySelector(".field-path");
      const typeSelect = row.querySelector(".field-type");
      const prefixInput = row.querySelector(".field-transform-prefix");
      const appendInput = row.querySelector(".field-transform-append");
      const requiredInput = row.querySelector(".field-required");

      supportedTypes.forEach(typeName => typeSelect.appendChild(createOption(typeName, typeName)));
      columnInput.value = field.column ?? "";
      pathInput.value = field.path ?? "";
      typeSelect.value = field.type ?? "text";
      prefixInput.value = field.transform?.prefix ?? "";
      appendInput.value = field.transform?.append ?? "";
      requiredInput.checked = Boolean(field.required);

      function syncTransformInputs() {
        const isText = typeSelect.value === "text";
        prefixInput.disabled = !isText;
        appendInput.disabled = !isText;

        if (!isText) {
          prefixInput.value = "";
          appendInput.value = "";
        }
      }

      typeSelect.addEventListener("change", () => {
        syncTransformInputs();
        updatePreview();
      });

      syncTransformInputs();

      row.querySelectorAll("input, select").forEach(element => {
        element.addEventListener("input", updatePreview);
        element.addEventListener("change", updatePreview);
      });

      row.querySelector(".remove-field").addEventListener("click", () => {
        row.remove();
        updatePreview();
      });

      return row;
    }

    function createIndexRow(index = {}) {
      const row = document.createElement("div");
      row.className = "index-row";
      row.innerHTML = `
        <label class="field">
          <span>Name</span>
          <input class="index-name" spellcheck="false" placeholder="ix_customers_customer_id" />
        </label>
        <label class="field">
          <span>Columns</span>
          <input class="index-columns" spellcheck="false" placeholder="customer_id, created_at" />
        </label>
        <label class="checkbox">
          <input class="index-unique" type="checkbox" />
          <span>Unique</span>
        </label>
        <button type="button" class="danger remove-index">Remove</button>
      `;

      row.querySelector(".index-name").value = index.name ?? "";
      row.querySelector(".index-columns").value = Array.isArray(index.columns) ? index.columns.join(", ") : "";
      row.querySelector(".index-unique").checked = Boolean(index.unique);

      row.querySelectorAll("input").forEach(element => {
        element.addEventListener("input", updatePreview);
        element.addEventListener("change", updatePreview);
      });

      row.querySelector(".remove-index").addEventListener("click", () => {
        row.remove();
        updatePreview();
      });

      return row;
    }

    function createTypeCard(type = null) {
      const card = document.createElement("article");
      card.className = "type-card";
      card.innerHTML = `
        <div class="type-header">
          <h3 class="type-title">Type</h3>
          <button type="button" class="danger remove-type">Remove type</button>
        </div>

        <div class="type-stack">
          <div class="grid-2">
            <label class="field">
              <span>Logical name</span>
              <input class="type-name" spellcheck="false" placeholder="customer" />
            </label>
            <label class="field">
              <span>Table</span>
              <input class="type-table" spellcheck="false" placeholder="customers" />
            </label>
          </div>

          <div class="section">
            <div class="section-head">
                <div>
                  <h4 class="section-title">Identify rule</h4>
                  <p class="section-note">Compose all/any groups or leaf rules without hand-writing the JSON tree.</p>
                </div>
              </div>
            <div class="rule-tree identify-rules"></div>
          </div>

          <div class="section">
            <div class="section-head">
              <div>
                <h4 class="section-title">Fields</h4>
                <p class="section-note">Map the document paths to PostgreSQL columns and types.</p>
              </div>
              <button type="button" class="ghost add-field">Add field</button>
            </div>
            <div class="list fields"></div>
          </div>

          <div class="section">
            <div class="section-head">
              <div>
                <h4 class="section-title">Indexes</h4>
                <p class="section-note">Optional indexes keep common lookups fast and predictable.</p>
              </div>
              <button type="button" class="ghost add-index">Add index</button>
            </div>
            <div class="list indexes"></div>
          </div>
        </div>
      `;

      const nameInput = card.querySelector(".type-name");
      const tableInput = card.querySelector(".type-table");
      const identifyRules = card.querySelector(".identify-rules");
      const fieldsContainer = card.querySelector(".fields");
      const indexesContainer = card.querySelector(".indexes");

      function seedFields(typeData) {
        const fields = Array.isArray(typeData?.fields) ? typeData.fields : [];
        if (fields.length === 0) {
          fieldsContainer.appendChild(createFieldRow());
          return;
        }

        fields.forEach(field => fieldsContainer.appendChild(createFieldRow(field)));
      }

      function seedIndexes(typeData) {
        const indexes = Array.isArray(typeData?.indexes) ? typeData.indexes : [];
        indexes.forEach(index => indexesContainer.appendChild(createIndexRow(index)));
      }

      function seedIdentify(typeData) {
        if (typeData?.identify) {
          identifyRules.appendChild(createRuleNode(typeData.identify));
          return;
        }

        identifyRules.appendChild(createRuleNode());
      }

      nameInput.value = type?.name ?? "";
      tableInput.value = type?.table ?? "";

      nameInput.addEventListener("input", updatePreview);
      tableInput.addEventListener("input", updatePreview);

      card.querySelector(".remove-type").addEventListener("click", () => {
        card.remove();
        updatePreview();
      });

      card.querySelector(".add-field").addEventListener("click", () => {
        fieldsContainer.appendChild(createFieldRow());
        updatePreview();
      });

      card.querySelector(".add-index").addEventListener("click", () => {
        indexesContainer.appendChild(createIndexRow());
        updatePreview();
      });

      seedIdentify(type);
      seedFields(type);
      seedIndexes(type);

      return card;
    }

    function collectRule(node) {
      const kind = node.querySelector(".rule-kind").value;

      if (kind === "all" || kind === "any") {
        const children = Array.from(node.querySelector(".rule-children").children).map(collectRule);
        if (children.length === 0) {
          throw new Error(`Identify rule '${kind}' requires at least one child rule.`);
        }

        return { [kind]: children };
      }

      const path = node.querySelector(".rule-path").value.trim();
      if (!path) {
        throw new Error("Identify rule path cannot be empty.");
      }

      if (kind === "exists") {
        return { path, exists: true };
      }

      const expected = safeJsonParse(node.querySelector(".rule-expected").value, "Identify rule expected JSON");
      return { path, [kind]: expected };
    }

    function collectDocument() {
      const document = {
        _id: documentIdInput.value.trim(),
        couchsql: {
          schemaVersion: Number(schemaVersionInput.value),
          types: []
        }
      };

      const revision = documentRevisionInput.value.trim();
      if (revision) {
        document._rev = revision;
      }

      if (!document._id) {
        throw new Error("The document _id is required.");
      }

      if (!Number.isInteger(document.couchsql.schemaVersion) || document.couchsql.schemaVersion < 1) {
        throw new Error("schemaVersion must be a positive integer.");
      }

      const typeCards = Array.from(typesContainer.children);
      if (typeCards.length === 0) {
        throw new Error("At least one type is required.");
      }

      document.couchsql.types = typeCards.map(card => {
        const fields = Array.from(card.querySelector(".fields").children).map(fieldRow => ({
          column: fieldRow.querySelector(".field-column").value.trim(),
          path: fieldRow.querySelector(".field-path").value.trim(),
          type: fieldRow.querySelector(".field-type").value,
          required: fieldRow.querySelector(".field-required").checked,
          transform: (() => {
            const prefix = fieldRow.querySelector(".field-transform-prefix").value;
            const append = fieldRow.querySelector(".field-transform-append").value;
            return prefix || append ? { prefix: prefix || undefined, append: append || undefined } : undefined;
          })()
        }));

        const indexes = Array.from(card.querySelector(".indexes").children).map(indexRow => {
          const columns = indexRow.querySelector(".index-columns").value
            .split(",")
            .map(column => column.trim())
            .filter(Boolean);

          return {
            name: indexRow.querySelector(".index-name").value.trim(),
            columns,
            unique: indexRow.querySelector(".index-unique").checked
          };
        });

        const identifyRoot = card.querySelector(".identify-rules").firstElementChild;
        if (!identifyRoot) {
          throw new Error("Each type requires at least one identify rule.");
        }

        return {
          name: card.querySelector(".type-name").value.trim(),
          table: card.querySelector(".type-table").value.trim(),
          identify: collectRule(identifyRoot),
          fields,
          indexes
        };
      });

      return document;
    }

    function updatePreview() {
      try {
        const document = collectDocument();
        const json = JSON.stringify(document, null, 2);
        previewElement.textContent = json;
        setStatus("The document is structurally complete. Use Validate to check it against the backend rules.", "success");
        return json;
      } catch (error) {
        previewElement.textContent = `Unable to generate preview: ${error.message}`;
        setStatus(error.message, "error");
        return null;
      }
    }

    function loadDocument(document) {
      const normalized = document ?? {};
      documentIdInput.value = normalized._id ?? "_design/couchsql";
      documentRevisionInput.value = normalized._rev ?? "";
      schemaVersionInput.value = normalized.couchsql?.schemaVersion ?? 1;
      pastedJsonInput.value = JSON.stringify(normalized, null, 2);

      clearChildren(typesContainer);
      const types = Array.isArray(normalized.couchsql?.types) && normalized.couchsql.types.length > 0
        ? normalized.couchsql.types
        : [{}];

      types.forEach(type => typesContainer.appendChild(createTypeCard(type)));
      updatePreview();
    }

    async function validateDocument() {
      const payload = collectDocument();
      const response = await fetch(validateUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });

      const result = await response.json();
      if (!response.ok) {
        throw new Error(result.error || "Validation failed.");
      }

      return result;
    }

    function downloadJson(json) {
      const blob = new Blob([json], { type: "application/json;charset=utf-8" });
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = "couchsql-design-document.json";
      document.body.appendChild(anchor);
      anchor.click();
      anchor.remove();
      URL.revokeObjectURL(url);
    }

    function applyPastedJson() {
      const text = pastedJsonInput.value.trim();
      if (!text) {
        throw new Error("Paste JSON is empty.");
      }

      loadDocument(JSON.parse(text));
    }

    loadSampleButton.addEventListener("click", async () => {
      try {
        const response = await fetch(templateUrl);
        if (!response.ok) {
          throw new Error(`Server returned ${response.status}.`);
        }

        const template = await response.json();
        loadDocument(template);
        setStatus("Sample document loaded from the server.", "success");
      } catch (error) {
        loadDocument(sampleDocument);
        setStatus(`Loaded the embedded sample because the server template could not be fetched: ${error.message}`, "error");
      }
    });

    importFileInput.addEventListener("change", async () => {
      const file = importFileInput.files?.[0];
      if (!file) {
        return;
      }

      try {
        const text = await file.text();
        const parsed = JSON.parse(text);
        loadDocument(parsed);
        setStatus(`Loaded ${file.name}.`, "success");
      } catch (error) {
        setStatus(`Unable to load file: ${error.message}`, "error");
      } finally {
        importFileInput.value = "";
      }
    });

    applyPastedJsonButton.addEventListener("click", () => {
      try {
        applyPastedJson();
        setStatus("Pasted JSON applied to the builder.", "success");
      } catch (error) {
        setStatus(`Unable to apply pasted JSON: ${error.message}`, "error");
      }
    });

    validateButton.addEventListener("click", async () => {
      try {
        validateButton.disabled = true;
        setStatus("Validating against the server contract...", "neutral");
        await validateDocument();
        setStatus("Validation succeeded.", "success");
      } catch (error) {
        setStatus(error.message, "error");
      } finally {
        validateButton.disabled = false;
      }
    });

    copyButton.addEventListener("click", async () => {
      const json = updatePreview();
      if (!json) {
        return;
      }

      try {
        await navigator.clipboard.writeText(json);
        setStatus("JSON copied to the clipboard.", "success");
      } catch (error) {
        setStatus(`Unable to copy JSON: ${error.message}`, "error");
      }
    });

    downloadButton.addEventListener("click", () => {
      const json = updatePreview();
      if (!json) {
        return;
      }

      downloadJson(json);
      setStatus("JSON download started.", "success");
    });

    addTypeButton.addEventListener("click", () => {
      typesContainer.appendChild(createTypeCard());
      updatePreview();
    });

    [documentIdInput, documentRevisionInput, schemaVersionInput].forEach(element => {
      element.addEventListener("input", updatePreview);
      element.addEventListener("change", updatePreview);
    });

    loadDocument(sampleDocument);
  </script>
</body>
</html>
""";

        return html.Replace("__INITIAL_DOCUMENT__", initialJson);
    }

    private static JsonElement ParseJsonElement(string json)
    {
        return JsonDocument.Parse(json).RootElement.Clone();
    }
}