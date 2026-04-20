# Database Build And Sync Design

## Purpose

This document defines how `couchSQL` should create and maintain a PostgreSQL target database when a new CouchDB source is registered through the admin API.

It covers:
- source registration flow
- design document loading and validation
- target database creation rules
- table creation rules from the CouchDB design document
- listener lifecycle for design and data changes
- persisted sync state required for restart and recovery

It assumes the foundation decisions from the initial planning document are already fixed:
- one PostgreSQL database per CouchDB source
- a PostgreSQL admin metadata database exists
- CouchDB credentials are stored encrypted in the admin metadata database
- query access is read-only and trusted-network only for the first delivery

## Core Design Principle

Schema configuration changes and data synchronization changes should not be handled by the same logical listener.

The system should use two listeners per tracked CouchDB database:

1. A design listener that watches only `_design/couchsql`.
2. A data listener that watches only documents selected for synchronization.

This split is required because schema changes affect PostgreSQL DDL, while normal document changes affect table rows.

## Registration Flow

When a caller registers a new CouchDB database through the admin API, the service should execute the following sequence.

### Step 1: Validate CouchDB Access

The service must:
- verify the CouchDB base URL is reachable
- verify the provided credentials are valid
- verify the requested CouchDB database exists
- fail registration immediately if any of these checks fail

### Step 2: Resolve Target PostgreSQL Database Name

The target PostgreSQL database name should be resolved as follows:
- by default, use the CouchDB database name
- allow the caller to provide an optional override name
- normalize the final name to a safe PostgreSQL identifier format
- reject the registration if the final name conflicts with an existing unmanaged database

Recommended normalization rules:
- lowercase all characters
- replace unsupported characters with underscores
- reject empty names after normalization
- store both the original CouchDB name and resolved PostgreSQL name in the admin metadata database

### Step 3: Create The Target PostgreSQL Database

The service must:
- create the PostgreSQL target database if it does not exist
- mark the database as managed by `couchSQL`
- fail registration if creation is required but cannot be completed cleanly

The target database should be dedicated to a single CouchDB source.

### Step 4: Load The Design Document

The service must load the CouchDB design document with ID `_design/couchsql`.

The registration must fail if:
- the design document does not exist
- the design document cannot be read with the supplied credentials
- the design document does not match the required `couchSQL` JSON contract

### Step 5: Validate The Design Document Contract

The design document must be validated before any PostgreSQL schema is created.

Validation must confirm:
- the design document contains the expected `couchsql` configuration object
- the configuration schema version is supported
- all declared types have unique logical names
- all declared table names are unique within the target database
- each type declares one valid identification rule
- all declared PostgreSQL column types are supported
- all field mappings are valid
- the runtime `_selector` payload can be generated from the validated per-type identification rules

If validation fails, the registration must stop before any listener starts.

### Step 6: Build Or Update Target Tables

After the design document passes validation, the service must create the required PostgreSQL tables in the target database.

Initial schema build should:
- create one table per configured type
- create required indexes
- create internal metadata structures required for row upsert and delete handling

### Step 7: Persist Registration State

Before listeners start, the service must persist the registration and initial sync state in the admin metadata database.

The persisted state must include at least:
- CouchDB base URL
- CouchDB database name
- resolved PostgreSQL database name
- encrypted CouchDB credentials
- design document ID
- last known design document revision
- last known design listener sequence
- last known data listener sequence
- current listener status
- current schema version from the design document

### Step 8: Start Listeners

After the state has been persisted, the service must start both listeners for the new source:
- design listener
- data listener

Registration should only be marked active when both listeners are successfully started or intentionally queued for supervised startup.

## Design Document Contract

The source CouchDB database must contain a design document at `_design/couchsql`.

The design document should contain the `couchsql` JSON configuration object that describes how PostgreSQL tables should be built and how source documents are identified.

Recommended high-level structure:

```json
{
  "_id": "_design/couchsql",
  "couchsql": {
    "schemaVersion": 1,
    "types": [
      {
        "name": "customer",
        "table": "customers",
        "identify": {
          "all": [
            { "path": "meta.entity", "equals": "customer" },
            { "path": "customer.id", "exists": true }
          ]
        },
        "fields": [
          { "column": "customer_id", "path": "customer.id", "type": "text", "required": true },
          { "column": "display_name", "path": "profile.name", "type": "text", "required": false },
          { "column": "city", "path": "profile.address.city", "type": "text", "required": false },
          { "column": "created_at", "path": "meta.createdAt", "type": "timestamptz", "required": false }
        ],
        "indexes": [
          { "name": "ix_customers_customer_id", "columns": ["customer_id"], "unique": true }
        ]
      }
    ]
  }
}
```

Important distinction:
- `types[].indexes` means PostgreSQL indexes that `couchSQL` should create on the target tables
- it does not mean CouchDB Mango indexes on the source database
- source-side CouchDB indexes are a separate optimization concern for `_selector` performance and should not be mixed into the row-mapping contract

Recommended exact top-level contract for the first delivery:

```json
{
  "_id": "_design/couchsql",
  "couchsql": {
    "schemaVersion": 1,
    "types": [
      {
        "name": "customer",
        "table": "customers",
        "identify": {
          "all": [
            { "path": "meta.entity", "equals": "customer" },
            { "path": "customer.id", "exists": true }
          ]
        },
        "fields": [
          { "column": "customer_id", "path": "customer.id", "type": "text", "required": true }
        ],
        "indexes": [
          { "name": "ix_customers_customer_id", "columns": ["customer_id"], "unique": true }
        ]
      }
    ]
  }
}
```

Top-level contract rules:
- `_id` must be `_design/couchsql`
- `couchsql` must exist and must be an object
- `schemaVersion` must exist and must be `1` for the first delivery
- `types` must exist and must be a non-empty array
- each type must contain `name`, `table`, `identify`, and `fields`
- `indexes` is optional and defaults to an empty array

## Document Identification Rules

For the first delivery, type routing should be per-type rule-based.

The source documents may be completely different from one another.

That means the design must not depend on a shared discriminator field such as `type`.

Instead, each type should declare its own simple identification rule.

Best case:
- several types can still use the same field, such as `type` or `meta.entity`
- but this is only an optimization and simplification, not a requirement

The service should support a small identification DSL that is simple for users and can be reused in two places:

1. to build the CouchDB `_changes?filter=_selector` request
2. to let the C# sync engine decide which mapping owns a returned document

Recommended per-type property name:
- `identify`

Recommended type configuration rules:
- each type must declare `name`
- each type must declare one `identify` object
- the `identify` object should contain only simple predicates that are valid both in CouchDB selector generation and in local C# evaluation
- users should not have to write raw Mango selector JSON

Supported first-delivery predicate forms should be:
- `equals`: exact field equality, for example `{ "path": "meta.entityName", "equals": "customer" }`
- `exists`: structural existence, for example `{ "path": "invoice.id", "exists": true }`
- `contains`: exact element membership in an array field, for example `{ "path": "tags", "contains": "customer" }`
- `all`: all listed predicates must match
- `any`: at least one listed predicate must match

`contains` rules for the first delivery:
- `contains` means array membership only
- it should be used only when the source field is a JSON array
- it checks for exact element equality, not substring matching
- string substring search is intentionally out of scope for the first delivery DSL

Important scope rule:
- `couchSQL` should not attempt to support arbitrary Mango selector syntax inside the mapping contract
- users define `identify` rules in the smaller `couchSQL` DSL only
- the service translates that DSL into Mango selector branches for CouchDB and into local predicates for the C# sync engine
- if a desired identification rule cannot be represented in the supported DSL, registration should fail with a validation error instead of allowing raw selector text

Recommended first-delivery rule shape:

```json
{
  "identify": {
    "all": [
      { "path": "invoice.id", "exists": true },
      { "path": "invoice.total", "exists": true }
    ]
  }
}
```

Example using `contains`:

```json
{
  "identify": {
    "all": [
      { "path": "tags", "contains": "customer" },
      { "path": "customer.id", "exists": true }
    ]
  }
}
```

## Exact Identify DSL Contract

The first delivery should lock the `identify` contract down to a very small recursive JSON structure.

Allowed node shapes:

### Equality Predicate

```json
{ "path": "meta.entityName", "equals": "customer" }
```

Rules:
- `path` is required
- `equals` is required
- `equals` may be any JSON scalar value supported by the first delivery
- the rule matches when the value resolved from `path` is exactly equal to `equals`

### Existence Predicate

```json
{ "path": "invoice.id", "exists": true }
```

Rules:
- `path` is required
- `exists` is required
- the first delivery should only allow `true`
- the rule matches when the path resolves to an existing JSON value

### Contains Predicate

```json
{ "path": "tags", "contains": "customer" }
```

Rules:
- `path` is required
- `contains` is required
- the resolved value must be a JSON array
- the rule matches when the array contains one element exactly equal to `contains`
- string substring matching is not supported

### All Predicate

```json
{
  "all": [
    { "path": "meta.entityName", "equals": "customer" },
    { "path": "customer.id", "exists": true }
  ]
}
```

Rules:
- `all` is required
- `all` must be a non-empty array
- every child entry must itself be a valid `identify` node
- the rule matches only when all child rules match

### Any Predicate

```json
{
  "any": [
    { "path": "meta.entityName", "equals": "customer" },
    { "path": "legacyType", "equals": "customer" }
  ]
}
```

Rules:
- `any` is required
- `any` must be a non-empty array
- every child entry must itself be a valid `identify` node
- the rule matches when at least one child rule matches

### Structural Validation Rules

Each `identify` node must contain exactly one operator family:
- one of `equals`, `exists`, `contains`, `all`, or `any`

Additional validation rules:
- `path` values must use the supported field-path syntax already defined for mappings
- `all` and `any` cannot be nested infinitely in practice; the implementation should enforce a reasonable maximum depth
- empty strings are not valid field paths
- arrays used by `contains` should not rely on partial-object matching in the first delivery
- unsupported combinations such as `{ "path": "x", "equals": 1, "exists": true }` must fail validation

## Internal C# Rule Model

The C# implementation should parse each validated `identify` JSON object into an internal rule tree.

Recommended first-delivery model:

```csharp
abstract record IdentifyRule;

sealed record AllRule(IReadOnlyList<IdentifyRule> Children) : IdentifyRule;
sealed record AnyRule(IReadOnlyList<IdentifyRule> Children) : IdentifyRule;
sealed record EqualsRule(string Path, JsonElement Expected) : IdentifyRule;
sealed record ExistsRule(string Path) : IdentifyRule;
sealed record ContainsRule(string Path, JsonElement Expected) : IdentifyRule;
```

Responsibilities:
- parse the stored JSON contract into this rule tree once during configuration validation
- keep the parsed rule tree in memory as part of the active source registration
- use the same parsed rule tree for both Mango selector generation and local document ownership matching

## Rule Compilation Responsibilities

Once an `identify` rule has been validated and parsed, the service should compile it in two directions.

### Direction 1: Identify To Mango Selector Branch

Purpose:
- produce one safe selector branch for the `_changes` feed prefilter

Examples:

- `EqualsRule("type", "customer")` becomes `{ "type": "customer" }`
- `ExistsRule("invoice.id")` becomes `{ "invoice.id": { "$exists": true } }`
- `ContainsRule("tags", "customer")` becomes `{ "tags": { "$all": ["customer"] } }`
- `AllRule([...])` becomes one branch containing all generated child constraints
- `AnyRule([...])` becomes a nested `$or` when required

Important rule:
- only supported `identify` nodes may be translated
- if a rule cannot be translated safely and deterministically into Mango, registration must fail

### Direction 2: Identify To Local Predicate

Purpose:
- produce the C# ownership matcher used after CouchDB has returned a candidate document

Recommended runtime shape:

```csharp
Func<JsonElement, bool>
```

Conceptual evaluation rules:
- `AllRule` returns true only when all children return true
- `AnyRule` returns true when any child returns true
- `EqualsRule` resolves the field path and compares the resolved JSON value for exact equality
- `ExistsRule` returns true when the field path resolves successfully
- `ContainsRule` returns true when the resolved JSON value is an array containing an exactly equal element

Important rule:
- the C# side evaluates the `identify` DSL directly
- it must not attempt to interpret arbitrary Mango selector syntax

## Step 1 And Step 2 In Implementation Terms

In practical terms, the earlier matching flow means:

1. Parse each validated `identify` JSON object into an internal `IdentifyRule` tree.
2. Compile that `IdentifyRule` tree into a local predicate for ownership matching.

What is needed for step 1:
- a parser from `identify` JSON into the internal rule tree
- validation that each node uses one supported operator family only
- field-path parsing using the same supported path rules used elsewhere in the contract

What is needed for step 2:
- a recursive evaluator or compiled predicate over `JsonElement`
- exact JSON equality checks for `equals`
- path resolution helpers for nested fields and array indexes
- array membership checks for `contains`

What is explicitly not needed for step 1 and step 2:
- a full Mango parser
- support for all CouchDB Mango operators
- a generic in-process query engine

Routing rules:
- the service builds one `$or` selector from all type `identify` rules
- CouchDB uses that selector only to decide whether a changed document should be returned in the feed
- after a document is returned, the C# sync engine evaluates each type's `identify` rule against that concrete document
- the document is assigned to the one type whose rule matches
- matching is exact and case-sensitive unless a future normalization rule is explicitly introduced
- if more than one type matches the same returned document, the mapping configuration is ambiguous and the source should be marked as configuration error
- if no type matches a changed document, the event is ignored by the row-mapping stage

Why this is better than regex for the first delivery:
- the user writes one simple identification rule per type
- the `_changes` selector can be generated automatically as one `$or` across all types
- the C# side uses the exact same rules for final ownership matching
- the mapping contract is easier to validate and reason about
- configuration stays simple even when documents have different shapes

Why the C# side should not support all Mango selectors:
- Mango is a database query language, not a good application-side mapping contract
- many Mango operators are about query planning and index usage rather than deterministic document identity
- implementing the full Mango surface in C# would create two different selector engines that are hard to keep behaviorally identical
- a smaller DSL lets the service guarantee that the CouchDB prefilter and the C# ownership evaluator mean the same thing

Compatibility decision:
- legacy regex fallback should not exist in the first delivery
- sources must be expressible through the supported `identify` DSL or registration fails

## Worked Example

The easiest way to understand the split is to look at one concrete source database.

### Easy Case: Shared Type Field

Assume CouchDB contains these documents:

```json
{
  "_id": "customer:1001",
  "type": "customer",
  "customer": {
    "id": "1001"
  },
  "profile": {
    "name": "Ada Lovelace",
    "address": {
      "city": "London"
    }
  },
  "meta": {
    "createdAt": "2026-04-20T10:15:00Z"
  }
}
```

```json
{
  "_id": "invoice:9001",
  "type": "invoice",
  "invoice": {
    "id": "9001",
    "customerId": "1001",
    "total": 149.95
  },
  "meta": {
    "createdAt": "2026-04-20T10:20:00Z"
  }
}
```

And the design document contains this mapping contract:

```json
{
  "_id": "_design/couchsql",
  "couchsql": {
    "schemaVersion": 1,
    "types": [
      {
        "name": "customer",
        "table": "customers",
        "identify": {
          "all": [
            { "path": "type", "equals": "customer" }
          ]
        },
        "fields": [
          { "column": "customer_id", "path": "customer.id", "type": "text", "required": true },
          { "column": "display_name", "path": "profile.name", "type": "text", "required": false },
          { "column": "city", "path": "profile.address.city", "type": "text", "required": false },
          { "column": "created_at", "path": "meta.createdAt", "type": "timestamptz", "required": false }
        ]
      },
      {
        "name": "invoice",
        "table": "invoices",
        "identify": {
          "all": [
            { "path": "type", "equals": "invoice" }
          ]
        },
        "fields": [
          { "column": "invoice_id", "path": "invoice.id", "type": "text", "required": true },
          { "column": "customer_id", "path": "invoice.customerId", "type": "text", "required": true },
          { "column": "total", "path": "invoice.total", "type": "numeric", "required": true },
          { "column": "created_at", "path": "meta.createdAt", "type": "timestamptz", "required": false }
        ]
      }
    ]
  }
}
```

What each part is used for:

- each type `identify` rule tells both CouchDB and the C# sync engine how that document family is recognized.
- each type `table` tells PostgreSQL where matching documents are written.
- each field `path` tells the C# mapper which JSON value to extract.
- each field `column` tells PostgreSQL which target column receives the extracted value.

What the service generates for the data listener:

```json
{
  "selector": {
    "type": {
      "$in": ["customer", "invoice"]
    }
  }
}
```

What happens when the customer document arrives:

1. CouchDB returns the changed document because `type = "customer"` matches the generated selector.
2. The C# service evaluates the `customer` identification rule against the returned JSON document.
3. The rule matches, so the document is assigned to the `customer` mapping.
4. The service extracts `customer.id`, `profile.name`, `profile.address.city`, and `meta.createdAt`.
5. The row is written into the `customers` table.

The resulting PostgreSQL row is conceptually:

```text
customers
_id = customer:1001
_rev = <couchdb rev>
customer_id = 1001
display_name = Ada Lovelace
city = London
created_at = 2026-04-20 10:15:00+00
```

What happens when the invoice document arrives:

1. CouchDB returns the changed document because `type = "invoice"` matches the same selector.
2. The C# service evaluates the `invoice` identification rule against the returned JSON document.
3. The rule matches, so the document is assigned to the `invoice` mapping.
4. The service extracts `invoice.id`, `invoice.customerId`, `invoice.total`, and `meta.createdAt`.
5. The row is written into the `invoices` table.

The resulting PostgreSQL row is conceptually:

```text
invoices
_id = invoice:9001
_rev = <couchdb rev>
invoice_id = 9001
customer_id = 1001
total = 149.95
created_at = 2026-04-20 10:20:00+00
```

What the design listener is used for in the same example:

- it does not watch customer or invoice documents
- it watches only `_design/couchsql`
- if the mapping changes, for example a new `shipment` type is added, the design listener detects that change first
- the service then marks the source as pending schema reconcile so the new contract is applied on the next startup before normal data sync resumes

Why this is simpler than regex in practice:

- the selector decides which documents are interesting for sync
- the `identify` rule decides which mapping owns the document
- the field paths decide which values are extracted

Each concern has one job, instead of trying to use regex both for feed filtering and for final type resolution.

### Different Shapes Case: No Shared Type Field

Assume the source documents do not share a single `type` field.

One document family looks like this:

```json
{
  "_id": "cust_1001",
  "meta": {
    "entityName": "customer"
  },
  "customer": {
    "id": "1001"
  }
}
```

Another looks like this:

```json
{
  "_id": "inv_9001",
  "invoice": {
    "id": "9001",
    "customerId": "1001",
    "total": 149.95
  }
}
```

In that case, the mapping contract should describe how each type is identified:

```json
{
  "_id": "_design/couchsql",
  "couchsql": {
    "schemaVersion": 1,
    "types": [
      {
        "name": "customer",
        "table": "customers",
        "identify": {
          "all": [
            { "path": "meta.entityName", "equals": "customer" },
            { "path": "customer.id", "exists": true }
          ]
        },
        "fields": [
          { "column": "customer_id", "path": "customer.id", "type": "text", "required": true }
        ]
      },
      {
        "name": "invoice",
        "table": "invoices",
        "identify": {
          "all": [
            { "path": "invoice.id", "exists": true },
            { "path": "invoice.total", "exists": true }
          ]
        },
        "fields": [
          { "column": "invoice_id", "path": "invoice.id", "type": "text", "required": true },
          { "column": "total", "path": "invoice.total", "type": "numeric", "required": true }
        ]
      }
    ]
  }
}
```

What the single selector does in this case:

```json
{
  "selector": {
    "$or": [
      {
        "meta.entityName": "customer",
        "customer.id": { "$exists": true }
      },
      {
        "invoice.id": { "$exists": true },
        "invoice.total": { "$exists": true }
      }
    ]
  }
}
```

What the C# side still does after CouchDB returns a document:

1. take one changed document from the `_changes` response
2. evaluate the validated `identify` rule for `customer`
3. if that fails, evaluate the validated `identify` rule for `invoice`
4. once exactly one type matches, use that type's field mappings

So the single selector is not the final type decision.

It only answers:
- should this document enter the sync pipeline?

The C# match rules answer:
- which mapping owns this document?

That is the key distinction.

How the C# side should implement matching:

1. parse the validated `identify` object into an internal rule tree
2. compile that rule tree into a local predicate such as `Func<JsonElement, bool>`
3. compile the same rule tree into one Mango selector branch for the `_changes` request
4. for each returned document, run the local predicate for each type until exactly one type matches

This means the C# side evaluates the `couchSQL` identification DSL, not arbitrary Mango.

## Field Mapping Rules

Each type must define how JSON fields map to PostgreSQL columns.

Required mapping properties:
- `column`: PostgreSQL column name
- `path`: source JSON path
- `type`: PostgreSQL target type
- `required`: whether the value must be present for a valid row

Supported path behavior for the first delivery:
- dot notation for nested objects such as `profile.address.city`
- array index notation such as `items[0].sku`
- missing optional values map to `NULL`

Recommended supported PostgreSQL types for the first delivery:
- `text`
- `integer`
- `bigint`
- `numeric`
- `boolean`
- `date`
- `timestamp`
- `timestamptz`
- `uuid`
- `jsonb`

Recommended first-delivery conversion rules:
- JSON strings may map to `text`, `uuid`, `date`, `timestamp`, or `timestamptz` when they parse cleanly
- JSON numbers may map to `integer`, `bigint`, or `numeric` when the value fits the declared PostgreSQL type
- JSON booleans may map to `boolean`
- any JSON value may map to `jsonb` without scalar coercion
- `required: true` must fail row processing for that document when the value is missing or cannot be converted to the declared PostgreSQL type
- `required: false` should map missing values to `NULL`, but type conversion errors should still be treated as row-processing failures unless a future coercion policy is introduced

Validation rules for mappings:
- column names must be unique within a type
- paths must be non-empty
- required fields must not map to incompatible nullable-only logic
- unsupported PostgreSQL types must fail validation

## Index Definition Contract

Index definitions should stay structured and declarative in the first delivery.

Recommended index shape:

```json
{
  "name": "ix_customers_customer_id",
  "columns": ["customer_id"],
  "unique": true
}
```

Index rules:
- `name` is required
- `columns` is required and must be a non-empty array of mapped PostgreSQL column names
- `unique` is optional and defaults to `false`
- raw SQL index definitions should not be allowed in the first delivery
- expression indexes, partial indexes, and storage-method-specific options should be deferred until a later version

## Target Database Build Rules

The service creates one PostgreSQL database per CouchDB source.

Inside that target database, the system should create one table per configured type.

Each table should contain:
- a primary key column for CouchDB `_id`
- a stored `_rev` column
- one column per configured field mapping
- operational metadata columns needed by the sync engine

Each synchronized row must always store both CouchDB identity values:
- `_id` as the stable source row key
- `_rev` as the last applied source revision

These columns are required so the sync engine can:
- perform deterministic upserts by `_id`
- detect repeated deliveries of the same effective source state
- skip write work when the incoming `_rev` is already stored for the row

Recommended fixed columns on every managed table:
- `_id text primary key`
- `_rev text not null`
- `_source_seq text not null`
- `_synced_at timestamptz not null`

User-defined mapped columns are added next to those fixed columns.

Recommended table build rules:
- quote identifiers safely during DDL generation
- create declared indexes after the base table exists
- track managed tables in metadata so schema reconciliation is explicit
- support shadow-table rebuilds for affected types during schema reconcile

Default index guidance:
- `_id` already has an index because it is the primary key
- that primary-key index is the correct default path for upsert and row lookup
- a default composite index on `(_id, _rev)` should not be created in the first delivery because it duplicates the leading lookup behavior already provided by the primary key
- a separate default index on `_rev` is also not required for the first delivery

## Schema Update Rules

When `_design/couchsql` changes, the system must re-read and re-validate the design document.

For the first delivery, schema changes should be applied destructively when needed.

Important implication:
- the target PostgreSQL tables do not store the full source CouchDB document
- therefore new mapped columns cannot be filled correctly from PostgreSQL alone
- changes that require existing rows to be reprojected from CouchDB source data must use shadow-table rebuild
- changes that only remove target-side structure, such as dropping a mapped column, may be handled in place without rebuild

The schema update flow should be:

1. Detect the updated design document through the design listener.
2. Store the new design listener sequence and design document revision.
3. Mark the source as pending schema reconcile.
4. Pause or gate the data listener for the affected source.
5. On the next service startup, load the updated design document.
6. Validate the new configuration.
7. Compare the new configuration with the currently applied schema metadata.
8. Classify each type change as either index-only, in-place table change, or rebuild-required.
9. Apply index-only and in-place table changes where possible.
10. For each rebuild-required type, create a shadow table with the new schema.
11. Run an initial snapshot and catch-up pass from CouchDB into the shadow table using the new contract.
12. Atomically swap the rebuilt table into place and remove the old table.
13. Store the newly applied schema state.
14. Resume the data listener from the final snapshot sequence.

Important safety rule:
- schema changes must be applied transactionally where PostgreSQL supports it

Rebuild-required changes should include at least:
- adding mapped columns that must be backfilled from source documents
- changing a mapped field path
- changing a mapped PostgreSQL type
- changing a type's `identify` rule
- changing the target table name

In-place table changes should include at least:
- dropping mapped columns
- dropping whole managed tables for types removed from the design document
- renaming a mapped column when the mapping path, target type, and row ownership rules stay unchanged

Column rename rule:
- a target-column rename should be treated as an in-place PostgreSQL metadata change
- PostgreSQL documents that `ALTER TABLE ... RENAME COLUMN` has no effect on stored data
- therefore a mapped-column rename does not require a shadow-table rebuild by itself
- if the same change also modifies the source `path`, target `type`, or document ownership rules, the change is no longer a simple rename and should be classified separately

Index-only changes should include at least:
- adding a PostgreSQL index
- dropping a PostgreSQL index
- changing index uniqueness or indexed column combinations

First-delivery schema decision:
- additive and destructive schema changes should both be applied automatically during startup reconcile when they can be satisfied by rebuilding the affected types from source data
- if a type is removed from the design document, its managed PostgreSQL table should be dropped during reconcile
- if a mapped column is added or otherwise changed in a way that requires backfilling existing rows, the affected table should be rebuilt from CouchDB source data
- if a mapped column is only removed, the service may apply that as in-place destructive DDL without using a shadow-table rebuild
- if a mapped column is only renamed, the service should apply that as in-place `ALTER TABLE ... RENAME COLUMN`
- if reconcile fails, the source should remain paused and be marked as configuration error until the next successful startup reconcile

Recommended rebuild implementation:
- create a new physical shadow table for each affected type
- build the new indexes on the shadow table
- backfill the shadow table from CouchDB using the new `identify` and field-mapping rules
- once catch-up reaches the current `last_seq`, swap the shadow table into the canonical table name
- drop the previous managed table after a successful swap

Shadow-table naming rule:
- the shadow table for a managed table should be named `<tableName>_shadow`
- only one shadow table per managed table should exist at a time in the first delivery
- if a previous failed rebuild left `<tableName>_shadow` behind, startup should inspect it and either resume or discard it based on the stored rebuild state

Why shadow-table rebuild is the default:
- it handles added and changed projected columns with one consistent mechanism when source re-projection is required
- it guarantees that the final table shape exactly matches the latest design document
- it avoids in-place partial backfills against live tables
- it keeps the logic valid even when millions of rows must be reprojected

## Table State Model

The admin API should expose the current state of each managed table.

Recommended endpoint:
- `GET /internal/v1/couchdb/connections/{connectionId}/tables/{tableName}/state`

Recommended response shape:

```json
{
  "table": "customers",
  "shadowTable": "customers_shadow",
  "state": "rebuilding",
  "hasShadowTable": true,
  "activeDesignRevision": "7-abc123",
  "lastAppliedDesignRevision": "6-def456",
  "currentSequence": "120-g1AAAA...",
  "snapshotMode": "rebuild",
  "pendingChanges": 48210,
  "processedRowCount": 1452300,
  "lastError": null,
  "updatedAt": "2026-04-20T11:05:00Z"
}
```

Recommended table states:
- `active`: the canonical table is in service and no rebuild is running
- `snapshotting`: the table is being filled for the first time
- `rebuilding`: a shadow-table rebuild is in progress
- `swapping`: the rebuild finished and the service is performing the final cutover
- `paused`: processing is intentionally stopped
- `error`: processing stopped because the table could not be brought to a valid state

Recommended response fields:
- `table`: canonical PostgreSQL table name
- `shadowTable`: shadow table name when relevant
- `state`: current lifecycle state for the table
- `hasShadowTable`: whether `<tableName>_shadow` currently exists
- `activeDesignRevision`: latest design revision seen from CouchDB
- `lastAppliedDesignRevision`: design revision currently reflected by the canonical table
- `currentSequence`: latest stored source sequence for the running snapshot or listener
- `snapshotMode`: `initial-load`, `rebuild`, or `steady-state`
- `pendingChanges`: latest `pending` value from `_changes` when available
- `processedRowCount`: approximate number of rows written during the current snapshot or rebuild
- `lastError`: last table-specific error
- `updatedAt`: last state update timestamp

## Recommended Shadow-Table Cutover Procedure

The final swap should be explicit and restartable.

Recommended cutover steps:

1. Pause writes from the steady-state data listener for the affected table.
2. Run the shadow-table snapshot until `_changes` reports `pending = 0`.
3. Request one final catch-up batch from the stored `last_seq` to close the race between backfill and cutover.
4. Start a PostgreSQL transaction for the metadata and rename steps.
5. Rename the current canonical table to `<tableName>_old`.
6. Rename `<tableName>_shadow` to `<tableName>`.
7. Update schema metadata to mark the new design revision as applied.
8. Commit the transaction.
9. Drop `<tableName>_old` after the swap is confirmed successful.
10. Resume steady-state synchronization from the final stored sequence.

Why this cutover is recommended:
- it gives a short and well-defined write pause window
- the final rename is fast compared with row-by-row in-place migration
- rollback is simpler because the old canonical table is still available until after a successful commit

## Recommended Rollback Behavior

The first delivery should prefer restartable rollback over complicated partial recovery.

Recommended failure handling:
- if backfill into `<tableName>_shadow` fails, keep the canonical table unchanged, keep the source in `rebuilding` or `error`, and resume or restart the rebuild on the next startup
- if the failure happens before the rename transaction commits, drop or reuse `<tableName>_shadow` according to the stored rebuild state and leave the canonical table untouched
- if the rename transaction commits but post-commit cleanup fails, keep the new canonical table active, leave `<tableName>_old` for operator inspection, and mark cleanup as pending rather than reverting the swap
- automatic rollback after a committed swap should not try to replay the old table back into service in the first delivery; instead the service should surface the failure state and let the next startup complete cleanup or start a new rebuild if required

## Listener Model

The system should run two supervised background listeners for each active CouchDB source.

### Design Listener

Purpose:
- watch only `_design/couchsql`
- detect configuration changes that require schema reconcile on the next startup

Recommended `_changes` strategy:
- use `POST /{db}/_changes?feed=longpoll&filter=_selector&include_docs=true&since=<stored-sequence>`
- send `{"selector":{"_id":"_design/couchsql"}}` in the request body

Why this approach:
- it still listens only for the single design document
- it allows exact resume from the last stored sequence
- `include_docs=true` lets the listener use the returned design document directly without a second fetch
- it is simpler to supervise and reconnect than a permanently open raw socket

Important note:
- this selector does not require a user-managed Mango index to work
- index selection for `_changes?filter=_selector` is not exposed the way it is for `_find`, but exact `_id` filtering is still acceptable here because the design listener volume is tiny

The design listener must store after every accepted event:
- the exact `seq` value returned by CouchDB
- the latest `_rev` of `_design/couchsql`
- the returned design document body when needed for reconcile planning
- the last successful processing timestamp

### Data Listener

Purpose:
- receive only source documents intended for PostgreSQL synchronization

Recommended `_changes` strategy has two modes:

1. Snapshot and catch-up mode for first load or rebuild
2. Steady-state mode for ongoing synchronization

Snapshot and catch-up mode:
- use `POST /{db}/_changes?feed=normal&filter=_selector&include_docs=true&since=<stored-sequence-or-0>&limit=<batch-size>&seq_interval=<batch-size>`
- generate the selector body from the validated per-type `identify` configuration
- process one batch at a time
- persist `last_seq` after each successful batch
- continue requesting batches until `pending = 0`

Steady-state mode:
- use `POST /{db}/_changes?feed=longpoll&filter=_selector&include_docs=true&since=<stored-sequence>`
- generate the selector body from the validated per-type `identify` configuration

Why this approach:
- the selector can be generated directly from the mapping contract
- the stored `since` sequence lets the service restart without losing position
- `include_docs=true` avoids an extra document fetch for the common case
- `feed=normal` can return all past matching changes immediately, which makes it suitable for initial load and rebuild backfill
- `seq_interval=<batch-size>` is documented by CouchDB as a way to reduce source load when fetching large batches, especially in sharded clusters

Why this is the default for the first delivery:
- the source may need to watch multiple document types at once, and a generated `$or` selector can combine different identification rules in one feed
- the same `identify` rules are used both for feed prefiltering and in-process ownership matching
- it removes one layer of user-authored JavaScript from the design contract
- it lets the system use the same source-read path for initial load, rebuild catch-up, and steady-state sync

The data listener must store after every processed event:
- the exact `seq` value returned by CouchDB
- the last successful processing timestamp
- the sync outcome for the event

The data snapshot process must additionally store after every successful batch:
- the batch `last_seq`
- the `pending` value returned by CouchDB
- the current snapshot mode such as `initial-load`, `rebuild`, or `steady-state`

Recommended safe defaults for the first delivery:
- default snapshot batch size should be `1000`
- default `seq_interval` should be the same as the snapshot batch size
- default longpoll heartbeat should be `60000` milliseconds
- when heartbeat is enabled, the service should not rely on CouchDB `timeout` for connection shutdown
- if the HTTP client uses a read timeout, the default should be safely above the heartbeat interval, for example `90` seconds
- all of these values should be configurable through appsettings

### Selector Generation Rules

The service should generate the data-listener selector from the validated per-type `identify` configuration.

One possible compacted selector shape when several types share the same exact-match field:

```json
{
  "selector": {
    "type": {
      "$in": ["customer", "invoice", "shipment"]
    }
  }
}
```

Generation rules:
- if several types use exact equality on the same field, those branches may be compacted into `$in`
- otherwise generate one `$or` branch per type
- each branch is generated from that type's `identify` rule
- if a predicate can be represented safely in Mango, include it in the selector branch
- `contains` on array membership should be translated into the nearest supported Mango array predicate, such as `$all` with a single expected value
- if a type rule cannot be represented safely in Mango selector syntax, registration should fail until the rule is rewritten in a supported form
- the selector is a coarse prefilter only; final ownership is still decided by the C# type match evaluator using the same `identify` rules

Recommended first-delivery implementation rule:
- treat `identify` as the source of truth
- generate Mango from `identify`; do not parse Mango back into `identify`
- evaluate `identify` directly in C#; do not try to execute arbitrary Mango in process

This should be treated as generated runtime state, not user-authored selector text.

This keeps the source configuration smaller and makes the C# implementation deterministic.

Selector limitations relevant to this project:
- the best case remains one stable field that identifies document type across all synced documents
- `$or` is valid and should be used when different types need different match conditions
- regular expressions should not be part of selector generation for high-volume feeds
- if a source cannot be identified cleanly with the supported `identify` DSL, registration should be rejected
- only the supported `identify` DSL is allowed; unsupported Mango-style logic should be rejected during validation

Source-load guidance for large databases:
- the fields used by `identify` rules should be chosen so selector filtering stays as selective and index-friendly as possible
- regular expressions should not be used for high-volume sync because CouchDB documents that regex conditions do not work with indexes
- when a source database is large, operators should create appropriate source-side Mango indexes for the fields used by the generated selector where practical for production workloads

## Initial Snapshot And Catch-Up

The system must support loading the initial PostgreSQL state even when the source contains millions of rows.

The initial load should use CouchDB `_changes`, not a separate ad hoc source scan.

Why `_changes` is sufficient:
- CouchDB documents that `_changes` with `since=0` returns past changes immediately in normal mode
- CouchDB documents that `include_docs=true` can include the winning document body with each change row
- CouchDB returns `last_seq` and `pending`, which allow the client to checkpoint progress and continue in batches
- CouchDB only guarantees the most recent change for a document, which is exactly what this projection system needs because the target stores current state, not document history

Recommended initial-load algorithm:

1. Validate the design document and build the target schema.
2. Start snapshot mode with `since=0`.
3. Request `_changes` in normal mode with `filter=_selector`, `include_docs=true`, `limit=<batch-size>`, and `seq_interval=<batch-size>`.
4. Process the returned documents into PostgreSQL using the same ownership matching and row-writing logic used by steady-state sync.
5. Persist the returned `last_seq` after each successful batch.
6. Repeat from the new `since=<last_seq>` until CouchDB returns `pending = 0`.
7. Switch the source to steady-state longpoll mode from the final stored sequence.

Recommended operational rules for large initial loads:
- keep `attachments=false`
- keep conflicts disabled unless a later feature explicitly needs them
- make the snapshot batch size configurable
- use a moderate batch size rather than a single huge response so checkpoints are frequent and memory usage stays bounded
- write to PostgreSQL in batches, but persist the source sequence only after the corresponding PostgreSQL work succeeds

Default tuning rule:
- first delivery should start with the safe defaults above and allow operators to raise or lower them through configuration after observing source and target load

The system should not use `/_all_docs` as the primary initial-load path:
- `/_all_docs` cannot directly reuse the generated `identify` selector contract
- it would require a second fetch path to materialize full documents for mapping
- it would increase source reads for databases where only a subset of documents should be synchronized

## Why Normal Plus Longpoll Instead Of Continuous For The First Delivery

Although CouchDB supports continuous feeds, the first delivery should use normal feed batches for snapshot and rebuild catch-up, then longpoll for steady-state synchronization.

Reasoning:
- normal feed returns past changes immediately, which is the correct behavior for initial load and rebuild catch-up
- longpoll is simpler to restart cleanly in a managed .NET background service
- sequence checkpointing is straightforward between responses
- failure handling and backoff are easier to reason about
- near-real-time behavior is still good enough for the initial release
- CouchDB documents that longpoll is efficient for waiting until at least one change occurs, which keeps steady-state load low

This is an implementation choice only. The logical listener model remains the same.

## Persisted State In The Admin Metadata Database

The sync engine must always be restartable.

For each registered CouchDB source, the admin metadata database must store:
- source registration identity
- resolved PostgreSQL target database name
- encrypted CouchDB credentials
- active design document revision
- last processed design listener sequence
- last processed data listener sequence
- current source status such as `pending`, `snapshotting`, `rebuilding`, `active`, `paused`, `error`
- last listener heartbeat timestamp
- last listener error details
- last successfully applied schema version

Important storage rule:
- CouchDB update sequences must be treated as opaque values and stored exactly as returned

This matters because CouchDB sequences are not guaranteed to be simple integers.

Related recovery rule:
- the stored design document `_rev` is useful for detecting schema changes, but it does not replace the stored `_changes` sequence for restart recovery

## Startup Restore Behavior

When the API starts, it must restore all active CouchDB registrations from the admin metadata database.

For each active registration the service should:

1. load the registration and encrypted credentials
2. resolve the last stored design sequence and data sequence
3. verify the target PostgreSQL database still exists
4. verify `_design/couchsql` can still be loaded
5. compare the stored design revision with the current design document revision if needed
6. run schema reconcile if the design document has changed since the last applied schema state
7. if the source has never completed its initial snapshot, run snapshot mode from `since=0`
8. recreate and start both listeners in the correct mode for the stored state

If the service was offline while CouchDB changed:
- the design listener resumes from the stored design sequence
- the data listener resumes from the stored data sequence
- the service must be able to process missed events after restart

Startup reconcile rule:
- if the current `_design/couchsql` revision differs from the last applied schema revision, the service must run schema reconcile for that source before resuming normal data synchronization
- if schema reconcile rebuilt any tables, the source must resume from the final snapshot `last_seq` produced by that rebuild

## Processing Rules For Changed Documents

For every accepted data change event, the sync engine should follow this flow:

1. read the changed document from the `_changes` event payload
2. identify the matching type by evaluating the configured `identify` rules until exactly one type matches
3. extract mapped values using the configured field paths
4. transform values into the declared PostgreSQL types
5. upsert the row into the target table by `_id`
6. store the event sequence after the row operation succeeds

If the change event indicates deletion:
- delete the matching row from the target table by `_id`
- still advance the stored sequence after the delete succeeds

Delete behavior decision:
- the first delivery should implement hard deletes only
- soft-delete projection rules are out of scope for the first delivery

If a document does not match any configured type:
- ignore it for table writes
- still allow the listener to continue normally

## Idempotency And Replay Requirements

CouchDB change processing must be idempotent.

This is required because `_changes` consumers can receive repeated or replayed events after reconnect, failover, or checkpoint recovery.

The PostgreSQL write model must therefore support safe reprocessing by:
- using `_id` as the stable row key
- storing the current `_rev` on every row
- treating repeated processing of the same effective source state as harmless

Recommended rule:
- if the incoming `_rev` matches the stored `_rev`, the write can be treated as a no-op

## Failure Handling

The registration flow must fail fast before activation if any of the following occur:
- CouchDB login validation fails
- target PostgreSQL database creation fails
- `_design/couchsql` is missing
- the design document does not pass validation
- the initial schema build fails

After activation, runtime failures should not delete the source registration.

Instead, the system should:
- mark the source state as `error`
- preserve the last successful design and data sequences
- log the failure through Serilog and console output where appropriate
- retry listener startup with backoff under supervision

## Recommended Admin Metadata Tables

The admin metadata database should at minimum contain structures equivalent to:
- `couch_sources`
- `couch_source_credentials`
- `couch_source_listener_state`
- `couch_source_schema_state`

Suggested responsibilities:
- `couch_sources`: identity, CouchDB URL, source database name, target PostgreSQL database name, lifecycle status
- `couch_source_credentials`: encrypted secrets and key metadata
- `couch_source_listener_state`: design sequence, data sequence, last heartbeat, last error, last design revision
- `couch_source_schema_state`: applied type definitions, table definitions, schema version, applied-at timestamp

## First-Delivery Decisions Fixed In This Document

The following decisions are fixed for the first delivery:
- the `couchsql` configuration object uses the exact structured contract described in this document
- supported PostgreSQL types are the common application types listed in the field-mapping section, with strict conversion rules
- index definitions are structured JSON objects, not raw SQL
- legacy regex fallback is rejected
- row deletes are hard deletes only
- design-document schema changes are detected by the design listener and reconciled during service startup before data sync resumes
- destructive schema changes are applied by rebuilding affected tables from CouchDB source data
- initial load and rebuild backfill use `_changes` snapshot batches starting from `since=0` or the appropriate checkpoint sequence

The next detail pass should focus on:
- the exact status model and progress reporting for long-running initial loads and rebuilds
- the PostgreSQL swap procedure for shadow tables, including rollback behavior if cutover fails