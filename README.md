# couchSQL

`couchSQL` connects a CouchDB database to PostgreSQL and keeps a SQL-shaped projection of selected CouchDB documents available for querying.

This repository is currently aimed at the first alpha release: local or trusted-network development, explicit schema projection through a CouchDB design document, and a read-only SQL API over managed PostgreSQL targets.

## Start Here

- installation, local setup, quick start, configuration, operations, and troubleshooting: [INSTALL.md](INSTALL.md)
- product and API planning background: [docs/01-initial-product-and-implementation-plan.md](docs/01-initial-product-and-implementation-plan.md)
- sync and schema design notes: [docs/02-database-build-and-sync-design.md](docs/02-database-build-and-sync-design.md)

## Alpha Summary

In the current alpha, `couchSQL` provides:

- one managed PostgreSQL target database per registered CouchDB source
- an admin API for health, source registration, resync, query settings, and design-document tooling
- a public query API for listing managed databases, listing tables, inspecting table structure, and running read-only SQL
- a hosted design-document builder at `/internal/v1/design-documents/builder`
- continuous sync from CouchDB `_changes`
- schema reconcile with targeted per-type planning and shadow-table swap for rebuild-required changes
- optional per-field text transforms using `prefix` and `append`

## How It Works

At a high level:

1. A source is registered through the admin API.
2. `couchSQL` validates CouchDB access and loads `_design/couchsql`.
3. The design document is validated and translated into PostgreSQL schema.
4. A dedicated PostgreSQL target database is created or reused for that source.
5. A sync supervisor starts listeners for design changes and document changes.
6. Matching CouchDB documents are projected into PostgreSQL tables.
7. The public API exposes those managed tables through a restricted read-only SQL surface.

There are two logical listener paths per source:

- design listener: watches `_design/couchsql`
- data listener: watches source documents through `_changes` with `_selector`

That split allows ordinary document changes to stay on the row-update path while schema-affecting design changes go through reconcile logic.

## Repository Layout

- [src/CouchSql.Api](src/CouchSql.Api): ASP.NET Core host, endpoints, middleware, Swagger, runtime bootstrapping
- [src/CouchSql.Core](src/CouchSql.Core): shared contracts, design models, options, interfaces
- [src/CouchSql.Infrastructure](src/CouchSql.Infrastructure): CouchDB client, PostgreSQL service, sync supervisor, reconcile logic, validation, credential protection
- [tests/CouchSql.Tests](tests/CouchSql.Tests): unit tests

## API Surface

Public query API base path: `http://127.0.0.1:8080/api/v1`

- `GET /databases`
- `GET /databases/{databaseName}/tables`
- `GET /databases/{databaseName}/tables/{tableName}/structure`
- `POST /query`

Local admin API base path: `http://127.0.0.1:8081/internal/v1`

- `GET /health`
- `POST /couchdb/connections`
- `DELETE /couchdb/connections/{connectionId}`
- `POST /couchdb/connections/{connectionId}/resync`
- `GET /couchdb/connections/{connectionId}/tables/{tableName}/state`
- `GET /settings/query`
- `PUT /settings/query`
- `GET /design-documents`
- `GET /design-documents/builder`
- `GET /design-documents/template`
- `POST /design-documents/validate`

Swagger UIs:

- public/query: `http://127.0.0.1:8080/swagger/query`
- admin: `http://127.0.0.1:8081/swagger/admin`

Admin routes are loopback-only by default through [src/CouchSql.Api/Middleware/LocalAdminOnlyMiddleware.cs](src/CouchSql.Api/Middleware/LocalAdminOnlyMiddleware.cs).

## Design Contract Highlights

The source CouchDB database must include `_design/couchsql` with a `couchsql` object containing:

- `schemaVersion = 1`
- one or more declared types
- identify rules describing which documents map to each type
- field mappings describing PostgreSQL columns and types
- optional indexes

Supported identify predicates:

- `equals`
- `exists`
- `contains`
- `all`
- `any`

Supported target field types:

- `text`
- `integer`
- `bigint`
- `numeric`
- `boolean`
- `date`
- `timestamp`
- `timestamptz`
- `jsonb`
- `uuid`
- `double precision`

Optional text transforms:

```json
"transform": {
	"prefix": "Customer: ",
	"append": " (active)"
}
```

Transforms are only applied to `text` fields, and changing a transform on an existing field is treated as a rebuild-required schema change.

The setup guide in [INSTALL.md](INSTALL.md) includes a complete example and registration walkthrough.

## Current Boundaries

Current alpha constraints include:

- no auth model beyond local-only admin routing
- intended for local development or trusted internal environments
- one PostgreSQL target database per registered CouchDB source
- read-only query surface with single-statement `SELECT` or `WITH ... SELECT` only
- strict design-document validation
- reconcile logic optimized first for correctness and safe transition behavior
