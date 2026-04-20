# Initial Product And Implementation Plan

## Purpose

`couchSQL` is intended to provide an API-driven way to connect Apache CouchDB sources to PostgreSQL so CouchDB data can later be queried through SQL.

This document defines the initial foundation only. It focuses on the first API layer, system boundaries, and phased implementation planning. It deliberately stops before the replication mechanics and data-shaping strategy.

## Scope Of This First Planning Step

This planning step covers:
- the runtime platform and API technology
- the initial endpoint groups and their responsibilities
- the separation between user-facing and administrative operations
- the first configuration model
- the admin database bootstrap and migration approach
- logging, startup validation, and health monitoring requirements
- security and deployment guardrails for the admin surface
- a phased implementation plan for the foundation

This planning step does not yet cover:
- how CouchDB changes are read and replayed
- how PostgreSQL tables are generated from CouchDB documents
- conflict handling during synchronization
- bulk backfill strategy
- long-running job orchestration

## Product Goals

The initial product should satisfy these goals:

1. Run cross-platform on Windows, macOS, and Linux by using .NET.
2. Expose a query-focused API that allows callers to discover available PostgreSQL databases and tables and execute approved queries.
3. Expose a separate administrative API that allows operators to register or remove CouchDB database connections.
4. Keep the administrative surface localhost-only by default.
5. Keep the initial runtime configuration minimal by requiring only PostgreSQL system access in application configuration.
6. Support automated admin database creation and SQL-script migrations during startup.
7. Provide operational visibility through Serilog file logging, startup self-check output, and a health endpoint.
8. Be structured so later synchronization logic can be added without redesigning the API boundary.

## Non-Goals For The First Delivery

The first delivery should not attempt to solve the full product.

Out of scope for the first implementation:
- document replication logic
- schema inference and migration rules
- user interface work
- distributed coordination across multiple API nodes
- broad authentication and authorization design beyond the local admin boundary

## Proposed Foundation Architecture

The initial architecture should be a single ASP.NET Core Web API application with two logical API surfaces:

1. A query API for callers that need discovery and SQL query access.
2. An admin API for registering or removing CouchDB sources.

Using one application keeps deployment simple in the early phase, but the surfaces should be isolated by route prefix, policy, and Swagger document.

Recommended platform choices:
- .NET 10 or newer ASP.NET Core Web API
- OpenAPI and Swagger UI for endpoint documentation
- Npgsql for PostgreSQL connectivity
- Serilog with file sink for operational logging
- strongly typed options for application configuration
- SQL-script based database migration runner for the admin database

## API Surface Design

The system should treat the two required endpoints as two API groups rather than literally one route each.

### 1. Query API

Purpose:
- expose database and table discovery
- expose SQL query execution against PostgreSQL

Recommended route prefix:
- `/api/v1`

Recommended initial operations:
- `GET /api/v1/databases`
- `GET /api/v1/databases/{databaseName}/tables`
- `POST /api/v1/query`

Expected behavior:
- database listing returns only databases managed or approved by the service
- table listing returns queryable tables for the selected database
- query execution accepts a controlled query payload and returns streamed or paged results
- query execution allows `SELECT` statements only
- query execution enforces a built-in row limit policy

Important design constraint:
- the query endpoint must reject non-`SELECT` SQL and must enforce validation rules, row limits, timeouts, and auditing hooks from the first delivery.

Query limit policy:
- a default row limit must be configurable through application configuration
- a maximum allowed row limit must also be configurable through application configuration
- the effective query limit should be changeable through a management API call without redeploying the service
- callers must not be allowed to bypass the configured upper bound through request payloads

### 2. Admin API

Purpose:
- register a CouchDB database source
- remove a previously registered CouchDB database source
- manage service-level operational settings that should not require a redeploy

Recommended route prefix:
- `/internal/v1`

Recommended initial operations:
- `POST /internal/v1/couchdb/connections`
- `DELETE /internal/v1/couchdb/connections/{connectionId}`
- `GET /internal/v1/couchdb/connections/{connectionId}/tables/{tableName}/state`
- `GET /internal/v1/settings/query`
- `PUT /internal/v1/settings/query`

Expected request data for registration:
- CouchDB base URL
- CouchDB username
- CouchDB password or token
- CouchDB database name
- optional logical connection name

Expected behavior:
- validate the CouchDB connection before accepting it
- persist the registration in system metadata
- keep the remove operation idempotent where practical
- expose the current state of a managed table, including whether it is active, snapshotting, rebuilding, swapping, paused, or in error
- allow operators to inspect and update the active query limit settings

Expected behavior for table state inspection:
- return the canonical table name
- return the current lifecycle state for that table
- return whether a shadow table currently exists
- return the current shadow table name when relevant
- return the current snapshot or rebuild progress if one is running
- return the last applied design revision and last processed source sequence for that table where available
- return pending change count and processed row count when a snapshot or rebuild is active
- return the last error message if the table is paused or failed

## Swagger And Endpoint Separation

The admin surface should not clutter the main API documentation.

Recommended approach:

1. Generate two OpenAPI documents.
2. Expose the query Swagger UI as the main documentation surface.
3. Expose the admin Swagger UI separately, and only on the localhost-bound admin listener.

Suggested mapping:
- query Swagger: `/swagger/query`
- admin Swagger: `/swagger/admin`

This keeps consumer-facing documentation clean while still making local operational setup straightforward.

## Network And Security Boundary

The admin API should be localhost-only by default.

Recommended hosting model:
- bind the query API listener to the normal application host address
- bind the admin API listener to `127.0.0.1` and `::1` only

This can be implemented with separate Kestrel listeners and route or policy checks.

Initial security stance:
- local-only admin access is the primary control for the first phase
- CouchDB credentials must not be hardcoded in application configuration
- PostgreSQL system credentials must be stored through standard .NET configuration and secret-management practices

Open point:
- the query API is assumed to run in a trusted environment for the first delivery, so no authentication is required initially

## Initial Configuration Model

The initial application configuration should include only the PostgreSQL system connection and service-level settings.

Required configuration at startup:
- PostgreSQL host
- PostgreSQL port
- PostgreSQL system database name
- PostgreSQL admin metadata database name
- PostgreSQL administrative username
- PostgreSQL administrative password
- listener settings for public API and localhost-only admin API
- query limit defaults and upper bounds
- sync snapshot and longpoll tuning defaults
- encryption key file location for CouchDB credential protection
- Serilog file logging location and retention settings

The PostgreSQL user configured here must be able to:
- create databases
- remove databases
- update database objects

CouchDB registrations should not be part of static configuration. They should be added and removed only through the admin API.

At startup the service should:
- connect to PostgreSQL using the configured system user
- create the admin metadata database if it does not exist
- apply pending SQL-script migrations to the admin metadata database
- ensure the credential-encryption key exists at the configured key path, generating it if missing
- print a startup self-check summary to the console

Example configuration shape:

```json
{
  "PostgreSql": {
    "Host": "localhost",
    "Port": 5432,
    "SystemDatabase": "postgres",
    "AdminDatabase": "couchsql_admin",
    "Username": "couchsql_admin",
    "Password": "<secret>"
  },
  "Endpoints": {
    "Public": "http://0.0.0.0:8080",
    "Admin": "http://127.0.0.1:8081"
  },
  "Query": {
    "DefaultRowLimit": 1000,
    "MaxRowLimit": 10000,
    "CommandTimeoutSeconds": 30
  },
  "Sync": {
    "SnapshotBatchSize": 1000,
    "SnapshotSeqInterval": 1000,
    "LongpollHeartbeatMilliseconds": 60000,
    "LongpollReadTimeoutSeconds": 90
  },
  "Security": {
    "EncryptionKeyPath": "./keys/couchsql.key"
  },
  "Serilog": {
    "Path": "./logs/couchsql-.log",
    "RollingInterval": "Day",
    "RetainedFileCountLimit": 14
  }
}
```

Recommended sync default rules:
- `SnapshotBatchSize` should default to `1000`
- `SnapshotSeqInterval` should default to the same value as `SnapshotBatchSize`
- `LongpollHeartbeatMilliseconds` should default to `60000`
- `LongpollReadTimeoutSeconds` should default to a value safely above the heartbeat interval, such as `90`

## Admin Database And Migration Strategy

An internal admin database should be used to store service metadata and control-plane state.

Responsibilities of the admin database:
- store registered CouchDB connections
- store encrypted CouchDB credentials
- store query setting overrides made through the management API
- store migration history
- store source-level synchronization metadata such as listener sequences, last design revision, and source lifecycle state
- store table-level synchronization metadata such as table lifecycle state, current snapshot mode, pending change count, processed row count, shadow-table presence, and last table error

Migration approach:
- migrations should be plain SQL scripts stored in the repository
- each script should be versioned and applied in order
- the application should maintain a migration history table inside the admin database
- startup should apply only pending migrations
- startup should fail fast if a migration cannot be applied cleanly

This keeps upgrades explicit and auditable while avoiding runtime dependence on ORM-managed schema changes.

## Credential Storage And Encryption

CouchDB credentials must be stored because the service needs them after registration.

Recommended approach:
- encrypt CouchDB secrets before writing them to the admin database
- load the encryption key from the configured key path
- generate a new key during startup if the file does not exist
- keep the key outside the database so database backups do not automatically expose usable credentials

Important constraint:
- key rotation is not required for the first delivery, but the implementation should avoid blocking future rotation support

## Internal Service Responsibilities

Even before synchronization is designed, the codebase should be split by responsibility.

Suggested initial components:
- API layer for HTTP routing and request validation
- PostgreSQL system service for database and metadata operations
- admin database bootstrap and migration service
- CouchDB connection validation service
- credential encryption service
- metadata repository for registered CouchDB sources
- source synchronization metadata repository
- table state metadata repository
- query execution service with guardrails for limits and timeouts
- query settings service for effective row-limit management
- startup diagnostics and health reporting service

This separation prevents the future synchronization engine from being coupled directly to HTTP controllers.

## Logging, Startup Self-Check, And Health Monitoring

Operational visibility is required from the first delivery.

Logging requirements:
- use Serilog as the application logging framework
- write logs to rolling files on disk
- write startup state and migration activity to both the console and file logs
- include enough context in logs to troubleshoot connection, migration, and query-limit issues

Startup self-check requirements:
- validate PostgreSQL connectivity
- validate or create the admin metadata database
- validate migration status and apply pending migrations
- validate encryption key availability
- print a concise pass or fail summary to the console during startup

Health endpoint requirements:
- expose a simple `GET` endpoint on the system API surface
- recommended route: `GET /api/v1/health`
- return an easily machine-readable status payload
- include at least application readiness, PostgreSQL connectivity, and migration state

## High-Volume Planning Guardrails

Because the tool is expected to handle high data volumes later, the initial API design should avoid choices that will block scale.

Guardrails to adopt now:
- avoid in-memory loading of large result sets
- design query responses for paging or streaming
- enforce configurable query timeouts
- enforce configurable result size limits with a default and hard maximum
- keep administrative actions idempotent where possible
- record metadata and operational events for later troubleshooting

These are foundation constraints only. The actual synchronization throughput model will be addressed in a later planning step.

## Phased Implementation Plan

### Phase 1: Solution Bootstrap

Deliverables:
- create the ASP.NET Core Web API solution
- add Swagger and OpenAPI setup
- define configuration objects and validation
- add Serilog console and rolling-file logging
- define project structure for API, services, and infrastructure

Exit criteria:
- application starts successfully
- Swagger loads for the query surface
- admin surface is separated by route prefix and listener binding
- startup self-check prints a clear summary to the console

### Phase 2: PostgreSQL Foundation

Deliverables:
- integrate Npgsql
- implement PostgreSQL connection factory
- implement admin database creation if missing
- implement SQL-script migration runner and migration history tracking
- implement database discovery operations
- implement metadata storage for registered CouchDB sources
- implement source-level synchronization metadata storage
- implement table-level synchronization metadata storage
- implement encrypted CouchDB credential storage

Exit criteria:
- service can connect to PostgreSQL using configured credentials
- service creates the admin database if missing
- service applies pending admin database migrations automatically
- service can list databases and tables
- service can persist and read connection metadata
- service can persist and read source and table synchronization state

### Phase 3: Query API

Deliverables:
- implement database listing endpoint
- implement table listing endpoint
- implement controlled `SELECT`-only SQL query endpoint
- implement query row-limit configuration and enforcement
- add request validation, limits, and error handling

Exit criteria:
- callers can discover available databases and tables
- callers can run approved `SELECT` queries through the API
- query limits can be set from configuration and updated through the management API
- responses enforce limits and fail predictably

### Phase 4: Localhost-Only Admin API

Deliverables:
- implement CouchDB registration endpoint
- implement CouchDB removal endpoint
- implement managed-table state endpoint
- implement query settings management endpoint
- validate CouchDB connectivity during registration
- expose separate admin Swagger documentation locally

Exit criteria:
- new CouchDB sources can be registered and removed through localhost-only endpoints
- operators can inspect managed table state, including rebuild and snapshot progress, through the localhost-only admin API
- query-limit settings can be queried and updated locally through the management API
- admin endpoints are absent from the main consumer Swagger surface

### Phase 5: Operational Readiness

Deliverables:
- structured logging
- health checks
- startup configuration validation
- startup self-check reporting
- baseline integration tests for PostgreSQL and admin routing behavior

Exit criteria:
- the service fails fast on invalid config
- key operational flows are test-covered
- rolling file logging is enabled through Serilog
- `GET /api/v1/health` reports service readiness correctly
- localhost-only admin access is verified by tests or deployment checks

## Confirmed Design Decisions For The First Delivery

The following decisions are now fixed for the first delivery:

1. The query endpoint allows `SELECT` statements only.
2. The service creates one PostgreSQL database per CouchDB source.
3. CouchDB registration metadata lives in the admin metadata database.
4. CouchDB credentials are stored encrypted at rest in the admin metadata database.
5. The encryption key is loaded from a configured path and generated at startup if the key file does not yet exist.
6. The query API runs in a trusted environment in the first delivery and does not require authentication.
7. Query row limits are controlled by configuration and can be updated through the management API.
8. The service must create the admin metadata database if missing and apply pending SQL-script migrations automatically at startup.
9. The service must use Serilog file logging, print startup state to the console, and expose a simple GET health endpoint.
10. The service exposes a localhost-only managed-table state endpoint for snapshot, rebuild, and cutover visibility.
11. The admin metadata database stores both source-level and table-level synchronization state.
12. Sync batching and longpoll behavior use safe defaults from configuration and remain adjustable through appsettings.

## Recommended Outcome Of This Review

If this document is accepted, the next planning step should focus on synchronization design only:
- CouchDB change capture approach
- initial load and incremental sync flow
- PostgreSQL target structure generation
- failure recovery and replay strategy

That keeps the planning sequence disciplined:

1. define API and operational boundaries
2. define synchronization model
3. define schema generation and evolution rules
4. implement in phases with tests