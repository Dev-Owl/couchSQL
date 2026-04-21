# Install And Run

This guide covers the first-alpha local setup flow for `couchSQL`: prerequisites, configuration, startup, registration, querying, operational behavior, and common troubleshooting.

For the shorter project overview, see [README.md](README.md).

## Runtime Defaults

The default runtime settings in [src/CouchSql.Api/appsettings.json](src/CouchSql.Api/appsettings.json) are:

- PostgreSQL host: `localhost`
- PostgreSQL port: `5432`
- PostgreSQL system database: `postgres`
- couchSQL admin metadata database: `couchsql_admin`
- PostgreSQL username: `couchsql`
- PostgreSQL password: `couchsql`
- public API URL: `http://127.0.0.1:8080`
- admin API URL: `http://127.0.0.1:8081`
- encryption key path: `./keys/couchsql.key`
- log path: `./logs/couchsql-.log`

When the API starts successfully it will:

- ensure the credential-encryption key exists
- ensure the admin metadata database exists
- apply admin database migrations
- seed default query settings

## Prerequisites

Install locally:

- .NET SDK 10.x
- PostgreSQL 16+ or another version compatible with the Npgsql provider in use
- CouchDB 3.x

The configured PostgreSQL login must be able to:

- connect to the system database configured in `PostgreSql:SystemDatabase`
- create databases
- create tables, indexes, and metadata in managed databases
- drop managed databases when a source is removed

For the default configuration, the simplest local setup is:

- PostgreSQL running on `localhost:5432`
- a PostgreSQL login named `couchsql` with password `couchsql`
- CouchDB reachable over HTTP from the same machine

Example PostgreSQL role creation on a local instance:

```sql
create role couchsql with login password 'couchsql' createdb;
```

## Quick Start

### 1. Clone And Restore

```powershell
git clone <your-repo-url>
cd couchSQL
dotnet restore
```

### 2. Configure PostgreSQL

Ensure PostgreSQL is running and the configured login can create databases.

If your local PostgreSQL setup differs from the repo defaults, update [src/CouchSql.Api/appsettings.json](src/CouchSql.Api/appsettings.json).

### 3. Prepare CouchDB

Create or identify a CouchDB database that contains:

- documents you want to project
- a design document with `_id = _design/couchsql`

You can author that design document by:

- using the hosted builder at `/internal/v1/design-documents/builder`
- pasting existing JSON copied from CouchDB into the builder
- authoring JSON manually and saving it into CouchDB

### 4. Start The API

```powershell
dotnet run --project .\src\CouchSql.Api\CouchSql.Api.csproj
```

The development launch profile in [src/CouchSql.Api/Properties/launchSettings.json](src/CouchSql.Api/Properties/launchSettings.json) binds:

- `http://127.0.0.1:8080`
- `http://127.0.0.1:8081`

### 5. Verify Startup

```powershell
Invoke-RestMethod http://127.0.0.1:8081/internal/v1/health
```

Healthy startup should report:

- `ready = true`
- PostgreSQL available
- admin database ready
- migrations applied
- encryption key ready

### 6. Register A CouchDB Source

```powershell
$body = @{
	baseUrl = "http://127.0.0.1:5984"
	username = "admin"
	passwordOrToken = "password"
	databaseName = "example_source"
	logicalConnectionName = "Local Example"
	targetDatabaseName = "example_reporting"
} | ConvertTo-Json

Invoke-RestMethod \
	-Method Post \
	-Uri http://127.0.0.1:8081/internal/v1/couchdb/connections \
	-ContentType application/json \
	-Body $body
```

During registration, `couchSQL`:

- validates CouchDB access
- loads and validates `_design/couchsql`
- normalizes the target PostgreSQL database name
- creates the target PostgreSQL database if needed
- creates the initial schema
- persists source metadata in `couchsql_admin`
- queues sync immediately

### 7. Query The Managed Data

List managed databases:

```powershell
Invoke-RestMethod http://127.0.0.1:8080/api/v1/databases
```

List tables in a managed database:

```powershell
Invoke-RestMethod http://127.0.0.1:8080/api/v1/databases/example_reporting/tables
```

Inspect table structure:

```powershell
Invoke-RestMethod http://127.0.0.1:8080/api/v1/databases/example_reporting/tables/customers/structure
```

Run a query:

```powershell
$query = @{
	databaseName = "example_reporting"
	sql = "select * from customers order by customer_id"
	rowLimit = 100
} | ConvertTo-Json

Invoke-RestMethod \
	-Method Post \
	-Uri http://127.0.0.1:8080/api/v1/query \
	-ContentType application/json \
	-Body $query
```

## Design Document Contract

The source CouchDB database must contain a design document with:

- `_id = _design/couchsql`
- a top-level `couchsql` configuration object
- `schemaVersion = 1`
- at least one declared type

Example:

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
					{
						"column": "customer_id",
						"path": "customer.id",
						"type": "text",
						"required": true
					},
					{
						"column": "customer_name",
						"path": "customer.name",
						"type": "text",
						"required": false,
						"transform": {
							"prefix": "Customer: ",
							"append": ""
						}
					},
					{
						"column": "created_at",
						"path": "meta.createdAt",
						"type": "timestamptz",
						"required": false
					}
				],
				"indexes": [
					{
						"name": "ix_customers_customer_id",
						"columns": ["customer_id"],
						"unique": true
					}
				]
			}
		]
	}
}
```

### Supported Field Types

Supported PostgreSQL target types are:

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

### Identify DSL

Supported identify predicates are:

- `equals`
- `exists`
- `contains`
- `all`
- `any`

Current `contains` behavior in the alpha implementation:

- array membership for array values
- substring matching for string values

### Field Transform Support

Each field can optionally declare:

```json
"transform": {
	"prefix": "Customer: ",
	"append": " (active)"
}
```

Behavior:

- transforms are only applied to `text` fields
- `prefix` is prepended
- `append` is appended
- if the projected source value is `null`, no prefix or append is added
- if `transform` is configured on a non-text field, runtime projection ignores it
- adding or changing a field transform on an existing mapped field is treated as a rebuild-required schema change and will trigger shadow reconcile for that table

## Sync And Reconcile Behavior

### Initial Sync

After registration, the source is queued for sync immediately.

The initial sync path:

1. creates or confirms the target schema
2. snapshots matching CouchDB documents through `_changes`
3. writes rows into PostgreSQL
4. transitions into steady-state long-poll listening

### Steady-State Sync

For ordinary document updates:

- matching documents are projected into their configured target table
- deletes remove rows by `_id`
- sync state is persisted in the admin metadata database

### Design Changes

When `_design/couchsql` changes:

- the design listener detects the new revision
- the schema reconciler compares the previously applied type definitions to the new ones
- unchanged tables are left alone
- index-only and simple in-place changes are applied directly where possible
- rebuild-required tables are rebuilt through shadow tables and swapped atomically

Examples of rebuild-required changes:

- changing a field path
- changing a field type
- changing a field transform
- adding a mapped column that needs backfill
- changing a type identify rule
- changing a target table name

### Forced Resync

To replay a source from scratch:

```powershell
Invoke-RestMethod \
	-Method Post \
	-Uri http://127.0.0.1:8081/internal/v1/couchdb/connections/<connection-id>/resync
```

This operation:

- truncates managed target tables
- clears listener sequence state
- resets source metadata to `snapshotting`
- restarts the source supervisor

## Query Rules

The query endpoint is intentionally restricted.

Current query rules:

- only a single statement is allowed
- only `SELECT` and `WITH ... SELECT` are allowed
- DDL and DML keywords are blocked
- the API wraps the SQL in a limiting outer query
- row limits are enforced through query settings

If a query is rejected, inspect [src/CouchSql.Infrastructure/Query/SqlQueryValidator.cs](src/CouchSql.Infrastructure/Query/SqlQueryValidator.cs).

## Configuration Reference

### PostgreSql

- `Host`: PostgreSQL server host
- `Port`: PostgreSQL server port
- `SystemDatabase`: database used for cluster-level operations like `CREATE DATABASE`
- `AdminDatabase`: admin metadata database used by `couchSQL`
- `Username`: PostgreSQL login
- `Password`: PostgreSQL password

### Endpoints

- `Public`: bind URL for the query API
- `Admin`: bind URL for the admin API

### Query

- `DefaultRowLimit`: default result limit when the caller omits `rowLimit`
- `MaxRowLimit`: hard cap for query results
- `CommandTimeoutSeconds`: PostgreSQL command timeout

### Sync

- `SnapshotBatchSize`: batch size during initial or rebuild snapshot
- `SnapshotSeqInterval`: CouchDB `_changes` sequence interval during snapshot
- `LongpollHeartbeatMilliseconds`: long-poll heartbeat sent to CouchDB
- `LongpollReadTimeoutSeconds`: local read timeout for long-poll requests

### Security

- `EncryptionKeyPath`: path to the AES key used to encrypt stored CouchDB credentials

Important: the encryption key file is generated automatically if it does not exist. Keep it stable if you want previously stored credentials to remain decryptable.

## Operational Notes

### Database Naming

Target database names are normalized before use:

- lowercased
- unsupported characters become underscores
- duplicate underscores are collapsed
- empty results are rejected
- names that do not start with a letter or underscore are prefixed with `db_`
- names are truncated to 63 characters

### Logging

By default logs go to:

- console
- `./logs/couchsql-.log`

The CouchDB client also logs `_changes` requests at `Information` level, including:

- request URL
- feed mode
- `since` sequence
- selector payload

### Credential Storage

CouchDB credentials are not stored in plaintext. They are encrypted with a file-backed AES key. The key file defaults to `./keys/couchsql.key`.

## Development Workflow

Restore and build:

```powershell
dotnet restore
dotnet build .\CouchSql.slnx
```

Run tests:

```powershell
dotnet test .\tests\CouchSql.Tests\CouchSql.Tests.csproj
```

Run the API:

```powershell
dotnet run --project .\src\CouchSql.Api\CouchSql.Api.csproj
```

## Troubleshooting

### Admin Routes Return 404

Cause:

- the request is not coming from loopback

Fix:

- call admin routes from the same machine
- or change the middleware behavior intentionally before exposing admin remotely

### Registration Fails With Design Validation Errors

Cause:

- `_design/couchsql` is missing or does not match the contract

Fix:

- open the builder at `/internal/v1/design-documents/builder`
- paste or compose the JSON
- validate the design
- save the corrected document back into CouchDB

### Query Returns 404

Cause:

- the `databaseName` in the query request is not a managed target database name

Fix:

- call `GET /api/v1/databases`
- use the managed PostgreSQL database name from that response

### Removing A Source Fails Because The Database Is In Use

Cause:

- an external PostgreSQL session still has the target database open

Fix:

- close active sessions using that managed database
- retry the delete operation

### Swagger Fails At Runtime

Cause:

- an explicit `Microsoft.OpenApi` 3.x package reference was added alongside Swashbuckle

Fix:

- do not override Swashbuckle's compatible `Microsoft.OpenApi` dependency in this repo

## Additional Design Notes

For deeper implementation detail, see:

- [docs/01-initial-product-and-implementation-plan.md](docs/01-initial-product-and-implementation-plan.md)
- [docs/02-database-build-and-sync-design.md](docs/02-database-build-and-sync-design.md)