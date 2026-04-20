# couchSQL

Connect CouchDB to PostgreSQL so replicated data can be queried with SQL.

## Current scope

This repository currently contains the initial product documentation and implementation plan for the API foundation.

The first planning pass covers:
- the .NET API shape
- endpoint separation between query and admin operations
- Swagger exposure strategy
- initial configuration boundaries
- phased implementation planning

The first planning pass intentionally does not yet define:
- the replication algorithm
- the CouchDB to PostgreSQL schema mapping strategy
- high-volume synchronization internals

## Documentation

- [Initial Product And Implementation Plan](docs/01-initial-product-and-implementation-plan.md)
- [Database Build And Sync Design](docs/02-database-build-and-sync-design.md)
