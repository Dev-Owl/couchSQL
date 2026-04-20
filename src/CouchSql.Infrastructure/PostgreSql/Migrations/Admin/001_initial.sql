create table if not exists query_settings
(
    settings_key text primary key,
    default_row_limit integer not null,
    max_row_limit integer not null,
    command_timeout_seconds integer not null,
    updated_at_utc timestamptz not null
);

create table if not exists couch_sources
(
    source_id uuid primary key,
    base_url text not null,
    database_name text not null,
    target_database_name text not null unique,
    logical_name text null,
    status text not null,
    design_document_id text not null,
    active_design_revision text not null,
    schema_version integer not null,
    created_at_utc timestamptz not null,
    updated_at_utc timestamptz not null
);

create table if not exists couch_source_credentials
(
    source_id uuid primary key references couch_sources(source_id) on delete cascade,
    username text not null,
    encrypted_secret bytea not null,
    key_id text not null,
    created_at_utc timestamptz not null
);

create table if not exists couch_source_listener_state
(
    source_id uuid primary key references couch_sources(source_id) on delete cascade,
    design_sequence text null,
    data_sequence text null,
    last_design_revision text null,
    last_heartbeat_at_utc timestamptz null,
    last_error text null,
    updated_at_utc timestamptz not null
);

create table if not exists couch_source_schema_state
(
    source_id uuid primary key references couch_sources(source_id) on delete cascade,
    applied_type_definitions_json jsonb not null,
    table_definitions_json jsonb not null,
    applied_schema_version integer not null,
    last_applied_design_revision text not null,
    applied_at_utc timestamptz not null
);

create table if not exists couch_table_state
(
    source_id uuid not null references couch_sources(source_id) on delete cascade,
    table_name text not null,
    state text not null,
    shadow_table_name text null,
    has_shadow_table boolean not null,
    snapshot_mode text null,
    current_sequence text null,
    pending_changes bigint null,
    processed_row_count bigint null,
    active_design_revision text null,
    last_applied_design_revision text null,
    last_error text null,
    updated_at_utc timestamptz not null,
    primary key (source_id, table_name)
);