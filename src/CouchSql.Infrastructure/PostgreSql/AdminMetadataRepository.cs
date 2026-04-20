using System.Text.Json;
using CouchSql.Core.Models;
using CouchSql.Core.Interfaces;
using CouchSql.Core.Options;
using Microsoft.Extensions.Options;
using Npgsql;

namespace CouchSql.Infrastructure.PostgreSql;

public sealed class AdminMetadataRepository(IOptions<PostgreSqlOptions> postgreSqlOptions) : IAdminMetadataRepository
{
    private readonly PostgreSqlOptions _options = postgreSqlOptions.Value;

    public async Task EnsureDefaultQuerySettingsAsync(QuerySettingsState settings, CancellationToken cancellationToken)
    {
        const string sql = """
            insert into query_settings(settings_key, default_row_limit, max_row_limit, command_timeout_seconds, updated_at_utc)
            values ('default', @defaultRowLimit, @maxRowLimit, @commandTimeoutSeconds, @updatedAtUtc)
            on conflict (settings_key) do nothing
            """;

        await using var connection = await OpenAdminConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("defaultRowLimit", settings.DefaultRowLimit);
        command.Parameters.AddWithValue("maxRowLimit", settings.MaxRowLimit);
        command.Parameters.AddWithValue("commandTimeoutSeconds", settings.CommandTimeoutSeconds);
        command.Parameters.AddWithValue("updatedAtUtc", DateTimeOffset.UtcNow);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<QuerySettingsState> GetQuerySettingsAsync(CancellationToken cancellationToken)
    {
        const string sql = """
            select default_row_limit, max_row_limit, command_timeout_seconds
            from query_settings
            where settings_key = 'default'
            """;

        await using var connection = await OpenAdminConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(sql, connection);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        if (!await reader.ReadAsync(cancellationToken))
        {
            throw new InvalidOperationException("Query settings were not initialized.");
        }

        return new QuerySettingsState(reader.GetInt32(0), reader.GetInt32(1), reader.GetInt32(2));
    }

    public async Task SaveQuerySettingsAsync(QuerySettingsState settings, CancellationToken cancellationToken)
    {
        const string sql = """
            update query_settings
            set default_row_limit = @defaultRowLimit,
                max_row_limit = @maxRowLimit,
                command_timeout_seconds = @commandTimeoutSeconds,
                updated_at_utc = @updatedAtUtc
            where settings_key = 'default'
            """;

        await using var connection = await OpenAdminConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("defaultRowLimit", settings.DefaultRowLimit);
        command.Parameters.AddWithValue("maxRowLimit", settings.MaxRowLimit);
        command.Parameters.AddWithValue("commandTimeoutSeconds", settings.CommandTimeoutSeconds);
        command.Parameters.AddWithValue("updatedAtUtc", DateTimeOffset.UtcNow);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<ManagedDatabaseSummary>> GetManagedDatabasesAsync(CancellationToken cancellationToken)
    {
        const string sql = """
            select source_id, target_database_name, logical_name, status
            from couch_sources
            order by target_database_name
            """;

        await using var connection = await OpenAdminConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(sql, connection);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        var results = new List<ManagedDatabaseSummary>();
        while (await reader.ReadAsync(cancellationToken))
        {
            results.Add(new ManagedDatabaseSummary(
                reader.GetGuid(0),
                reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.GetString(3)));
        }

        return results;
    }

    public async Task<bool> DatabaseIsManagedAsync(string databaseName, CancellationToken cancellationToken)
    {
        const string sql = "select 1 from couch_sources where lower(target_database_name) = lower(@databaseName)";
        await using var connection = await OpenAdminConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("databaseName", databaseName);
        return await command.ExecuteScalarAsync(cancellationToken) is not null;
    }

    public async Task<IReadOnlyList<string>> GetQueryableTablesAsync(string databaseName, CancellationToken cancellationToken)
    {
        const string sql = """
            select ts.table_name
            from couch_table_state ts
            inner join couch_sources s on s.source_id = ts.source_id
            where lower(s.target_database_name) = lower(@databaseName)
            order by ts.table_name
            """;

        await using var connection = await OpenAdminConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("databaseName", databaseName);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        var results = new List<string>();
        while (await reader.ReadAsync(cancellationToken))
        {
            results.Add(reader.GetString(0));
        }

        return results;
    }

    public async Task<TableStateRecord?> GetTableStateAsync(Guid sourceId, string tableName, CancellationToken cancellationToken)
    {
        const string sql = """
            select source_id, table_name, state, shadow_table_name, has_shadow_table, snapshot_mode,
                   current_sequence, pending_changes, processed_row_count, active_design_revision,
                   last_applied_design_revision, last_error, updated_at_utc
            from couch_table_state
            where source_id = @sourceId and lower(table_name) = lower(@tableName)
            """;

        await using var connection = await OpenAdminConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("sourceId", sourceId);
        command.Parameters.AddWithValue("tableName", tableName);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new TableStateRecord(
            reader.GetGuid(0),
            reader.GetString(1),
            reader.GetString(2),
            reader.IsDBNull(3) ? null : reader.GetString(3),
            reader.GetBoolean(4),
            reader.IsDBNull(5) ? null : reader.GetString(5),
            reader.IsDBNull(6) ? null : reader.GetString(6),
            reader.IsDBNull(7) ? null : reader.GetInt64(7),
            reader.IsDBNull(8) ? null : reader.GetInt64(8),
            reader.IsDBNull(9) ? null : reader.GetString(9),
            reader.IsDBNull(10) ? null : reader.GetString(10),
            reader.IsDBNull(11) ? null : reader.GetString(11),
            reader.GetFieldValue<DateTimeOffset>(12));
    }

    public async Task<SourceRegistrationRecord?> GetSourceAsync(Guid sourceId, CancellationToken cancellationToken)
    {
        const string sql = """
            select source_id, base_url, database_name, target_database_name, logical_name, status,
                   design_document_id, active_design_revision, schema_version, created_at_utc, updated_at_utc
            from couch_sources
            where source_id = @sourceId
            """;

        await using var connection = await OpenAdminConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("sourceId", sourceId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        return await reader.ReadAsync(cancellationToken)
            ? ReadSource(reader)
            : null;
    }

    public async Task<SourceRegistrationRecord?> GetSourceByDatabaseNameAsync(string databaseName, CancellationToken cancellationToken)
    {
        const string sql = """
            select source_id, base_url, database_name, target_database_name, logical_name, status,
                   design_document_id, active_design_revision, schema_version, created_at_utc, updated_at_utc
            from couch_sources
            where lower(target_database_name) = lower(@databaseName)
            """;

        await using var connection = await OpenAdminConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("databaseName", databaseName);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        return await reader.ReadAsync(cancellationToken)
            ? ReadSource(reader)
            : null;
    }

    public async Task SaveRegistrationAsync(
        SourceRegistrationRecord source,
        CredentialRecord credentials,
        ListenerStateRecord listenerState,
        SchemaStateRecord schemaState,
        IReadOnlyList<TableStateRecord> tableStates,
        CancellationToken cancellationToken)
    {
        await using var connection = await OpenAdminConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        try
        {
            await UpsertSourceAsync(connection, transaction, source, cancellationToken);
            await UpsertCredentialAsync(connection, transaction, credentials, cancellationToken);
            await UpsertListenerStateAsync(connection, transaction, listenerState, cancellationToken);
            await UpsertSchemaStateAsync(connection, transaction, schemaState, cancellationToken);
            await ReplaceTableStatesAsync(connection, transaction, source.SourceId, tableStates, cancellationToken);

            await transaction.CommitAsync(cancellationToken);
        }
        catch
        {
            await transaction.RollbackAsync(cancellationToken);
            throw;
        }
    }

    public async Task RemoveSourceAsync(Guid sourceId, CancellationToken cancellationToken)
    {
        await using var connection = await OpenAdminConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        try
        {
            foreach (var table in new[]
                     {
                         "couch_table_state",
                         "couch_source_schema_state",
                         "couch_source_listener_state",
                         "couch_source_credentials",
                         "couch_sources"
                     })
            {
                var sql = $"delete from {table} where source_id = @sourceId";
                await using var command = new NpgsqlCommand(sql, connection, transaction);
                command.Parameters.AddWithValue("sourceId", sourceId);
                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            await transaction.CommitAsync(cancellationToken);
        }
        catch
        {
            await transaction.RollbackAsync(cancellationToken);
            throw;
        }
    }

    private static SourceRegistrationRecord ReadSource(NpgsqlDataReader reader)
    {
        return new SourceRegistrationRecord(
            reader.GetGuid(0),
            reader.GetString(1),
            reader.GetString(2),
            reader.GetString(3),
            reader.IsDBNull(4) ? null : reader.GetString(4),
            reader.GetString(5),
            reader.GetString(6),
            reader.GetString(7),
            reader.GetInt32(8),
            reader.GetFieldValue<DateTimeOffset>(9),
            reader.GetFieldValue<DateTimeOffset>(10));
    }

    private static async Task UpsertSourceAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, SourceRegistrationRecord source, CancellationToken cancellationToken)
    {
        const string sql = """
            insert into couch_sources
            (
                source_id, base_url, database_name, target_database_name, logical_name, status,
                design_document_id, active_design_revision, schema_version, created_at_utc, updated_at_utc
            )
            values
            (
                @sourceId, @baseUrl, @databaseName, @targetDatabaseName, @logicalName, @status,
                @designDocumentId, @activeDesignRevision, @schemaVersion, @createdAtUtc, @updatedAtUtc
            )
            on conflict (source_id) do update set
                base_url = excluded.base_url,
                database_name = excluded.database_name,
                target_database_name = excluded.target_database_name,
                logical_name = excluded.logical_name,
                status = excluded.status,
                design_document_id = excluded.design_document_id,
                active_design_revision = excluded.active_design_revision,
                schema_version = excluded.schema_version,
                updated_at_utc = excluded.updated_at_utc
            """;

        await using var command = new NpgsqlCommand(sql, connection, transaction);
        command.Parameters.AddWithValue("sourceId", source.SourceId);
        command.Parameters.AddWithValue("baseUrl", source.BaseUrl);
        command.Parameters.AddWithValue("databaseName", source.DatabaseName);
        command.Parameters.AddWithValue("targetDatabaseName", source.TargetDatabaseName);
        command.Parameters.AddWithValue("logicalName", (object?)source.LogicalName ?? DBNull.Value);
        command.Parameters.AddWithValue("status", source.Status);
        command.Parameters.AddWithValue("designDocumentId", source.DesignDocumentId);
        command.Parameters.AddWithValue("activeDesignRevision", source.ActiveDesignRevision);
        command.Parameters.AddWithValue("schemaVersion", source.SchemaVersion);
        command.Parameters.AddWithValue("createdAtUtc", source.CreatedAt);
        command.Parameters.AddWithValue("updatedAtUtc", source.UpdatedAt);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task UpsertCredentialAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, CredentialRecord credentials, CancellationToken cancellationToken)
    {
        const string sql = """
            insert into couch_source_credentials(source_id, username, encrypted_secret, key_id, created_at_utc)
            values (@sourceId, @username, @encryptedSecret, @keyId, @createdAtUtc)
            on conflict (source_id) do update set
                username = excluded.username,
                encrypted_secret = excluded.encrypted_secret,
                key_id = excluded.key_id
            """;

        await using var command = new NpgsqlCommand(sql, connection, transaction);
        command.Parameters.AddWithValue("sourceId", credentials.SourceId);
        command.Parameters.AddWithValue("username", credentials.Username);
        command.Parameters.AddWithValue("encryptedSecret", credentials.EncryptedSecret);
        command.Parameters.AddWithValue("keyId", credentials.KeyId);
        command.Parameters.AddWithValue("createdAtUtc", credentials.CreatedAt);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task UpsertListenerStateAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, ListenerStateRecord listenerState, CancellationToken cancellationToken)
    {
        const string sql = """
            insert into couch_source_listener_state
            (
                source_id, design_sequence, data_sequence, last_design_revision,
                last_heartbeat_at_utc, last_error, updated_at_utc
            )
            values
            (
                @sourceId, @designSequence, @dataSequence, @lastDesignRevision,
                @lastHeartbeatAtUtc, @lastError, @updatedAtUtc
            )
            on conflict (source_id) do update set
                design_sequence = excluded.design_sequence,
                data_sequence = excluded.data_sequence,
                last_design_revision = excluded.last_design_revision,
                last_heartbeat_at_utc = excluded.last_heartbeat_at_utc,
                last_error = excluded.last_error,
                updated_at_utc = excluded.updated_at_utc
            """;

        await using var command = new NpgsqlCommand(sql, connection, transaction);
        command.Parameters.AddWithValue("sourceId", listenerState.SourceId);
        command.Parameters.AddWithValue("designSequence", (object?)listenerState.DesignSequence ?? DBNull.Value);
        command.Parameters.AddWithValue("dataSequence", (object?)listenerState.DataSequence ?? DBNull.Value);
        command.Parameters.AddWithValue("lastDesignRevision", (object?)listenerState.LastDesignRevision ?? DBNull.Value);
        command.Parameters.AddWithValue("lastHeartbeatAtUtc", (object?)listenerState.LastHeartbeatAt ?? DBNull.Value);
        command.Parameters.AddWithValue("lastError", (object?)listenerState.LastError ?? DBNull.Value);
        command.Parameters.AddWithValue("updatedAtUtc", listenerState.UpdatedAt);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task UpsertSchemaStateAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, SchemaStateRecord schemaState, CancellationToken cancellationToken)
    {
        const string sql = """
            insert into couch_source_schema_state
            (
                source_id, applied_type_definitions_json, table_definitions_json,
                applied_schema_version, last_applied_design_revision, applied_at_utc
            )
            values
            (
                @sourceId, @appliedTypeDefinitionsJson, @tableDefinitionsJson,
                @appliedSchemaVersion, @lastAppliedDesignRevision, @appliedAtUtc
            )
            on conflict (source_id) do update set
                applied_type_definitions_json = excluded.applied_type_definitions_json,
                table_definitions_json = excluded.table_definitions_json,
                applied_schema_version = excluded.applied_schema_version,
                last_applied_design_revision = excluded.last_applied_design_revision,
                applied_at_utc = excluded.applied_at_utc
            """;

        await using var command = new NpgsqlCommand(sql, connection, transaction);
        command.Parameters.AddWithValue("sourceId", schemaState.SourceId);
        command.Parameters.AddWithValue("appliedTypeDefinitionsJson", JsonDocument.Parse(schemaState.AppliedTypeDefinitionsJson));
        command.Parameters.AddWithValue("tableDefinitionsJson", JsonDocument.Parse(schemaState.TableDefinitionsJson));
        command.Parameters.AddWithValue("appliedSchemaVersion", schemaState.AppliedSchemaVersion);
        command.Parameters.AddWithValue("lastAppliedDesignRevision", schemaState.LastAppliedDesignRevision);
        command.Parameters.AddWithValue("appliedAtUtc", schemaState.AppliedAt);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task ReplaceTableStatesAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        Guid sourceId,
        IReadOnlyList<TableStateRecord> tableStates,
        CancellationToken cancellationToken)
    {
        await using (var deleteCommand = new NpgsqlCommand("delete from couch_table_state where source_id = @sourceId", connection, transaction))
        {
            deleteCommand.Parameters.AddWithValue("sourceId", sourceId);
            await deleteCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        const string insertSql = """
            insert into couch_table_state
            (
                source_id, table_name, state, shadow_table_name, has_shadow_table, snapshot_mode,
                current_sequence, pending_changes, processed_row_count, active_design_revision,
                last_applied_design_revision, last_error, updated_at_utc
            )
            values
            (
                @sourceId, @tableName, @state, @shadowTableName, @hasShadowTable, @snapshotMode,
                @currentSequence, @pendingChanges, @processedRowCount, @activeDesignRevision,
                @lastAppliedDesignRevision, @lastError, @updatedAtUtc
            )
            """;

        foreach (var tableState in tableStates)
        {
            await using var insertCommand = new NpgsqlCommand(insertSql, connection, transaction);
            insertCommand.Parameters.AddWithValue("sourceId", tableState.SourceId);
            insertCommand.Parameters.AddWithValue("tableName", tableState.TableName);
            insertCommand.Parameters.AddWithValue("state", tableState.State);
            insertCommand.Parameters.AddWithValue("shadowTableName", (object?)tableState.ShadowTableName ?? DBNull.Value);
            insertCommand.Parameters.AddWithValue("hasShadowTable", tableState.HasShadowTable);
            insertCommand.Parameters.AddWithValue("snapshotMode", (object?)tableState.SnapshotMode ?? DBNull.Value);
            insertCommand.Parameters.AddWithValue("currentSequence", (object?)tableState.CurrentSequence ?? DBNull.Value);
            insertCommand.Parameters.AddWithValue("pendingChanges", (object?)tableState.PendingChanges ?? DBNull.Value);
            insertCommand.Parameters.AddWithValue("processedRowCount", (object?)tableState.ProcessedRowCount ?? DBNull.Value);
            insertCommand.Parameters.AddWithValue("activeDesignRevision", (object?)tableState.ActiveDesignRevision ?? DBNull.Value);
            insertCommand.Parameters.AddWithValue("lastAppliedDesignRevision", (object?)tableState.LastAppliedDesignRevision ?? DBNull.Value);
            insertCommand.Parameters.AddWithValue("lastError", (object?)tableState.LastError ?? DBNull.Value);
            insertCommand.Parameters.AddWithValue("updatedAtUtc", tableState.UpdatedAt);
            await insertCommand.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private async Task<NpgsqlConnection> OpenAdminConnectionAsync(CancellationToken cancellationToken)
    {
        var connectionString = new NpgsqlConnectionStringBuilder
        {
            Host = _options.Host,
            Port = _options.Port,
            Database = _options.AdminDatabase,
            Username = _options.Username,
            Password = _options.Password,
            Pooling = true
        }.ConnectionString;

        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        return connection;
    }
}