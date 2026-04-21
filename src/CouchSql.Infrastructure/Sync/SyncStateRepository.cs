using System.Text.Json;
using CouchSql.Core.Models;
using CouchSql.Core.Options;
using Microsoft.Extensions.Options;
using Npgsql;

namespace CouchSql.Infrastructure.Sync;

public sealed class SyncStateRepository(IOptions<PostgreSqlOptions> postgreSqlOptions)
{
    private readonly PostgreSqlOptions _options = postgreSqlOptions.Value;

    public async Task<IReadOnlyList<SyncSourceRegistrationSnapshot>> GetStartupSourcesAsync(CancellationToken cancellationToken)
    {
        const string sql = """
            select s.source_id, s.base_url, s.database_name, s.target_database_name, s.logical_name, s.status,
                   s.design_document_id, s.active_design_revision, s.schema_version, s.created_at_utc, s.updated_at_utc,
                   c.username, c.encrypted_secret, c.key_id, c.created_at_utc,
                   ls.design_sequence, ls.data_sequence, ls.last_design_revision, ls.last_heartbeat_at_utc, ls.last_error, ls.updated_at_utc,
                   ss.applied_type_definitions_json::text, ss.table_definitions_json::text, ss.applied_schema_version, ss.last_applied_design_revision, ss.applied_at_utc
            from couch_sources s
            inner join couch_source_credentials c on c.source_id = s.source_id
            inner join couch_source_listener_state ls on ls.source_id = s.source_id
            inner join couch_source_schema_state ss on ss.source_id = s.source_id
            order by s.target_database_name
            """;

        var results = new List<SyncSourceRegistrationSnapshot>();
        await using var connection = await OpenAdminConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(sql, connection);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            results.Add(ReadSnapshot(reader));
        }

        return results;
    }

    public async Task<SyncSourceRegistrationSnapshot?> GetSourceAsync(Guid sourceId, CancellationToken cancellationToken)
    {
        const string sql = """
            select s.source_id, s.base_url, s.database_name, s.target_database_name, s.logical_name, s.status,
                   s.design_document_id, s.active_design_revision, s.schema_version, s.created_at_utc, s.updated_at_utc,
                   c.username, c.encrypted_secret, c.key_id, c.created_at_utc,
                   ls.design_sequence, ls.data_sequence, ls.last_design_revision, ls.last_heartbeat_at_utc, ls.last_error, ls.updated_at_utc,
                   ss.applied_type_definitions_json::text, ss.table_definitions_json::text, ss.applied_schema_version, ss.last_applied_design_revision, ss.applied_at_utc
            from couch_sources s
            inner join couch_source_credentials c on c.source_id = s.source_id
            inner join couch_source_listener_state ls on ls.source_id = s.source_id
            inner join couch_source_schema_state ss on ss.source_id = s.source_id
            where s.source_id = @sourceId
            """;

        await using var connection = await OpenAdminConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("sourceId", sourceId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        return await reader.ReadAsync(cancellationToken) ? ReadSnapshot(reader) : null;
    }

    public async Task UpdateSourceStatusAsync(Guid sourceId, string status, string? activeDesignRevision, CancellationToken cancellationToken)
    {
        const string sql = """
            update couch_sources
            set status = @status,
                active_design_revision = coalesce(@activeDesignRevision, active_design_revision),
                updated_at_utc = @updatedAtUtc
            where source_id = @sourceId
            """;

        await using var connection = await OpenAdminConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("sourceId", sourceId);
        command.Parameters.AddWithValue("status", status);
        command.Parameters.AddWithValue("activeDesignRevision", (object?)activeDesignRevision ?? DBNull.Value);
        command.Parameters.AddWithValue("updatedAtUtc", DateTimeOffset.UtcNow);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task UpdateListenerStateAsync(ListenerStateRecord state, CancellationToken cancellationToken)
    {
        const string sql = """
            update couch_source_listener_state
            set design_sequence = @designSequence,
                data_sequence = @dataSequence,
                last_design_revision = @lastDesignRevision,
                last_heartbeat_at_utc = @lastHeartbeatAtUtc,
                last_error = @lastError,
                updated_at_utc = @updatedAtUtc
            where source_id = @sourceId
            """;

        await using var connection = await OpenAdminConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("sourceId", state.SourceId);
        command.Parameters.AddWithValue("designSequence", (object?)state.DesignSequence ?? DBNull.Value);
        command.Parameters.AddWithValue("dataSequence", (object?)state.DataSequence ?? DBNull.Value);
        command.Parameters.AddWithValue("lastDesignRevision", (object?)state.LastDesignRevision ?? DBNull.Value);
        command.Parameters.AddWithValue("lastHeartbeatAtUtc", (object?)state.LastHeartbeatAt ?? DBNull.Value);
        command.Parameters.AddWithValue("lastError", (object?)state.LastError ?? DBNull.Value);
        command.Parameters.AddWithValue("updatedAtUtc", state.UpdatedAt);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task UpdateDesignListenerAsync(
        Guid sourceId,
        string? designSequence,
        string? lastDesignRevision,
        DateTimeOffset? lastHeartbeatAt,
        string? lastError,
        CancellationToken cancellationToken)
    {
        const string sql = """
            update couch_source_listener_state
            set design_sequence = @designSequence,
                last_design_revision = @lastDesignRevision,
                last_heartbeat_at_utc = @lastHeartbeatAtUtc,
                last_error = @lastError,
                updated_at_utc = @updatedAtUtc
            where source_id = @sourceId
            """;

        await using var connection = await OpenAdminConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("sourceId", sourceId);
        command.Parameters.AddWithValue("designSequence", (object?)designSequence ?? DBNull.Value);
        command.Parameters.AddWithValue("lastDesignRevision", (object?)lastDesignRevision ?? DBNull.Value);
        command.Parameters.AddWithValue("lastHeartbeatAtUtc", (object?)lastHeartbeatAt ?? DBNull.Value);
        command.Parameters.AddWithValue("lastError", (object?)lastError ?? DBNull.Value);
        command.Parameters.AddWithValue("updatedAtUtc", DateTimeOffset.UtcNow);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task UpdateDataListenerAsync(
        Guid sourceId,
        string? dataSequence,
        DateTimeOffset? lastHeartbeatAt,
        string? lastError,
        CancellationToken cancellationToken)
    {
        const string sql = """
            update couch_source_listener_state
            set data_sequence = @dataSequence,
                last_heartbeat_at_utc = @lastHeartbeatAtUtc,
                last_error = @lastError,
                updated_at_utc = @updatedAtUtc
            where source_id = @sourceId
            """;

        await using var connection = await OpenAdminConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("sourceId", sourceId);
        command.Parameters.AddWithValue("dataSequence", (object?)dataSequence ?? DBNull.Value);
        command.Parameters.AddWithValue("lastHeartbeatAtUtc", (object?)lastHeartbeatAt ?? DBNull.Value);
        command.Parameters.AddWithValue("lastError", (object?)lastError ?? DBNull.Value);
        command.Parameters.AddWithValue("updatedAtUtc", DateTimeOffset.UtcNow);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task UpdateTableStatesAsync(IReadOnlyList<TableStateRecord> states, CancellationToken cancellationToken)
    {
        if (states.Count == 0)
        {
            return;
        }

        const string sql = """
            update couch_table_state
            set state = @state,
                shadow_table_name = @shadowTableName,
                has_shadow_table = @hasShadowTable,
                snapshot_mode = @snapshotMode,
                current_sequence = @currentSequence,
                pending_changes = @pendingChanges,
                processed_row_count = @processedRowCount,
                active_design_revision = @activeDesignRevision,
                last_applied_design_revision = @lastAppliedDesignRevision,
                last_error = @lastError,
                updated_at_utc = @updatedAtUtc
            where source_id = @sourceId and lower(table_name) = lower(@tableName)
            """;

        await using var connection = await OpenAdminConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        try
        {
            foreach (var state in states)
            {
                await using var command = new NpgsqlCommand(sql, connection, transaction);
                command.Parameters.AddWithValue("sourceId", state.SourceId);
                command.Parameters.AddWithValue("tableName", state.TableName);
                command.Parameters.AddWithValue("state", state.State);
                command.Parameters.AddWithValue("shadowTableName", (object?)state.ShadowTableName ?? DBNull.Value);
                command.Parameters.AddWithValue("hasShadowTable", state.HasShadowTable);
                command.Parameters.AddWithValue("snapshotMode", (object?)state.SnapshotMode ?? DBNull.Value);
                command.Parameters.AddWithValue("currentSequence", (object?)state.CurrentSequence ?? DBNull.Value);
                command.Parameters.AddWithValue("pendingChanges", (object?)state.PendingChanges ?? DBNull.Value);
                command.Parameters.AddWithValue("processedRowCount", (object?)state.ProcessedRowCount ?? DBNull.Value);
                command.Parameters.AddWithValue("activeDesignRevision", (object?)state.ActiveDesignRevision ?? DBNull.Value);
                command.Parameters.AddWithValue("lastAppliedDesignRevision", (object?)state.LastAppliedDesignRevision ?? DBNull.Value);
                command.Parameters.AddWithValue("lastError", (object?)state.LastError ?? DBNull.Value);
                command.Parameters.AddWithValue("updatedAtUtc", state.UpdatedAt);
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

    public async Task ResetSourceForResyncAsync(Guid sourceId, string activeDesignRevision, CancellationToken cancellationToken)
    {
        const string sourceSql = """
            update couch_sources
            set status = 'snapshotting',
                active_design_revision = @activeDesignRevision,
                updated_at_utc = @updatedAtUtc
            where source_id = @sourceId
            """;

        const string listenerSql = """
            update couch_source_listener_state
            set design_sequence = null,
                data_sequence = null,
                last_design_revision = @activeDesignRevision,
                last_heartbeat_at_utc = null,
                last_error = null,
                updated_at_utc = @updatedAtUtc
            where source_id = @sourceId
            """;

        const string tableSql = """
            update couch_table_state
            set state = 'snapshotting',
                shadow_table_name = null,
                has_shadow_table = false,
                snapshot_mode = 'initial-load',
                current_sequence = null,
                pending_changes = null,
                processed_row_count = 0,
                active_design_revision = @activeDesignRevision,
                last_applied_design_revision = @activeDesignRevision,
                last_error = null,
                updated_at_utc = @updatedAtUtc
            where source_id = @sourceId
            """;

        await using var connection = await OpenAdminConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        try
        {
            var updatedAtUtc = DateTimeOffset.UtcNow;

            await using (var command = new NpgsqlCommand(sourceSql, connection, transaction))
            {
                command.Parameters.AddWithValue("sourceId", sourceId);
                command.Parameters.AddWithValue("activeDesignRevision", activeDesignRevision);
                command.Parameters.AddWithValue("updatedAtUtc", updatedAtUtc);
                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            await using (var command = new NpgsqlCommand(listenerSql, connection, transaction))
            {
                command.Parameters.AddWithValue("sourceId", sourceId);
                command.Parameters.AddWithValue("activeDesignRevision", activeDesignRevision);
                command.Parameters.AddWithValue("updatedAtUtc", updatedAtUtc);
                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            await using (var command = new NpgsqlCommand(tableSql, connection, transaction))
            {
                command.Parameters.AddWithValue("sourceId", sourceId);
                command.Parameters.AddWithValue("activeDesignRevision", activeDesignRevision);
                command.Parameters.AddWithValue("updatedAtUtc", updatedAtUtc);
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

    public async Task ApplySchemaReconcileAsync(
        Guid sourceId,
        int schemaVersion,
        string sourceStatus,
        string activeDesignRevision,
        string? designSequence,
        string? dataSequence,
        SchemaStateRecord schemaState,
        IReadOnlyList<TableStateRecord> tableStates,
        CancellationToken cancellationToken)
    {
        const string sourceSql = """
            update couch_sources
            set status = @sourceStatus,
                active_design_revision = @activeDesignRevision,
                schema_version = @schemaVersion,
                updated_at_utc = @updatedAtUtc
            where source_id = @sourceId
            """;

        const string listenerSql = """
            update couch_source_listener_state
            set design_sequence = @designSequence,
                data_sequence = @dataSequence,
                last_design_revision = @activeDesignRevision,
                last_heartbeat_at_utc = null,
                last_error = null,
                updated_at_utc = @updatedAtUtc
            where source_id = @sourceId
            """;

        const string schemaSql = """
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

        const string deleteTableStatesSql = "delete from couch_table_state where source_id = @sourceId";
        const string insertTableStateSql = """
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

        await using var connection = await OpenAdminConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        try
        {
            var updatedAtUtc = DateTimeOffset.UtcNow;

            await using (var command = new NpgsqlCommand(sourceSql, connection, transaction))
            {
                command.Parameters.AddWithValue("sourceId", sourceId);
                command.Parameters.AddWithValue("schemaVersion", schemaVersion);
                command.Parameters.AddWithValue("sourceStatus", sourceStatus);
                command.Parameters.AddWithValue("activeDesignRevision", activeDesignRevision);
                command.Parameters.AddWithValue("updatedAtUtc", updatedAtUtc);
                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            await using (var command = new NpgsqlCommand(listenerSql, connection, transaction))
            {
                command.Parameters.AddWithValue("sourceId", sourceId);
                command.Parameters.AddWithValue("designSequence", (object?)designSequence ?? DBNull.Value);
                command.Parameters.AddWithValue("dataSequence", (object?)dataSequence ?? DBNull.Value);
                command.Parameters.AddWithValue("activeDesignRevision", activeDesignRevision);
                command.Parameters.AddWithValue("updatedAtUtc", updatedAtUtc);
                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            await using (var command = new NpgsqlCommand(schemaSql, connection, transaction))
            {
                command.Parameters.AddWithValue("sourceId", schemaState.SourceId);
                command.Parameters.AddWithValue("appliedTypeDefinitionsJson", JsonDocument.Parse(schemaState.AppliedTypeDefinitionsJson));
                command.Parameters.AddWithValue("tableDefinitionsJson", JsonDocument.Parse(schemaState.TableDefinitionsJson));
                command.Parameters.AddWithValue("appliedSchemaVersion", schemaState.AppliedSchemaVersion);
                command.Parameters.AddWithValue("lastAppliedDesignRevision", schemaState.LastAppliedDesignRevision);
                command.Parameters.AddWithValue("appliedAtUtc", schemaState.AppliedAt);
                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            await using (var deleteCommand = new NpgsqlCommand(deleteTableStatesSql, connection, transaction))
            {
                deleteCommand.Parameters.AddWithValue("sourceId", sourceId);
                await deleteCommand.ExecuteNonQueryAsync(cancellationToken);
            }

            foreach (var tableState in tableStates)
            {
                await using var insertCommand = new NpgsqlCommand(insertTableStateSql, connection, transaction);
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

            await transaction.CommitAsync(cancellationToken);
        }
        catch
        {
            await transaction.RollbackAsync(cancellationToken);
            throw;
        }
    }

    public IReadOnlyList<string> ReadManagedTables(SchemaStateRecord schemaState)
    {
        return JsonSerializer.Deserialize<string[]>(schemaState.TableDefinitionsJson) ?? Array.Empty<string>();
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

    private static SyncSourceRegistrationSnapshot ReadSnapshot(NpgsqlDataReader reader)
    {
        var source = new SourceRegistrationRecord(
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

        var credentials = new CredentialRecord(
            source.SourceId,
            reader.GetString(11),
            (byte[])reader[12],
            reader.GetString(13),
            reader.GetFieldValue<DateTimeOffset>(14));

        var listener = new ListenerStateRecord(
            source.SourceId,
            reader.IsDBNull(15) ? null : reader.GetString(15),
            reader.IsDBNull(16) ? null : reader.GetString(16),
            reader.IsDBNull(17) ? null : reader.GetString(17),
            reader.IsDBNull(18) ? null : reader.GetFieldValue<DateTimeOffset>(18),
            reader.IsDBNull(19) ? null : reader.GetString(19),
            reader.GetFieldValue<DateTimeOffset>(20));

        var schema = new SchemaStateRecord(
            source.SourceId,
            reader.GetString(21),
            reader.GetString(22),
            reader.GetInt32(23),
            reader.GetString(24),
            reader.GetFieldValue<DateTimeOffset>(25));

        return new SyncSourceRegistrationSnapshot(source, credentials, listener, schema);
    }
}

public sealed record SyncSourceRegistrationSnapshot(
    SourceRegistrationRecord Source,
    CredentialRecord Credentials,
    ListenerStateRecord ListenerState,
    SchemaStateRecord SchemaState);