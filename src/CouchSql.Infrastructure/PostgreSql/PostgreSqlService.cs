using System.Data;
using System.Text;
using CouchSql.Core.Contracts;
using CouchSql.Core.Design;
using CouchSql.Core.Interfaces;
using CouchSql.Core.Models;
using CouchSql.Core.Options;
using Microsoft.Extensions.Options;
using Npgsql;

namespace CouchSql.Infrastructure.PostgreSql;

public sealed class PostgreSqlService(
    IOptions<PostgreSqlOptions> postgreSqlOptions,
    ISqlQueryValidator sqlQueryValidator) : IPostgreSqlService
{
    private readonly PostgreSqlOptions _options = postgreSqlOptions.Value;

    public async Task EnsureAdminDatabaseAsync(CancellationToken cancellationToken)
    {
        await using var connection = await OpenSystemConnectionAsync(cancellationToken);
        await using var existsCommand = new NpgsqlCommand("select 1 from pg_database where datname = @databaseName", connection);
        existsCommand.Parameters.AddWithValue("databaseName", _options.AdminDatabase);

        var exists = await existsCommand.ExecuteScalarAsync(cancellationToken) is not null;
        if (exists)
        {
            return;
        }

        var createSql = $"create database {QuoteIdentifier(_options.AdminDatabase)}";
        await using var createCommand = new NpgsqlCommand(createSql, connection);
        await createCommand.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task ApplyAdminMigrationsAsync(CancellationToken cancellationToken)
    {
        await using var connection = await OpenAdminConnectionAsync(cancellationToken);

        const string ensureMigrationsTableSql = """
            create table if not exists schema_migrations
            (
                script_name text primary key,
                applied_at_utc timestamptz not null
            )
            """;

        await using (var ensureCommand = new NpgsqlCommand(ensureMigrationsTableSql, connection))
        {
            await ensureCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        var migrationsDirectory = Path.Combine(AppContext.BaseDirectory, "PostgreSql", "Migrations", "Admin");
        if (!Directory.Exists(migrationsDirectory))
        {
            throw new InvalidOperationException($"Admin migration directory was not found: {migrationsDirectory}");
        }

        foreach (var scriptPath in Directory.GetFiles(migrationsDirectory, "*.sql").OrderBy(path => path, StringComparer.OrdinalIgnoreCase))
        {
            var scriptName = Path.GetFileName(scriptPath);
            if (await MigrationAlreadyAppliedAsync(connection, scriptName, cancellationToken))
            {
                continue;
            }

            var script = await File.ReadAllTextAsync(scriptPath, cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

            try
            {
                await using var migrationCommand = new NpgsqlCommand(script, connection, transaction);
                await migrationCommand.ExecuteNonQueryAsync(cancellationToken);

                await using var insertCommand = new NpgsqlCommand(
                    "insert into schema_migrations(script_name, applied_at_utc) values (@scriptName, @appliedAtUtc)",
                    connection,
                    transaction);
                insertCommand.Parameters.AddWithValue("scriptName", scriptName);
                insertCommand.Parameters.AddWithValue("appliedAtUtc", DateTimeOffset.UtcNow);
                await insertCommand.ExecuteNonQueryAsync(cancellationToken);

                await transaction.CommitAsync(cancellationToken);
            }
            catch
            {
                await transaction.RollbackAsync(cancellationToken);
                throw;
            }
        }
    }

    public async Task<bool> CanConnectToAdminDatabaseAsync(CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await OpenAdminConnectionAsync(cancellationToken);
            return connection.FullState.HasFlag(ConnectionState.Open);
        }
        catch
        {
            return false;
        }
    }

    public async Task EnsureTargetDatabaseAsync(string databaseName, CancellationToken cancellationToken)
    {
        await using var connection = await OpenSystemConnectionAsync(cancellationToken);
        await using var existsCommand = new NpgsqlCommand("select 1 from pg_database where datname = @databaseName", connection);
        existsCommand.Parameters.AddWithValue("databaseName", databaseName);

        var exists = await existsCommand.ExecuteScalarAsync(cancellationToken) is not null;
        if (exists)
        {
            return;
        }

        var createSql = $"create database {QuoteIdentifier(databaseName)}";
        await using var createCommand = new NpgsqlCommand(createSql, connection);
        await createCommand.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task DropTargetDatabaseAsync(string databaseName, CancellationToken cancellationToken)
    {
        NpgsqlConnection.ClearAllPools();
        await using var connection = await OpenSystemConnectionAsync(cancellationToken);

        var dropSql = $"drop database if exists {QuoteIdentifier(databaseName)}";
        await using var dropCommand = new NpgsqlCommand(dropSql, connection);

        try
        {
            await dropCommand.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (PostgresException exception) when (string.Equals(exception.SqlState, "55006", StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"The target database '{databaseName}' is still in use by active PostgreSQL sessions. Close those sessions and try removing the connection again.",
                exception);
        }
    }

    public async Task DropManagedTablesAsync(string databaseName, IReadOnlyList<string> tableNames, CancellationToken cancellationToken)
    {
        if (tableNames.Count == 0)
        {
            return;
        }

        await using var connection = await OpenConnectionAsync(databaseName, cancellationToken);
        var joinedTableNames = string.Join(", ", tableNames.Select(QuoteIdentifier));
        var sql = $"drop table if exists {joinedTableNames} cascade";
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task DropManagedColumnsAsync(string databaseName, string tableName, IReadOnlyList<string> columnNames, CancellationToken cancellationToken)
    {
        if (columnNames.Count == 0)
        {
            return;
        }

        await using var connection = await OpenConnectionAsync(databaseName, cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        try
        {
            foreach (var columnName in columnNames)
            {
                var sql = $"alter table {QuoteIdentifier(tableName)} drop column if exists {QuoteIdentifier(columnName)}";
                await using var command = new NpgsqlCommand(sql, connection, transaction);
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

    public async Task RenameManagedColumnsAsync(string databaseName, string tableName, IReadOnlyDictionary<string, string> renamedColumns, CancellationToken cancellationToken)
    {
        if (renamedColumns.Count == 0)
        {
            return;
        }

        await using var connection = await OpenConnectionAsync(databaseName, cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        try
        {
            foreach (var rename in renamedColumns)
            {
                var sql = $"alter table {QuoteIdentifier(tableName)} rename column {QuoteIdentifier(rename.Key)} to {QuoteIdentifier(rename.Value)}";
                await using var command = new NpgsqlCommand(sql, connection, transaction);
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

    public async Task ReplaceManagedIndexesAsync(
        string databaseName,
        string tableName,
        IReadOnlyList<string> previousIndexNames,
        IReadOnlyList<CouchSqlFieldDefinition> fields,
        IReadOnlyList<CouchSqlIndexDefinition> desiredIndexes,
        CancellationToken cancellationToken)
    {
        var indexNamesToDrop = previousIndexNames
            .Concat(desiredIndexes.Select(index => index.Name))
            .Where(indexName => !string.IsNullOrWhiteSpace(indexName))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        if (indexNamesToDrop.Length == 0 && desiredIndexes.Count == 0)
        {
            return;
        }

        var knownColumns = CreateKnownColumnMap(fields);
        await using var connection = await OpenConnectionAsync(databaseName, cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        try
        {
            foreach (var indexName in indexNamesToDrop)
            {
                await using var dropCommand = new NpgsqlCommand($"drop index if exists {QuoteIdentifier(indexName!)}", connection, transaction);
                await dropCommand.ExecuteNonQueryAsync(cancellationToken);
            }

            foreach (var index in desiredIndexes)
            {
                var sql = BuildCreateIndexSql(tableName, index, knownColumns);
                await using var createCommand = new NpgsqlCommand(sql, connection, transaction);
                await createCommand.ExecuteNonQueryAsync(cancellationToken);
            }

            await transaction.CommitAsync(cancellationToken);
        }
        catch
        {
            await transaction.RollbackAsync(cancellationToken);
            throw;
        }
    }

    public async Task SwapShadowTablesAsync(string databaseName, IReadOnlyDictionary<string, string> shadowTablesByCanonicalTable, CancellationToken cancellationToken)
    {
        if (shadowTablesByCanonicalTable.Count == 0)
        {
            return;
        }

        await using var connection = await OpenConnectionAsync(databaseName, cancellationToken);
        var renamedOldTables = new List<string>();
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        try
        {
            foreach (var pair in shadowTablesByCanonicalTable)
            {
                var canonicalTable = pair.Key;
                var shadowTable = pair.Value;
                var oldTable = canonicalTable + "_old";

                var canonicalExists = await TableExistsAsync(connection, transaction, canonicalTable, cancellationToken);
                if (canonicalExists)
                {
                    await using (var dropOldCommand = new NpgsqlCommand($"drop table if exists {QuoteIdentifier(oldTable)} cascade", connection, transaction))
                    {
                        await dropOldCommand.ExecuteNonQueryAsync(cancellationToken);
                    }

                    await using (var renameCanonicalCommand = new NpgsqlCommand($"alter table {QuoteIdentifier(canonicalTable)} rename to {QuoteIdentifier(oldTable)}", connection, transaction))
                    {
                        await renameCanonicalCommand.ExecuteNonQueryAsync(cancellationToken);
                    }

                    renamedOldTables.Add(oldTable);
                }

                await using (var renameShadowCommand = new NpgsqlCommand($"alter table {QuoteIdentifier(shadowTable)} rename to {QuoteIdentifier(canonicalTable)}", connection, transaction))
                {
                    await renameShadowCommand.ExecuteNonQueryAsync(cancellationToken);
                }
            }

            await transaction.CommitAsync(cancellationToken);
        }
        catch
        {
            await transaction.RollbackAsync(cancellationToken);
            throw;
        }

        if (renamedOldTables.Count > 0)
        {
            await DropManagedTablesAsync(databaseName, renamedOldTables, cancellationToken);
        }
    }

    public async Task BuildInitialSchemaAsync(string databaseName, CouchSqlDesignDocument designDocument, CancellationToken cancellationToken)
    {
        var configuration = designDocument.CouchSql ?? throw new InvalidOperationException("The design document did not contain the couchsql configuration object.");

        await using var connection = await OpenConnectionAsync(databaseName, cancellationToken);

        foreach (var type in configuration.Types)
        {
            var tableName = type.Table ?? throw new InvalidOperationException("A design type is missing the table name.");
            var knownColumns = CreateKnownColumnMap(type.Fields);
            var createTableSql = BuildCreateTableSql(tableName, type.Fields);

            await using (var createTableCommand = new NpgsqlCommand(createTableSql, connection))
            {
                await createTableCommand.ExecuteNonQueryAsync(cancellationToken);
            }

            foreach (var alterSql in BuildEnsureManagedColumnsSql(tableName, type.Fields))
            {
                await using var alterCommand = new NpgsqlCommand(alterSql, connection);
                await alterCommand.ExecuteNonQueryAsync(cancellationToken);
            }

            foreach (var index in type.Indexes)
            {
                var createIndexSql = BuildCreateIndexSql(tableName, index, knownColumns);
                await using var createIndexCommand = new NpgsqlCommand(createIndexSql, connection);
                await createIndexCommand.ExecuteNonQueryAsync(cancellationToken);
            }
        }
    }

    public async Task TruncateManagedTablesAsync(string databaseName, IReadOnlyList<string> tableNames, CancellationToken cancellationToken)
    {
        if (tableNames.Count == 0)
        {
            return;
        }

        await using var connection = await OpenConnectionAsync(databaseName, cancellationToken);
        var joinedTableNames = string.Join(", ", tableNames.Select(QuoteIdentifier));
        var sql = $"truncate table {joinedTableNames}";
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<string>> GetTablesAsync(string databaseName, CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(databaseName, cancellationToken);
        const string sql = """
            select table_name
            from information_schema.tables
            where table_schema = 'public' and table_type = 'BASE TABLE'
            order by table_name
            """;

        await using var command = new NpgsqlCommand(sql, connection);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        var tables = new List<string>();
        while (await reader.ReadAsync(cancellationToken))
        {
            tables.Add(reader.GetString(0));
        }

        return tables;
    }

    public async Task<TableStructureResponse?> GetTableStructureAsync(string databaseName, string tableName, CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(databaseName, cancellationToken);

        const string columnsSql = """
            select
                column_name,
                udt_name,
                is_nullable,
                ordinal_position
            from information_schema.columns
            where table_schema = 'public' and lower(table_name) = lower(@tableName)
            order by ordinal_position
            """;

        var columns = new List<TableColumnResponse>();
        await using (var columnsCommand = new NpgsqlCommand(columnsSql, connection))
        {
            columnsCommand.Parameters.AddWithValue("tableName", tableName);
            await using var reader = await columnsCommand.ExecuteReaderAsync(cancellationToken);

            while (await reader.ReadAsync(cancellationToken))
            {
                columns.Add(new TableColumnResponse(
                    reader.GetString(0),
                    MapInformationSchemaType(reader.GetString(1)),
                    string.Equals(reader.GetString(2), "YES", StringComparison.OrdinalIgnoreCase),
                    reader.GetInt32(3)));
            }
        }

        if (columns.Count == 0)
        {
            return null;
        }

        const string indexesSql = """
            select
                indexname,
                indexdef
            from pg_indexes
            where schemaname = 'public' and lower(tablename) = lower(@tableName)
            order by indexname
            """;

        var indexes = new List<TableIndexResponse>();
        await using (var indexesCommand = new NpgsqlCommand(indexesSql, connection))
        {
            indexesCommand.Parameters.AddWithValue("tableName", tableName);
            await using var reader = await indexesCommand.ExecuteReaderAsync(cancellationToken);

            while (await reader.ReadAsync(cancellationToken))
            {
                var indexName = reader.GetString(0);
                var indexDefinition = reader.GetString(1);
                indexes.Add(new TableIndexResponse(
                    indexName,
                    indexDefinition.Contains("create unique index", StringComparison.OrdinalIgnoreCase),
                    ParseIndexedColumns(indexDefinition)));
            }
        }

        return new TableStructureResponse(databaseName, tableName, columns, indexes);
    }

    public async Task<QueryExecutionResult> ExecuteSelectAsync(QueryRequest request, QuerySettingsState settings, CancellationToken cancellationToken)
    {
        var requestedLimit = request.RowLimit ?? settings.DefaultRowLimit;
        if (requestedLimit <= 0)
        {
            throw new InvalidOperationException("The requested row limit must be greater than zero.");
        }

        var effectiveLimit = Math.Min(requestedLimit, settings.MaxRowLimit);
        var validatedQuery = sqlQueryValidator.Validate(request.Sql);

        await using var connection = await OpenConnectionAsync(request.DatabaseName, cancellationToken);
        await using var command = new NpgsqlCommand(validatedQuery.WrappedSql, connection)
        {
            CommandTimeout = settings.CommandTimeoutSeconds
        };
        command.Parameters.AddWithValue("__couchsql_limit", effectiveLimit + 1);

        await using var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken);

        var columns = new List<string>(reader.FieldCount);
        for (var index = 0; index < reader.FieldCount; index++)
        {
            columns.Add(reader.GetName(index));
        }

        var rows = new List<IReadOnlyDictionary<string, object?>>();
        var truncated = false;

        while (await reader.ReadAsync(cancellationToken))
        {
            if (rows.Count == effectiveLimit)
            {
                truncated = true;
                break;
            }

            var row = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            for (var index = 0; index < reader.FieldCount; index++)
            {
                var value = reader.IsDBNull(index) ? null : reader.GetValue(index);
                row[columns[index]] = value;
            }

            rows.Add(row);
        }

        return new QueryExecutionResult(columns, rows, truncated, effectiveLimit);
    }

    private async Task<bool> MigrationAlreadyAppliedAsync(NpgsqlConnection connection, string scriptName, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("select 1 from schema_migrations where script_name = @scriptName", connection);
        command.Parameters.AddWithValue("scriptName", scriptName);
        return await command.ExecuteScalarAsync(cancellationToken) is not null;
    }

    private static async Task<bool> TableExistsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string tableName, CancellationToken cancellationToken)
    {
        const string sql = "select to_regclass(@qualifiedTableName) is not null";
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        command.Parameters.AddWithValue("qualifiedTableName", $"public.{QuoteIdentifier(tableName)}");
        return await command.ExecuteScalarAsync(cancellationToken) as bool? == true;
    }

    private async Task<NpgsqlConnection> OpenSystemConnectionAsync(CancellationToken cancellationToken)
    {
        return await OpenConnectionAsync(_options.SystemDatabase, cancellationToken);
    }

    private async Task<NpgsqlConnection> OpenAdminConnectionAsync(CancellationToken cancellationToken)
    {
        return await OpenConnectionAsync(_options.AdminDatabase, cancellationToken);
    }

    private async Task<NpgsqlConnection> OpenConnectionAsync(string databaseName, CancellationToken cancellationToken)
    {
        var connectionString = new NpgsqlConnectionStringBuilder
        {
            Host = _options.Host,
            Port = _options.Port,
            Database = databaseName,
            Username = _options.Username,
            Password = _options.Password,
            Pooling = true
        }.ConnectionString;

        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        return connection;
    }

    private static string BuildCreateTableSql(string tableName, IReadOnlyCollection<CouchSqlFieldDefinition> fields)
    {
        var builder = new StringBuilder();
        builder.Append("create table if not exists ")
            .Append(QuoteIdentifier(tableName))
            .AppendLine(" (")
            .AppendLine("    \"_id\" text primary key,")
            .AppendLine("    \"_rev\" text not null,")
            .AppendLine("    \"_source_seq\" text not null,")
            .AppendLine("    \"_synced_at\" timestamptz not null");

        foreach (var field in fields)
        {
            builder.Append(",\n    ")
                .Append(QuoteIdentifier(field.Column ?? string.Empty))
                .Append(' ')
                .Append(MapPostgreSqlType(field.Type ?? string.Empty));

            if (field.Required)
            {
                builder.Append(" not null");
            }
        }

        builder.AppendLine().Append(')');
        return builder.ToString();
    }

    private static string BuildCreateIndexSql(string tableName, CouchSqlIndexDefinition index, IReadOnlyDictionary<string, string> knownColumns)
    {
        var uniquePrefix = index.Unique ? "unique " : string.Empty;
        var columns = string.Join(", ", index.Columns.Select(column => QuoteIdentifier(knownColumns[column])));
        return $"create {uniquePrefix}index if not exists {QuoteIdentifier(index.Name ?? string.Empty)} on {QuoteIdentifier(tableName)} ({columns})";
    }

    private static string MapPostgreSqlType(string configuredType)
    {
        return configuredType.ToLowerInvariant() switch
        {
            "text" => "text",
            "integer" => "integer",
            "bigint" => "bigint",
            "numeric" => "numeric",
            "boolean" => "boolean",
            "date" => "date",
            "timestamp" => "timestamp",
            "timestamptz" => "timestamptz",
            "jsonb" => "jsonb",
            "uuid" => "uuid",
            "double precision" => "double precision",
            _ => throw new InvalidOperationException($"Unsupported PostgreSQL type: {configuredType}")
        };
    }

    private static string QuoteIdentifier(string identifier)
    {
        return string.Concat('"', identifier.Replace("\"", "\"\"", StringComparison.Ordinal), '"');
    }

    private static IReadOnlyDictionary<string, string> CreateKnownColumnMap(IReadOnlyCollection<CouchSqlFieldDefinition> fields)
    {
        var knownColumns = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["_id"] = "_id",
            ["_rev"] = "_rev",
            ["_source_seq"] = "_source_seq",
            ["_synced_at"] = "_synced_at"
        };

        foreach (var field in fields)
        {
            if (!string.IsNullOrWhiteSpace(field.Column))
            {
                knownColumns[field.Column] = field.Column;
            }
        }

        return knownColumns;
    }

    private static string MapInformationSchemaType(string udtName)
    {
        return udtName.ToLowerInvariant() switch
        {
            "int2" => "smallint",
            "int4" => "integer",
            "int8" => "bigint",
            "float8" => "double precision",
            "float4" => "real",
            "bool" => "boolean",
            "timestamp" => "timestamp",
            "timestamptz" => "timestamptz",
            _ => udtName
        };
    }

    private static IReadOnlyList<string> ParseIndexedColumns(string indexDefinition)
    {
        var openParenIndex = indexDefinition.IndexOf('(');
        var closeParenIndex = indexDefinition.LastIndexOf(')');
        if (openParenIndex < 0 || closeParenIndex <= openParenIndex)
        {
            return Array.Empty<string>();
        }

        return indexDefinition[(openParenIndex + 1)..closeParenIndex]
            .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
            .Select(column => column.Trim().Trim('"'))
            .ToArray();
    }

    private static IEnumerable<string> BuildEnsureManagedColumnsSql(string tableName, IReadOnlyCollection<CouchSqlFieldDefinition> fields)
    {
        yield return $"alter table {QuoteIdentifier(tableName)} add column if not exists \"_source_seq\" text not null default ''";
        yield return $"alter table {QuoteIdentifier(tableName)} add column if not exists \"_synced_at\" timestamptz not null default now()";

        foreach (var field in fields)
        {
            var columnName = QuoteIdentifier(field.Column ?? string.Empty);
            var nullability = field.Required ? " not null" : string.Empty;
            yield return $"alter table {QuoteIdentifier(tableName)} add column if not exists {columnName} {MapPostgreSqlType(field.Type ?? string.Empty)}{nullability}";
        }
    }
}