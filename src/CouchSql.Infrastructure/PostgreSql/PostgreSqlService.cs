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
        await using var connection = await OpenSystemConnectionAsync(cancellationToken);

        const string terminateSql = """
            select pg_terminate_backend(pid)
            from pg_stat_activity
            where datname = @databaseName and pid <> pg_backend_pid()
            """;

        await using (var terminateCommand = new NpgsqlCommand(terminateSql, connection))
        {
            terminateCommand.Parameters.AddWithValue("databaseName", databaseName);
            await terminateCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        var dropSql = $"drop database if exists {QuoteIdentifier(databaseName)}";
        await using var dropCommand = new NpgsqlCommand(dropSql, connection);
        await dropCommand.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task BuildInitialSchemaAsync(string databaseName, CouchSqlDesignDocument designDocument, CancellationToken cancellationToken)
    {
        var configuration = designDocument.CouchSql ?? throw new InvalidOperationException("The design document did not contain the couchsql configuration object.");

        await using var connection = await OpenConnectionAsync(databaseName, cancellationToken);

        foreach (var type in configuration.Types)
        {
            var tableName = type.Table ?? throw new InvalidOperationException("A design type is missing the table name.");
            var createTableSql = BuildCreateTableSql(tableName, type.Fields);

            await using (var createTableCommand = new NpgsqlCommand(createTableSql, connection))
            {
                await createTableCommand.ExecuteNonQueryAsync(cancellationToken);
            }

            foreach (var index in type.Indexes)
            {
                var createIndexSql = BuildCreateIndexSql(tableName, index);
                await using var createIndexCommand = new NpgsqlCommand(createIndexSql, connection);
                await createIndexCommand.ExecuteNonQueryAsync(cancellationToken);
            }
        }
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
            .AppendLine("    \"_rev\" text not null");

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

    private static string BuildCreateIndexSql(string tableName, CouchSqlIndexDefinition index)
    {
        var uniquePrefix = index.Unique ? "unique " : string.Empty;
        var columns = string.Join(", ", index.Columns.Select(QuoteIdentifier));
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
}