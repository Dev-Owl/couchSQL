using System.Text;
using CouchSql.Core.Design;
using CouchSql.Core.Options;
using Microsoft.Extensions.Options;
using Npgsql;

namespace CouchSql.Infrastructure.Sync;

public sealed class PostgreSqlProjectionWriter(IOptions<PostgreSqlOptions> postgreSqlOptions)
{
    private readonly PostgreSqlOptions _options = postgreSqlOptions.Value;

    public async Task UpsertDocumentAsync(
        string databaseName,
        CompiledTypeDefinition type,
        string documentId,
        string revision,
        string sourceSequence,
        IReadOnlyDictionary<string, object?> values,
        DateTimeOffset syncedAt,
        CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(databaseName, cancellationToken);
        await using var command = new NpgsqlCommand(BuildUpsertSql(type.Fields, type.Table), connection);

        command.Parameters.AddWithValue("_id", documentId);
        command.Parameters.AddWithValue("_rev", revision);
        command.Parameters.AddWithValue("_source_seq", sourceSequence);
        command.Parameters.AddWithValue("_synced_at", syncedAt);

        foreach (var field in type.Fields)
        {
            var parameterName = GetParameterName(field.Column ?? string.Empty);
            var value = values[field.Column ?? string.Empty];
            command.Parameters.AddWithValue(parameterName, value ?? DBNull.Value);
        }

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task DeleteDocumentAsync(string databaseName, IReadOnlyCollection<string> tableNames, string documentId, CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(databaseName, cancellationToken);
        foreach (var tableName in tableNames)
        {
            await using var command = new NpgsqlCommand($"delete from {QuoteIdentifier(tableName)} where \"_id\" = @documentId", connection);
            command.Parameters.AddWithValue("documentId", documentId);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
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

    private static string BuildUpsertSql(IReadOnlyList<CouchSqlFieldDefinition> fields, string tableName)
    {
        var columns = new List<string> { "\"_id\"", "\"_rev\"", "\"_source_seq\"", "\"_synced_at\"" };
        var values = new List<string> { "@_id", "@_rev", "@_source_seq", "@_synced_at" };
        var updates = new List<string>
        {
            "\"_rev\" = excluded.\"_rev\"",
            "\"_source_seq\" = excluded.\"_source_seq\"",
            "\"_synced_at\" = excluded.\"_synced_at\""
        };

        foreach (var field in fields)
        {
            var columnName = QuoteIdentifier(field.Column ?? string.Empty);
            var parameterName = "@" + GetParameterName(field.Column ?? string.Empty);
            var valueSql = string.Equals(field.Type, "jsonb", StringComparison.OrdinalIgnoreCase)
                ? $"cast({parameterName} as jsonb)"
                : parameterName;

            columns.Add(columnName);
            values.Add(valueSql);
            updates.Add($"{columnName} = excluded.{columnName}");
        }

        var builder = new StringBuilder();
        builder.Append("insert into ")
            .Append(QuoteIdentifier(tableName))
            .Append(" (")
            .Append(string.Join(", ", columns))
            .Append(") values (")
            .Append(string.Join(", ", values))
            .Append(") on conflict (\"_id\") do update set ")
            .Append(string.Join(", ", updates))
            .Append(" where ")
            .Append(QuoteIdentifier(tableName))
            .Append(".\"_rev\" is distinct from excluded.\"_rev\"");

        return builder.ToString();
    }

    private static string GetParameterName(string columnName)
    {
        return "p_" + columnName.Replace("_", "__", StringComparison.Ordinal);
    }

    private static string QuoteIdentifier(string identifier)
    {
        return string.Concat('"', identifier.Replace("\"", "\"\"", StringComparison.Ordinal), '"');
    }
}