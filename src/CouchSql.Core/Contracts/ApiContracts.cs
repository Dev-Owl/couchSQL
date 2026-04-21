namespace CouchSql.Core.Contracts;

public sealed record QueryRequest(string DatabaseName, string Sql, int? RowLimit);

public sealed record QueryResponse(
    string DatabaseName,
    int EffectiveRowLimit,
    bool Truncated,
    IReadOnlyList<string> Columns,
    IReadOnlyList<IReadOnlyDictionary<string, object?>> Rows);

public sealed record ManagedDatabaseResponse(Guid ConnectionId, string DatabaseName, string? LogicalName, string Status);

public sealed record TableListResponse(string DatabaseName, IReadOnlyList<string> Tables);

public sealed record TableStructureResponse(
    string DatabaseName,
    string TableName,
    IReadOnlyList<TableColumnResponse> Columns,
    IReadOnlyList<TableIndexResponse> Indexes);

public sealed record TableColumnResponse(
    string Name,
    string PostgreSqlType,
    bool Nullable,
    int OrdinalPosition);

public sealed record TableIndexResponse(
    string Name,
    bool Unique,
    IReadOnlyList<string> Columns);

public sealed record QuerySettingsResponse(int DefaultRowLimit, int MaxRowLimit, int CommandTimeoutSeconds);

public sealed record UpdateQuerySettingsRequest(int DefaultRowLimit, int MaxRowLimit, int CommandTimeoutSeconds);

public sealed record RegisterCouchDbConnectionRequest(
    string BaseUrl,
    string Username,
    string PasswordOrToken,
    string DatabaseName,
    string? LogicalConnectionName,
    string? TargetDatabaseName);

public sealed record RegisterCouchDbConnectionResponse(
    Guid ConnectionId,
    string CouchDbDatabaseName,
    string TargetDatabaseName,
    string DesignRevision,
    IReadOnlyList<string> Tables,
    string Status);

public sealed record ForceResyncConnectionResponse(
    Guid ConnectionId,
    string Status,
    string Message);

public sealed record HealthResponse(
    bool Ready,
    bool PostgreSqlAvailable,
    bool AdminDatabaseReady,
    bool MigrationsApplied,
    bool EncryptionKeyReady,
    IReadOnlyList<string> Messages);