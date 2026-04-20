namespace CouchSql.Core.Models;

public sealed record QuerySettingsState(int DefaultRowLimit, int MaxRowLimit, int CommandTimeoutSeconds);

public sealed record StartupInitializationResult(
    bool Ready,
    bool PostgreSqlAvailable,
    bool AdminDatabaseReady,
    bool MigrationsApplied,
    bool EncryptionKeyReady,
    IReadOnlyList<string> Messages)
{
    public static StartupInitializationResult NotStarted { get; } = new(false, false, false, false, false, Array.Empty<string>());
}

public sealed record ManagedDatabaseSummary(Guid ConnectionId, string DatabaseName, string? LogicalName, string Status);

public sealed record SourceRegistrationRecord(
    Guid SourceId,
    string BaseUrl,
    string DatabaseName,
    string TargetDatabaseName,
    string? LogicalName,
    string Status,
    string DesignDocumentId,
    string ActiveDesignRevision,
    int SchemaVersion,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt);

public sealed record CredentialRecord(Guid SourceId, string Username, byte[] EncryptedSecret, string KeyId, DateTimeOffset CreatedAt);

public sealed record ListenerStateRecord(
    Guid SourceId,
    string? DesignSequence,
    string? DataSequence,
    string? LastDesignRevision,
    DateTimeOffset? LastHeartbeatAt,
    string? LastError,
    DateTimeOffset UpdatedAt);

public sealed record SchemaStateRecord(
    Guid SourceId,
    string AppliedTypeDefinitionsJson,
    string TableDefinitionsJson,
    int AppliedSchemaVersion,
    string LastAppliedDesignRevision,
    DateTimeOffset AppliedAt);

public sealed record TableStateRecord(
    Guid SourceId,
    string TableName,
    string State,
    string? ShadowTableName,
    bool HasShadowTable,
    string? SnapshotMode,
    string? CurrentSequence,
    long? PendingChanges,
    long? ProcessedRowCount,
    string? ActiveDesignRevision,
    string? LastAppliedDesignRevision,
    string? LastError,
    DateTimeOffset UpdatedAt);

public sealed record QueryExecutionResult(
    IReadOnlyList<string> Columns,
    IReadOnlyList<IReadOnlyDictionary<string, object?>> Rows,
    bool Truncated,
    int EffectiveRowLimit);

public sealed record ValidatedSqlQuery(string NormalizedSql, string WrappedSql);