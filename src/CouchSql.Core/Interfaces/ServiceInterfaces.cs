using CouchSql.Core.Contracts;
using CouchSql.Core.Design;
using CouchSql.Core.Models;

namespace CouchSql.Core.Interfaces;

public interface IStartupInitializer
{
    StartupInitializationResult Current { get; }

    Task<StartupInitializationResult> InitializeAsync(CancellationToken cancellationToken);
}

public interface ICouchDbClient
{
    Task ValidateDatabaseAsync(string baseUrl, string databaseName, string username, string passwordOrToken, CancellationToken cancellationToken);

    Task<CouchSqlDesignDocument> GetDesignDocumentAsync(string baseUrl, string databaseName, string username, string passwordOrToken, CancellationToken cancellationToken);
}

public interface ICredentialProtector
{
    string CurrentKeyId { get; }

    Task EnsureKeyExistsAsync(CancellationToken cancellationToken);

    byte[] Protect(string plaintext);

    string Unprotect(byte[] ciphertext, string? keyId);
}

public interface IAdminMetadataRepository
{
    Task EnsureDefaultQuerySettingsAsync(QuerySettingsState settings, CancellationToken cancellationToken);

    Task<QuerySettingsState> GetQuerySettingsAsync(CancellationToken cancellationToken);

    Task SaveQuerySettingsAsync(QuerySettingsState settings, CancellationToken cancellationToken);

    Task<IReadOnlyList<ManagedDatabaseSummary>> GetManagedDatabasesAsync(CancellationToken cancellationToken);

    Task<bool> DatabaseIsManagedAsync(string databaseName, CancellationToken cancellationToken);

    Task<IReadOnlyList<string>> GetQueryableTablesAsync(string databaseName, CancellationToken cancellationToken);

    Task<TableStateRecord?> GetTableStateAsync(Guid sourceId, string tableName, CancellationToken cancellationToken);

    Task<SourceRegistrationRecord?> GetSourceAsync(Guid sourceId, CancellationToken cancellationToken);

    Task<SourceRegistrationRecord?> GetSourceByDatabaseNameAsync(string databaseName, CancellationToken cancellationToken);

    Task SaveRegistrationAsync(
        SourceRegistrationRecord source,
        CredentialRecord credentials,
        ListenerStateRecord listenerState,
        SchemaStateRecord schemaState,
        IReadOnlyList<TableStateRecord> tableStates,
        CancellationToken cancellationToken);

    Task RemoveSourceAsync(Guid sourceId, CancellationToken cancellationToken);
}

public interface IPostgreSqlService
{
    Task EnsureAdminDatabaseAsync(CancellationToken cancellationToken);

    Task ApplyAdminMigrationsAsync(CancellationToken cancellationToken);

    Task<bool> CanConnectToAdminDatabaseAsync(CancellationToken cancellationToken);

    Task EnsureTargetDatabaseAsync(string databaseName, CancellationToken cancellationToken);

    Task DropTargetDatabaseAsync(string databaseName, CancellationToken cancellationToken);

    Task BuildInitialSchemaAsync(string databaseName, CouchSqlDesignDocument designDocument, CancellationToken cancellationToken);

    Task<IReadOnlyList<string>> GetTablesAsync(string databaseName, CancellationToken cancellationToken);

    Task<QueryExecutionResult> ExecuteSelectAsync(QueryRequest request, QuerySettingsState settings, CancellationToken cancellationToken);
}

public interface IDesignContractValidator
{
    void Validate(CouchSqlDesignDocument document);
}

public interface IConnectionRegistrationService
{
    Task<RegisterCouchDbConnectionResponse> RegisterAsync(RegisterCouchDbConnectionRequest request, CancellationToken cancellationToken);
}

public interface IConnectionRemovalService
{
    Task RemoveAsync(Guid sourceId, CancellationToken cancellationToken);
}

public interface IQuerySettingsService
{
    Task<QuerySettingsState> GetAsync(CancellationToken cancellationToken);

    Task<QuerySettingsState> UpdateAsync(UpdateQuerySettingsRequest request, CancellationToken cancellationToken);
}

public interface ISqlQueryValidator
{
    ValidatedSqlQuery Validate(string sql);
}