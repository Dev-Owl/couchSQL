using System.Text.Json;
using CouchSql.Core.Contracts;
using CouchSql.Core.Design;
using CouchSql.Core.Interfaces;
using CouchSql.Core.Models;
using CouchSql.Core.Options;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using CouchSql.Infrastructure.Sync;

namespace CouchSql.Infrastructure.Services;

public sealed class ConnectionRegistrationService(
    ICouchDbClient couchDbClient,
    IDesignContractValidator designContractValidator,
    IPostgreSqlService postgreSqlService,
    IAdminMetadataRepository adminMetadataRepository,
    ICredentialProtector credentialProtector,
    ISyncSupervisor syncSupervisor,
    IOptions<PostgreSqlOptions> postgreSqlOptions,
    ILogger<ConnectionRegistrationService> logger) : IConnectionRegistrationService
{
    public async Task<RegisterCouchDbConnectionResponse> RegisterAsync(RegisterCouchDbConnectionRequest request, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(request.BaseUrl) ||
            string.IsNullOrWhiteSpace(request.Username) ||
            string.IsNullOrWhiteSpace(request.PasswordOrToken) ||
            string.IsNullOrWhiteSpace(request.DatabaseName))
        {
            throw new InvalidOperationException("BaseUrl, Username, PasswordOrToken, and DatabaseName are required.");
        }

        var targetDatabaseName = DatabaseNameNormalizer.Normalize(request.TargetDatabaseName ?? request.DatabaseName);
        if (string.Equals(targetDatabaseName, postgreSqlOptions.Value.AdminDatabase, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException("The target database name cannot match the admin metadata database.");
        }

        if (await adminMetadataRepository.GetSourceByDatabaseNameAsync(targetDatabaseName, cancellationToken) is not null)
        {
            throw new InvalidOperationException($"The target database '{targetDatabaseName}' is already managed by couchSQL.");
        }

        await couchDbClient.ValidateDatabaseAsync(request.BaseUrl, request.DatabaseName, request.Username, request.PasswordOrToken, cancellationToken);
        var designDocument = await couchDbClient.GetDesignDocumentAsync(request.BaseUrl, request.DatabaseName, request.Username, request.PasswordOrToken, cancellationToken);
        designContractValidator.Validate(designDocument);

        await postgreSqlService.EnsureTargetDatabaseAsync(targetDatabaseName, cancellationToken);
        await postgreSqlService.BuildInitialSchemaAsync(targetDatabaseName, designDocument, cancellationToken);

        var now = DateTimeOffset.UtcNow;
        var sourceId = Guid.NewGuid();
        var schemaVersion = designDocument.CouchSql?.SchemaVersion ?? 1;
        var designRevision = designDocument.Revision ?? string.Empty;
        var tableNames = designDocument.CouchSql?.Types.Select(type => type.Table!).ToArray() ?? Array.Empty<string>();

        var source = new SourceRegistrationRecord(
            sourceId,
            request.BaseUrl.TrimEnd('/'),
            request.DatabaseName,
            targetDatabaseName,
            string.IsNullOrWhiteSpace(request.LogicalConnectionName) ? null : request.LogicalConnectionName,
            "pending",
            "_design/couchsql",
            designRevision,
            schemaVersion,
            now,
            now);

        var credentials = new CredentialRecord(sourceId, request.Username, credentialProtector.Protect(request.PasswordOrToken), credentialProtector.CurrentKeyId, now);

        var listenerState = new ListenerStateRecord(sourceId, null, null, designRevision, null, null, now);

        var schemaState = new SchemaStateRecord(
            sourceId,
            JsonSerializer.Serialize(designDocument.CouchSql?.Types?.ToArray() ?? Array.Empty<CouchSqlTypeDefinition>()),
            JsonSerializer.Serialize(tableNames),
            schemaVersion,
            designRevision,
            now);

        var tableStates = tableNames.Select(tableName => new TableStateRecord(
                sourceId,
                tableName,
                "snapshotting",
                null,
                false,
                "initial-load",
                null,
                null,
                0,
                designRevision,
                designRevision,
                null,
                now))
            .ToArray();

        await adminMetadataRepository.SaveRegistrationAsync(source, credentials, listenerState, schemaState, tableStates, cancellationToken);
        await syncSupervisor.EnqueueAsync(sourceId, cancellationToken);

        logger.LogInformation(
            "Registered CouchDB source {DatabaseName} at {BaseUrl} into PostgreSQL database {TargetDatabaseName}",
            request.DatabaseName,
            request.BaseUrl,
            targetDatabaseName);

        return new RegisterCouchDbConnectionResponse(sourceId, request.DatabaseName, targetDatabaseName, designRevision, tableNames, source.Status);
    }
}

public sealed class ConnectionRemovalService(
    IAdminMetadataRepository adminMetadataRepository,
    SyncStateRepository syncStateRepository,
    ISyncSupervisor syncSupervisor,
    IPostgreSqlService postgreSqlService,
    ILogger<ConnectionRemovalService> logger) : IConnectionRemovalService
{
    public async Task RemoveAsync(Guid sourceId, CancellationToken cancellationToken)
    {
        var source = await adminMetadataRepository.GetSourceAsync(sourceId, cancellationToken);
        if (source is null)
        {
            return;
        }

        await syncStateRepository.UpdateSourceStatusAsync(sourceId, "paused", source.ActiveDesignRevision, cancellationToken);
        await syncSupervisor.StopAsync(sourceId, cancellationToken);
        await postgreSqlService.DropTargetDatabaseAsync(source.TargetDatabaseName, cancellationToken);
        await adminMetadataRepository.RemoveSourceAsync(sourceId, cancellationToken);

        logger.LogInformation("Removed CouchDB source {SourceId} and dropped target database {TargetDatabaseName}", sourceId, source.TargetDatabaseName);
    }
}

public sealed class ConnectionResyncService(
    SyncStateRepository syncStateRepository,
    IPostgreSqlService postgreSqlService,
    ISyncSupervisor syncSupervisor,
    ILogger<ConnectionResyncService> logger) : IConnectionResyncService
{
    public async Task<ForceResyncConnectionResponse?> ForceResyncAsync(Guid sourceId, CancellationToken cancellationToken)
    {
        var snapshot = await syncStateRepository.GetSourceAsync(sourceId, cancellationToken);
        if (snapshot is null)
        {
            return null;
        }

        await postgreSqlService.TruncateManagedTablesAsync(
            snapshot.Source.TargetDatabaseName,
            syncStateRepository.ReadManagedTables(snapshot.SchemaState),
            cancellationToken);
        await syncStateRepository.ResetSourceForResyncAsync(sourceId, snapshot.Source.ActiveDesignRevision, cancellationToken);
        await syncSupervisor.RestartAsync(sourceId, cancellationToken);

        logger.LogInformation("Forced resync requested for CouchDB source {SourceId}", sourceId);

        return new ForceResyncConnectionResponse(
            sourceId,
            "snapshotting",
            "The source listener state was reset and a fresh snapshot has been queued.");
    }
}

public sealed class QuerySettingsService(
    IAdminMetadataRepository adminMetadataRepository,
    IOptions<QueryOptions> queryOptions) : IQuerySettingsService
{
    public Task<QuerySettingsState> GetAsync(CancellationToken cancellationToken)
    {
        return adminMetadataRepository.GetQuerySettingsAsync(cancellationToken);
    }

    public async Task<QuerySettingsState> UpdateAsync(UpdateQuerySettingsRequest request, CancellationToken cancellationToken)
    {
        if (request.DefaultRowLimit <= 0 || request.MaxRowLimit <= 0 || request.CommandTimeoutSeconds <= 0)
        {
            throw new InvalidOperationException("Query settings must be positive values.");
        }

        if (request.DefaultRowLimit > request.MaxRowLimit)
        {
            throw new InvalidOperationException("DefaultRowLimit cannot exceed MaxRowLimit.");
        }

        if (request.MaxRowLimit > queryOptions.Value.MaxRowLimit)
        {
            throw new InvalidOperationException($"MaxRowLimit cannot exceed the configured hard maximum of {queryOptions.Value.MaxRowLimit}.");
        }

        if (request.CommandTimeoutSeconds > queryOptions.Value.CommandTimeoutSeconds)
        {
            throw new InvalidOperationException($"CommandTimeoutSeconds cannot exceed the configured maximum of {queryOptions.Value.CommandTimeoutSeconds} seconds.");
        }

        var settings = new QuerySettingsState(request.DefaultRowLimit, request.MaxRowLimit, request.CommandTimeoutSeconds);
        await adminMetadataRepository.SaveQuerySettingsAsync(settings, cancellationToken);
        return settings;
    }
}

public sealed class StartupInitializer(
    IPostgreSqlService postgreSqlService,
    IAdminMetadataRepository adminMetadataRepository,
    ICredentialProtector credentialProtector,
    IOptions<QueryOptions> queryOptions,
    ILogger<StartupInitializer> logger) : IStartupInitializer
{
    public StartupInitializationResult Current { get; private set; } = StartupInitializationResult.NotStarted;

    public async Task<StartupInitializationResult> InitializeAsync(CancellationToken cancellationToken)
    {
        var messages = new List<string>();
        var postgreSqlAvailable = false;
        var adminDatabaseReady = false;
        var migrationsApplied = false;
        var encryptionKeyReady = false;

        try
        {
            await credentialProtector.EnsureKeyExistsAsync(cancellationToken);
            encryptionKeyReady = true;
            messages.Add("Credential encryption key is available.");

            await postgreSqlService.EnsureAdminDatabaseAsync(cancellationToken);
            postgreSqlAvailable = true;
            adminDatabaseReady = true;
            messages.Add("Admin metadata database is reachable.");

            await postgreSqlService.ApplyAdminMigrationsAsync(cancellationToken);
            migrationsApplied = true;
            messages.Add("Admin database migrations are up to date.");

            var defaultSettings = new QuerySettingsState(
                queryOptions.Value.DefaultRowLimit,
                queryOptions.Value.MaxRowLimit,
                queryOptions.Value.CommandTimeoutSeconds);
            await adminMetadataRepository.EnsureDefaultQuerySettingsAsync(defaultSettings, cancellationToken);
            messages.Add("Default query settings are initialized.");

            Current = new StartupInitializationResult(true, postgreSqlAvailable, adminDatabaseReady, migrationsApplied, encryptionKeyReady, messages);

            foreach (var message in messages)
            {
                logger.LogInformation("Startup self-check: {Message}", message);
            }

            return Current;
        }
        catch (Exception exception)
        {
            messages.Add(exception.Message);
            Current = new StartupInitializationResult(false, postgreSqlAvailable, adminDatabaseReady, migrationsApplied, encryptionKeyReady, messages);
            logger.LogError(exception, "Startup self-check failed.");
            throw;
        }
    }
}