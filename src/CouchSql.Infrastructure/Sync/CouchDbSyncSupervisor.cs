using System.Collections.Concurrent;
using System.Threading.Channels;
using System.Text.Json;
using CouchSql.Core.Design;
using CouchSql.Core.Interfaces;
using CouchSql.Core.Models;
using CouchSql.Core.Options;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CouchSql.Infrastructure.Sync;

public interface ISyncSupervisor
{
    Task EnqueueAsync(Guid sourceId, CancellationToken cancellationToken);

    Task RestartAsync(Guid sourceId, CancellationToken cancellationToken);

    Task StopAsync(Guid sourceId, CancellationToken cancellationToken);
}

public sealed class CouchDbSyncSupervisor(
    SyncStateRepository syncStateRepository,
    ICouchDbClient couchDbClient,
    ICredentialProtector credentialProtector,
    IPostgreSqlService postgreSqlService,
    IDesignContractValidator designContractValidator,
    SchemaReconciler schemaReconciler,
    PostgreSqlProjectionWriter projectionWriter,
    IOptions<SyncOptions> syncOptions,
    ILogger<CouchDbSyncSupervisor> logger) : BackgroundService, ISyncSupervisor
{
    private static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(15);

    private readonly SyncOptions _syncOptions = syncOptions.Value;
    private readonly Channel<Guid> _queue = Channel.CreateUnbounded<Guid>();
    private readonly ConcurrentDictionary<Guid, byte> _scheduled = new();
    private readonly ConcurrentDictionary<Guid, Task> _running = new();
    private readonly ConcurrentDictionary<Guid, CancellationTokenSource> _sourceStops = new();

    public async Task EnqueueAsync(Guid sourceId, CancellationToken cancellationToken)
    {
        if (!_scheduled.TryAdd(sourceId, 0))
        {
            return;
        }

        await _queue.Writer.WriteAsync(sourceId, cancellationToken);
    }

    public async Task RestartAsync(Guid sourceId, CancellationToken cancellationToken)
    {
        await StopAsync(sourceId, cancellationToken);

        await EnqueueAsync(sourceId, cancellationToken);
    }

    public async Task StopAsync(Guid sourceId, CancellationToken cancellationToken)
    {
        _scheduled.TryRemove(sourceId, out _);
        _sourceStops.TryGetValue(sourceId, out var sourceStop);

        if (sourceStop is not null)
        {
            sourceStop.Cancel();
        }

        if (_running.TryGetValue(sourceId, out var runningTask))
        {
            try
            {
                await runningTask.WaitAsync(cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (OperationCanceledException)
            {
            }

            CleanupCompletedSource(sourceId, runningTask, sourceStop);
            return;
        }

        CleanupCompletedSource(sourceId, null, sourceStop);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var existing = await syncStateRepository.GetStartupSourcesAsync(stoppingToken);
        foreach (var source in existing)
        {
            await EnqueueAsync(source.Source.SourceId, stoppingToken);
        }

        while (await _queue.Reader.WaitToReadAsync(stoppingToken))
        {
            while (_queue.Reader.TryRead(out var sourceId))
            {
                _scheduled.TryRemove(sourceId, out _);
                if (_running.ContainsKey(sourceId))
                {
                    continue;
                }

                var sourceStop = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                _sourceStops[sourceId] = sourceStop;
                var task = RunSourceLoopAsync(sourceId, sourceStop.Token, stoppingToken);
                _running[sourceId] = task;
                _ = task.ContinueWith(_ =>
                {
                    CleanupCompletedSource(sourceId, task, sourceStop);
                }, CancellationToken.None, TaskContinuationOptions.None, TaskScheduler.Default);
            }
        }
    }

    private void CleanupCompletedSource(Guid sourceId, Task? completedTask, CancellationTokenSource? completedSourceStop)
    {
        if (completedTask is null)
        {
            _running.TryRemove(sourceId, out _);
        }
        else if (_running.TryGetValue(sourceId, out var currentTask) && ReferenceEquals(currentTask, completedTask))
        {
            _running.TryRemove(sourceId, out _);
        }

        if (completedSourceStop is null)
        {
            return;
        }

        if (_sourceStops.TryGetValue(sourceId, out var currentSourceStop) && ReferenceEquals(currentSourceStop, completedSourceStop))
        {
            _sourceStops.TryRemove(sourceId, out _);
            completedSourceStop.Dispose();
        }
    }

    private async Task RunSourceLoopAsync(Guid sourceId, CancellationToken sourceCancellationToken, CancellationToken stoppingToken)
    {
        while (!sourceCancellationToken.IsCancellationRequested)
        {
            var snapshot = await syncStateRepository.GetSourceAsync(sourceId, sourceCancellationToken);
            if (snapshot is null)
            {
                return;
            }

            try
            {
                var disposition = await RunSourceAsync(snapshot, sourceCancellationToken);
                if (disposition.RestartRequired)
                {
                    continue;
                }

                return;
            }
            catch (OperationCanceledException) when (sourceCancellationToken.IsCancellationRequested && !stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "Source sync failed for {SourceId}. The supervisor will retry.", sourceId);
                await MarkErrorAsync(snapshot, exception.Message, sourceCancellationToken);
                await Task.Delay(RetryDelay, sourceCancellationToken);
            }
        }
    }

    private async Task<SourceRunDisposition> RunSourceAsync(SyncSourceRegistrationSnapshot snapshot, CancellationToken cancellationToken)
    {
        var source = snapshot.Source;
        var password = credentialProtector.Unprotect(snapshot.Credentials.EncryptedSecret, snapshot.Credentials.KeyId);
        var designDocument = await couchDbClient.GetDesignDocumentAsync(source.BaseUrl, source.DatabaseName, snapshot.Credentials.Username, password, cancellationToken);
        designContractValidator.Validate(designDocument);

        var activeDesignRevision = designDocument.Revision ?? source.ActiveDesignRevision;
        var reconcileRequired = !string.Equals(snapshot.SchemaState.LastAppliedDesignRevision, activeDesignRevision, StringComparison.Ordinal);

        logger.LogInformation(
            "Design check for source {SourceId}: status={Status}, lastAppliedRevision={LastAppliedRevision}, currentDesignRevision={CurrentDesignRevision}, reconcileRequired={ReconcileRequired}",
            source.SourceId,
            source.Status,
            snapshot.SchemaState.LastAppliedDesignRevision,
            activeDesignRevision,
            reconcileRequired);

        if (!string.Equals(snapshot.SchemaState.LastAppliedDesignRevision, activeDesignRevision, StringComparison.Ordinal))
        {
            await schemaReconciler.ReconcileAsync(snapshot, designDocument, activeDesignRevision, snapshot.ListenerState.DesignSequence, snapshot.Credentials.Username, password, cancellationToken);
            return SourceRunDisposition.Restart(snapshot.ListenerState.DesignSequence);
        }

        if (string.Equals(snapshot.Source.Status, "paused", StringComparison.OrdinalIgnoreCase))
        {
            logger.LogInformation(
                "Source {SourceId} remains paused because the current design revision {DesignRevision} already matches the last applied schema revision.",
                source.SourceId,
                activeDesignRevision);
            return SourceRunDisposition.Complete();
        }

        await postgreSqlService.EnsureTargetDatabaseAsync(source.TargetDatabaseName, cancellationToken);
        await postgreSqlService.BuildInitialSchemaAsync(source.TargetDatabaseName, designDocument, cancellationToken);

        var compiledTypes = SyncProjectionCompiler.Compile(designDocument);
        var selector = SyncProjectionCompiler.BuildSelector(compiledTypes);
        var tableNames = compiledTypes.Select(type => type.Table).Distinct(StringComparer.OrdinalIgnoreCase).ToArray();

        using var linkedCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var designTask = RunDesignListenerAsync(source, snapshot.Credentials.Username, password, snapshot.ListenerState.DesignSequence, activeDesignRevision, tableNames, linkedCancellation.Token);
        var dataTask = RunDataListenerAsync(source, snapshot.ListenerState.DataSequence, snapshot.Source.Status, snapshot.Credentials.Username, password, compiledTypes, selector, activeDesignRevision, tableNames, linkedCancellation.Token);

        var completed = await Task.WhenAny(designTask, dataTask);
        linkedCancellation.Cancel();

        var designDisposition = await AwaitTaskAfterLinkedCancellationAsync(designTask, cancellationToken, SourceRunDisposition.Complete());
        _ = await AwaitTaskAfterLinkedCancellationAsync(dataTask, cancellationToken, SourceRunDisposition.Complete());

        if (completed == designTask && designDisposition.RestartRequired)
        {
            var newDesignRevision = await couchDbClient.GetDesignDocumentAsync(source.BaseUrl, source.DatabaseName, snapshot.Credentials.Username, password, cancellationToken);
            designContractValidator.Validate(newDesignRevision);
            var reconciledRevision = newDesignRevision.Revision ?? activeDesignRevision;
            await schemaReconciler.ReconcileAsync(snapshot, newDesignRevision, reconciledRevision, designDisposition.DesignSequence, snapshot.Credentials.Username, password, cancellationToken);
            return SourceRunDisposition.Restart(designDisposition.DesignSequence);
        }

        if (completed.IsFaulted)
        {
            throw completed.Exception?.GetBaseException() ?? new InvalidOperationException("The source sync task failed.");
        }

        return SourceRunDisposition.Complete();
    }

    private async Task<SourceRunDisposition> RunDesignListenerAsync(
        SourceRegistrationRecord source,
        string username,
        string password,
        string? initialSequence,
        string activeDesignRevision,
        IReadOnlyList<string> tableNames,
        CancellationToken cancellationToken)
    {
        var sequence = string.IsNullOrWhiteSpace(initialSequence) ? "0" : initialSequence;
        var selector = JsonDocument.Parse("""{"_id":"_design/couchsql"}""").RootElement.Clone();

        while (!cancellationToken.IsCancellationRequested)
        {
            CouchDbChangesResponse batch;
            try
            {
                using var requestTimeout = CreateRequestTimeout(cancellationToken);
                batch = await couchDbClient.GetChangesAsync(
                    source.BaseUrl,
                    source.DatabaseName,
                    username,
                    password,
                    sequence,
                    selector,
                    "longpoll",
                    true,
                    null,
                    null,
                    _syncOptions.LongpollHeartbeatMilliseconds,
                    requestTimeout.Token);
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                continue;
            }

            var now = DateTimeOffset.UtcNow;
            if (batch.Results.Count == 0)
            {
                sequence = string.IsNullOrWhiteSpace(batch.LastSequence) ? sequence : batch.LastSequence;
                await syncStateRepository.UpdateDesignListenerAsync(source.SourceId, sequence, activeDesignRevision, now, null, cancellationToken);
                continue;
            }

            foreach (var change in batch.Results)
            {
                sequence = change.Sequence;
                var revision = change.Document is { } document && document.TryGetProperty("_rev", out var revisionElement)
                    ? revisionElement.GetString()
                    : change.Revision;

                if (change.Deleted || !string.Equals(revision, activeDesignRevision, StringComparison.Ordinal))
                {
                    var message = change.Deleted
                        ? "The design document was deleted. Sync has been paused."
                        : $"Detected design document revision '{revision}'. Schema reconcile will restart the source.";

                    if (!change.Deleted)
                    {
                        await syncStateRepository.UpdateDesignListenerAsync(source.SourceId, sequence, revision, now, null, cancellationToken);
                        logger.LogInformation("Detected updated design revision {DesignRevision} for source {SourceId}. Restarting for schema reconcile.", revision, source.SourceId);
                        return SourceRunDisposition.Restart(sequence);
                    }

                    await syncStateRepository.UpdateDesignListenerAsync(source.SourceId, sequence, revision, now, message, cancellationToken);
                    await PauseSourceAsync(source.SourceId, tableNames, revision ?? activeDesignRevision, message, cancellationToken);
                    return SourceRunDisposition.Complete();
                }

                await syncStateRepository.UpdateDesignListenerAsync(source.SourceId, sequence, revision, now, null, cancellationToken);
            }
        }

        return SourceRunDisposition.Complete();
    }

    private async Task<SourceRunDisposition> RunDataListenerAsync(
        SourceRegistrationRecord source,
        string? initialSequence,
        string currentStatus,
        string username,
        string password,
        IReadOnlyList<CompiledTypeDefinition> compiledTypes,
        JsonElement selector,
        string activeDesignRevision,
        IReadOnlyList<string> tableNames,
        CancellationToken cancellationToken)
    {
        var sequence = string.IsNullOrWhiteSpace(initialSequence) ? "0" : initialSequence;
        var processed = 0L;
        var requiresSnapshot = string.IsNullOrWhiteSpace(initialSequence) || string.Equals(currentStatus, "pending", StringComparison.OrdinalIgnoreCase) || string.Equals(currentStatus, "snapshotting", StringComparison.OrdinalIgnoreCase);

        if (requiresSnapshot)
        {
            await syncStateRepository.UpdateSourceStatusAsync(source.SourceId, "snapshotting", activeDesignRevision, cancellationToken);
            await syncStateRepository.UpdateTableStatesAsync(BuildTableStates(source.SourceId, tableNames, "snapshotting", "initial-load", sequence, null, processed, activeDesignRevision, null), cancellationToken);

            while (!cancellationToken.IsCancellationRequested)
            {
                var batch = await couchDbClient.GetChangesAsync(
                    source.BaseUrl,
                    source.DatabaseName,
                    username,
                    password,
                    sequence,
                    selector,
                    "normal",
                    true,
                    _syncOptions.SnapshotBatchSize,
                    _syncOptions.SnapshotSeqInterval,
                    null,
                    cancellationToken);

                foreach (var change in batch.Results)
                {
                    await ApplyChangeAsync(source.TargetDatabaseName, compiledTypes, tableNames, change, cancellationToken);
                    processed++;
                    sequence = change.Sequence;
                    await syncStateRepository.UpdateDataListenerAsync(source.SourceId, sequence, DateTimeOffset.UtcNow, null, cancellationToken);
                }

                sequence = string.IsNullOrWhiteSpace(batch.LastSequence) ? sequence : batch.LastSequence;
                await syncStateRepository.UpdateDataListenerAsync(source.SourceId, sequence, DateTimeOffset.UtcNow, null, cancellationToken);
                await syncStateRepository.UpdateTableStatesAsync(BuildTableStates(source.SourceId, tableNames, "snapshotting", "initial-load", sequence, batch.Pending, processed, activeDesignRevision, null), cancellationToken);

                if (batch.Pending.GetValueOrDefault() == 0)
                {
                    break;
                }
            }
        }

        await syncStateRepository.UpdateSourceStatusAsync(source.SourceId, "active", activeDesignRevision, cancellationToken);
        await syncStateRepository.UpdateTableStatesAsync(BuildTableStates(source.SourceId, tableNames, "active", "steady-state", sequence, 0, processed, activeDesignRevision, null), cancellationToken);

        while (!cancellationToken.IsCancellationRequested)
        {
            CouchDbChangesResponse batch;
            try
            {
                using var requestTimeout = CreateRequestTimeout(cancellationToken);
                batch = await couchDbClient.GetChangesAsync(
                    source.BaseUrl,
                    source.DatabaseName,
                    username,
                    password,
                    sequence,
                    selector,
                    "longpoll",
                    true,
                    null,
                    null,
                    _syncOptions.LongpollHeartbeatMilliseconds,
                    requestTimeout.Token);
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                continue;
            }

            if (batch.Results.Count == 0)
            {
                sequence = string.IsNullOrWhiteSpace(batch.LastSequence) ? sequence : batch.LastSequence;
                await syncStateRepository.UpdateDataListenerAsync(source.SourceId, sequence, DateTimeOffset.UtcNow, null, cancellationToken);
                continue;
            }

            foreach (var change in batch.Results)
            {
                await ApplyChangeAsync(source.TargetDatabaseName, compiledTypes, tableNames, change, cancellationToken);
                processed++;
                sequence = change.Sequence;
                await syncStateRepository.UpdateDataListenerAsync(source.SourceId, sequence, DateTimeOffset.UtcNow, null, cancellationToken);
                await syncStateRepository.UpdateTableStatesAsync(BuildTableStates(source.SourceId, tableNames, "active", "steady-state", sequence, null, processed, activeDesignRevision, null), cancellationToken);
            }
        }

        return SourceRunDisposition.Complete();
    }

    private async Task ApplyChangeAsync(
        string databaseName,
        IReadOnlyList<CompiledTypeDefinition> compiledTypes,
        IReadOnlyList<string> tableNames,
        CouchDbChangeResult change,
        CancellationToken cancellationToken)
    {
        if (change.Deleted)
        {
            await projectionWriter.DeleteDocumentAsync(databaseName, tableNames, change.Id, cancellationToken);
            return;
        }

        if (change.Document is not { } document)
        {
            return;
        }

        var matchedType = SyncProjectionCompiler.MatchType(compiledTypes, document);
        if (matchedType is null)
        {
            return;
        }

        if (!document.TryGetProperty("_id", out var documentIdElement) || string.IsNullOrWhiteSpace(documentIdElement.GetString()))
        {
            throw new InvalidOperationException("A synced document did not contain a valid _id.");
        }

        if (!document.TryGetProperty("_rev", out var revisionElement) || string.IsNullOrWhiteSpace(revisionElement.GetString()))
        {
            throw new InvalidOperationException($"The document '{documentIdElement.GetString()}' did not contain a valid _rev.");
        }

        var projectedValues = SyncProjectionCompiler.ProjectFields(matchedType, document);
        await projectionWriter.UpsertDocumentAsync(
            databaseName,
            matchedType,
            documentIdElement.GetString()!,
            revisionElement.GetString()!,
            change.Sequence,
            projectedValues,
            DateTimeOffset.UtcNow,
            cancellationToken);
    }

    private async Task PauseSourceAsync(Guid sourceId, IReadOnlyList<string> tableNames, string activeDesignRevision, string message, CancellationToken cancellationToken)
    {
        await syncStateRepository.UpdateSourceStatusAsync(sourceId, "paused", activeDesignRevision, cancellationToken);
        await syncStateRepository.UpdateTableStatesAsync(BuildTableStates(sourceId, tableNames, "paused", null, null, null, null, activeDesignRevision, message), cancellationToken);
    }

    private async Task MarkErrorAsync(SyncSourceRegistrationSnapshot snapshot, string message, CancellationToken cancellationToken)
    {
        await syncStateRepository.UpdateSourceStatusAsync(snapshot.Source.SourceId, "error", snapshot.Source.ActiveDesignRevision, cancellationToken);
        await syncStateRepository.UpdateListenerStateAsync(new ListenerStateRecord(
            snapshot.Source.SourceId,
            snapshot.ListenerState.DesignSequence,
            snapshot.ListenerState.DataSequence,
            snapshot.ListenerState.LastDesignRevision,
            DateTimeOffset.UtcNow,
            message,
            DateTimeOffset.UtcNow), cancellationToken);
        await syncStateRepository.UpdateTableStatesAsync(BuildTableStates(
            snapshot.Source.SourceId,
            syncStateRepository.ReadManagedTables(snapshot.SchemaState),
            "error",
            null,
            snapshot.ListenerState.DataSequence,
            null,
            null,
            snapshot.Source.ActiveDesignRevision,
            message), cancellationToken);
    }

    private IReadOnlyList<TableStateRecord> BuildTableStates(
        Guid sourceId,
        IReadOnlyList<string> tableNames,
        string state,
        string? snapshotMode,
        string? currentSequence,
        long? pendingChanges,
        long? processedRowCount,
        string activeDesignRevision,
        string? lastError)
    {
        var now = DateTimeOffset.UtcNow;
        return tableNames.Select(tableName => new TableStateRecord(
                sourceId,
                tableName,
                state,
                null,
                false,
                snapshotMode,
                currentSequence,
                pendingChanges,
                processedRowCount,
                activeDesignRevision,
                activeDesignRevision,
                lastError,
                now))
            .ToArray();
    }

    private CancellationTokenSource CreateRequestTimeout(CancellationToken cancellationToken)
    {
        var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(_syncOptions.LongpollReadTimeoutSeconds));
        return timeout;
    }

    private static async Task<T> AwaitTaskAfterLinkedCancellationAsync<T>(Task<T> task, CancellationToken stoppingToken, T defaultValue)
    {
        try
        {
            return await task;
        }
        catch (OperationCanceledException) when (!stoppingToken.IsCancellationRequested)
        {
            return defaultValue;
        }
    }
}

internal readonly record struct SourceRunDisposition(bool RestartRequired, string? DesignSequence)
{
    public static SourceRunDisposition Complete()
    {
        return new SourceRunDisposition(false, null);
    }

    public static SourceRunDisposition Restart(string? designSequence)
    {
        return new SourceRunDisposition(true, designSequence);
    }
}