using System.Text.Json;
using CouchSql.Core.Design;
using CouchSql.Core.Interfaces;
using CouchSql.Core.Models;
using Microsoft.Extensions.Logging;

namespace CouchSql.Infrastructure.Sync;

public sealed class SchemaReconciler(
    ICouchDbClient couchDbClient,
    IPostgreSqlService postgreSqlService,
    SyncStateRepository syncStateRepository,
    PostgreSqlProjectionWriter projectionWriter,
    ILogger<SchemaReconciler> logger)
{
    public async Task ReconcileAsync(
        SyncSourceRegistrationSnapshot snapshot,
        CouchSqlDesignDocument designDocument,
        string activeDesignRevision,
        string? designSequence,
        string username,
        string password,
        CancellationToken cancellationToken)
    {
        var configuration = designDocument.CouchSql ?? throw new InvalidOperationException("The design document did not contain the couchsql configuration object.");
        var previousTypes = ReadAppliedTypes(snapshot.SchemaState);
        var previousTables = syncStateRepository.ReadManagedTables(snapshot.SchemaState);
        var nextTables = configuration.Types
            .Select(type => type.Table ?? throw new InvalidOperationException("A design type is missing the table name."))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
        var plan = CreateReconcilePlan(previousTypes, configuration.Types);
        var previousAppliedRevision = snapshot.SchemaState.LastAppliedDesignRevision;
        var restartSequence = snapshot.ListenerState.DataSequence ?? "0";
        long rebuildProcessed = 0;

        logger.LogInformation(
            "Reconciling schema for source {SourceId}. Previous design revision {PreviousRevision}, new revision {NewRevision}. Unchanged={UnchangedCount}, IndexOnly={IndexOnlyCount}, InPlace={InPlaceCount}, Rebuild={RebuildCount}, Removed={RemovedCount}.",
            snapshot.Source.SourceId,
            previousAppliedRevision,
            activeDesignRevision,
            plan.UnchangedPlans.Count,
            plan.IndexOnlyPlans.Count,
            plan.InPlacePlans.Count,
            plan.RebuildPlans.Count,
            plan.RemovedPlans.Count);

        await ApplyDirectSchemaChangesAsync(snapshot.Source.TargetDatabaseName, plan, cancellationToken);

        if (plan.RebuildPlans.Count > 0)
        {
            var rebuildTables = plan.RebuildPlans.Select(rebuild => rebuild.CanonicalTable).ToArray();
            var shadowTableMap = plan.RebuildPlans.ToDictionary(
                rebuild => rebuild.CanonicalTable,
                rebuild => rebuild.ShadowTableName ?? throw new InvalidOperationException("A rebuild plan did not have a shadow table name."),
                StringComparer.OrdinalIgnoreCase);
            var shadowDesign = BuildShadowDesign(designDocument, shadowTableMap, activeDesignRevision);
            var compiledShadowTypes = SyncProjectionCompiler.Compile(shadowDesign);
            var rebuildSelector = SyncProjectionCompiler.BuildSelector(compiledShadowTypes);
            var shadowTables = shadowTableMap.Values.ToArray();

            await postgreSqlService.DropManagedTablesAsync(snapshot.Source.TargetDatabaseName, shadowTables, cancellationToken);
            await postgreSqlService.DropManagedTablesAsync(snapshot.Source.TargetDatabaseName, rebuildTables.Select(table => table + "_old").ToArray(), cancellationToken);
            await postgreSqlService.BuildInitialSchemaAsync(snapshot.Source.TargetDatabaseName, shadowDesign, cancellationToken);

            await syncStateRepository.UpdateSourceStatusAsync(snapshot.Source.SourceId, "rebuilding", activeDesignRevision, cancellationToken);
            await syncStateRepository.UpdateDesignListenerAsync(snapshot.Source.SourceId, designSequence, activeDesignRevision, DateTimeOffset.UtcNow, null, cancellationToken);
            await syncStateRepository.UpdateTableStatesAsync(BuildRebuildStates(
                snapshot.Source.SourceId,
                rebuildTables,
                shadowTableMap,
                "rebuilding",
                "rebuild",
                "0",
                null,
                0,
                activeDesignRevision,
                previousAppliedRevision,
                null), cancellationToken);

            var rebuildResult = await RunProjectedSnapshotAsync(
                snapshot,
                username,
                password,
                compiledShadowTypes,
                rebuildSelector,
                rebuildTables,
                shadowTableMap,
                shadowTables,
                activeDesignRevision,
                previousAppliedRevision,
                designSequence,
                "0",
                0,
                cancellationToken,
                allowRaceClosingPass: true);
            rebuildProcessed = rebuildResult.Processed;

            var routedDesign = BuildRoutedDesign(designDocument, shadowTableMap);
            var compiledCatchUpTypes = SyncProjectionCompiler.Compile(routedDesign);
            var catchUpSelector = SyncProjectionCompiler.BuildSelector(compiledCatchUpTypes);
            var catchUpTables = compiledCatchUpTypes
                .Select(type => type.Table)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToArray();

            var catchUpResult = await RunProjectedSnapshotAsync(
                snapshot,
                username,
                password,
                compiledCatchUpTypes,
                catchUpSelector,
                rebuildTables,
                shadowTableMap,
                catchUpTables,
                activeDesignRevision,
                previousAppliedRevision,
                designSequence,
                snapshot.ListenerState.DataSequence ?? "0",
                rebuildProcessed,
                cancellationToken,
                allowRaceClosingPass: true);
            restartSequence = catchUpResult.Sequence;

            await syncStateRepository.UpdateSourceStatusAsync(snapshot.Source.SourceId, "rebuilding", activeDesignRevision, cancellationToken);
            await syncStateRepository.UpdateTableStatesAsync(BuildRebuildStates(
                snapshot.Source.SourceId,
                rebuildTables,
                shadowTableMap,
                "swapping",
                "rebuild",
                restartSequence,
                0,
                rebuildProcessed,
                activeDesignRevision,
                previousAppliedRevision,
                null), cancellationToken);

            await postgreSqlService.SwapShadowTablesAsync(snapshot.Source.TargetDatabaseName, shadowTableMap, cancellationToken);
        }

        var removedTables = previousTables.Except(nextTables, StringComparer.OrdinalIgnoreCase).ToArray();
        await postgreSqlService.DropManagedTablesAsync(snapshot.Source.TargetDatabaseName, removedTables, cancellationToken);

        var now = DateTimeOffset.UtcNow;
        var schemaState = new SchemaStateRecord(
            snapshot.Source.SourceId,
            JsonSerializer.Serialize(configuration.Types.ToArray()),
            JsonSerializer.Serialize(nextTables),
            configuration.SchemaVersion,
            activeDesignRevision,
            now);

        var rebuildCounts = plan.RebuildPlans.ToDictionary(
            rebuild => rebuild.CanonicalTable,
            _ => rebuildProcessed,
            StringComparer.OrdinalIgnoreCase);
        var tableStates = BuildFinalStates(snapshot.Source.SourceId, configuration.Types, restartSequence, rebuildCounts, activeDesignRevision, now);

        await syncStateRepository.ApplySchemaReconcileAsync(
            snapshot.Source.SourceId,
            configuration.SchemaVersion,
            "active",
            activeDesignRevision,
            designSequence,
            restartSequence,
            schemaState,
            tableStates,
            cancellationToken);

        logger.LogInformation(
            "Schema reconcile completed for source {SourceId}. Rebuilt tables: {RebuildCount}. Total current tables: {TableCount}.",
            snapshot.Source.SourceId,
            plan.RebuildPlans.Count,
            nextTables.Length);
    }

    private async Task ApplyDirectSchemaChangesAsync(string databaseName, ReconcilePlan plan, CancellationToken cancellationToken)
    {
        foreach (var removedPlan in plan.RemovedPlans)
        {
            await postgreSqlService.DropManagedTablesAsync(databaseName, [removedPlan.CanonicalTable], cancellationToken);
        }

        foreach (var inPlacePlan in plan.InPlacePlans)
        {
            var nextType = inPlacePlan.NextType;
            if (inPlacePlan.RenamedColumns.Count > 0)
            {
                await postgreSqlService.RenameManagedColumnsAsync(databaseName, inPlacePlan.CanonicalTable, inPlacePlan.RenamedColumns, cancellationToken);
            }

            if (inPlacePlan.DroppedColumns.Count > 0)
            {
                await postgreSqlService.DropManagedColumnsAsync(databaseName, inPlacePlan.CanonicalTable, inPlacePlan.DroppedColumns, cancellationToken);
            }

            await postgreSqlService.ReplaceManagedIndexesAsync(
                databaseName,
                inPlacePlan.CanonicalTable,
                inPlacePlan.PreviousIndexNames,
                nextType is not null ? nextType.Fields : Array.Empty<CouchSqlFieldDefinition>(),
                nextType is not null ? nextType.Indexes : Array.Empty<CouchSqlIndexDefinition>(),
                cancellationToken);
        }

        foreach (var indexOnlyPlan in plan.IndexOnlyPlans)
        {
            var nextType = indexOnlyPlan.NextType;
            await postgreSqlService.ReplaceManagedIndexesAsync(
                databaseName,
                indexOnlyPlan.CanonicalTable,
                indexOnlyPlan.PreviousIndexNames,
                nextType is not null ? nextType.Fields : Array.Empty<CouchSqlFieldDefinition>(),
                nextType is not null ? nextType.Indexes : Array.Empty<CouchSqlIndexDefinition>(),
                cancellationToken);
        }
    }

    private async Task<RebuildProgress> RunProjectedSnapshotAsync(
        SyncSourceRegistrationSnapshot snapshot,
        string username,
        string password,
        IReadOnlyList<CompiledTypeDefinition> compiledTypes,
        JsonElement selector,
        IReadOnlyList<string> rebuildTables,
        IReadOnlyDictionary<string, string> shadowTableMap,
        IReadOnlyList<string> deleteTargetTables,
        string activeDesignRevision,
        string previousAppliedRevision,
        string? designSequence,
        string initialSequence,
        long initialProcessed,
        CancellationToken cancellationToken,
        bool allowRaceClosingPass)
    {
        var sequence = initialSequence;
        var processed = initialProcessed;
        var firstBatch = true;

        while (!cancellationToken.IsCancellationRequested)
        {
            var batch = await couchDbClient.GetChangesAsync(
                snapshot.Source.BaseUrl,
                snapshot.Source.DatabaseName,
                username,
                password,
                sequence,
                selector,
                "normal",
                true,
                1000,
                1000,
                null,
                cancellationToken);

            foreach (var change in batch.Results)
            {
                if (await ApplyProjectedChangeAsync(snapshot.Source.TargetDatabaseName, compiledTypes, deleteTargetTables, change, cancellationToken))
                {
                    processed++;
                }

                sequence = change.Sequence;
                await syncStateRepository.UpdateDataListenerAsync(snapshot.Source.SourceId, sequence, DateTimeOffset.UtcNow, null, cancellationToken);
            }

            sequence = string.IsNullOrWhiteSpace(batch.LastSequence) ? sequence : batch.LastSequence;
            await syncStateRepository.UpdateDesignListenerAsync(snapshot.Source.SourceId, designSequence, activeDesignRevision, DateTimeOffset.UtcNow, null, cancellationToken);
            await syncStateRepository.UpdateDataListenerAsync(snapshot.Source.SourceId, sequence, DateTimeOffset.UtcNow, null, cancellationToken);

            if (rebuildTables.Count > 0)
            {
                await syncStateRepository.UpdateTableStatesAsync(BuildRebuildStates(
                    snapshot.Source.SourceId,
                    rebuildTables,
                    shadowTableMap,
                    "rebuilding",
                    "rebuild",
                    sequence,
                    batch.Pending,
                    processed,
                    activeDesignRevision,
                    previousAppliedRevision,
                    null), cancellationToken);
            }

            if (batch.Pending.GetValueOrDefault() == 0)
            {
                if (allowRaceClosingPass && firstBatch)
                {
                    firstBatch = false;
                    continue;
                }

                break;
            }

            firstBatch = false;
        }

        return new RebuildProgress(sequence, processed);
    }

    private async Task<bool> ApplyProjectedChangeAsync(
        string databaseName,
        IReadOnlyList<CompiledTypeDefinition> compiledTypes,
        IReadOnlyList<string> deleteTargetTables,
        CouchDbChangeResult change,
        CancellationToken cancellationToken)
    {
        if (change.Deleted)
        {
            await projectionWriter.DeleteDocumentAsync(databaseName, deleteTargetTables, change.Id, cancellationToken);
            return true;
        }

        if (change.Document is not { } document)
        {
            return false;
        }

        var matchedType = SyncProjectionCompiler.MatchType(compiledTypes, document);
        if (matchedType is null)
        {
            return false;
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

        return true;
    }

    private static IReadOnlyList<CouchSqlTypeDefinition> ReadAppliedTypes(SchemaStateRecord schemaState)
    {
        return JsonSerializer.Deserialize<CouchSqlTypeDefinition[]>(schemaState.AppliedTypeDefinitionsJson) ?? Array.Empty<CouchSqlTypeDefinition>();
    }

    private static CouchSqlDesignDocument BuildShadowDesign(
        CouchSqlDesignDocument designDocument,
        IReadOnlyDictionary<string, string> shadowTableMap,
        string activeDesignRevision)
    {
        var configuration = designDocument.CouchSql ?? throw new InvalidOperationException("The design document did not contain the couchsql configuration object.");
        var revisionToken = CreateShadowRevisionToken(activeDesignRevision);

        return new CouchSqlDesignDocument
        {
            Id = designDocument.Id,
            Revision = designDocument.Revision,
            CouchSql = new CouchSqlDesignConfiguration
            {
                SchemaVersion = configuration.SchemaVersion,
                Types = configuration.Types
                    .Where(type => shadowTableMap.ContainsKey(type.Table ?? string.Empty))
                    .Select(type => new CouchSqlTypeDefinition
                    {
                        Name = type.Name,
                        Table = shadowTableMap[type.Table ?? throw new InvalidOperationException("A design type is missing the table name.")],
                        Identify = type.Identify.Clone(),
                        Fields = type.Fields.Select(field => new CouchSqlFieldDefinition
                        {
                            Column = field.Column,
                            Path = field.Path,
                            Type = field.Type,
                            Required = field.Required,
                            Transform = field.Transform is null
                                ? null
                                : new CouchSqlFieldTransformDefinition
                                {
                                    Prefix = field.Transform.Prefix,
                                    Append = field.Transform.Append
                                }
                        }).ToList(),
                        Indexes = type.Indexes.Select(index => new CouchSqlIndexDefinition
                        {
                            Name = $"{index.Name}__sh_{revisionToken}",
                            Columns = index.Columns.ToList(),
                            Unique = index.Unique
                        }).ToList()
                    })
                    .ToList()
            }
        };
    }

    private static CouchSqlDesignDocument BuildRoutedDesign(
        CouchSqlDesignDocument designDocument,
        IReadOnlyDictionary<string, string> shadowTableMap)
    {
        var configuration = designDocument.CouchSql ?? throw new InvalidOperationException("The design document did not contain the couchsql configuration object.");

        return new CouchSqlDesignDocument
        {
            Id = designDocument.Id,
            Revision = designDocument.Revision,
            CouchSql = new CouchSqlDesignConfiguration
            {
                SchemaVersion = configuration.SchemaVersion,
                Types = configuration.Types.Select(type => new CouchSqlTypeDefinition
                {
                    Name = type.Name,
                    Table = shadowTableMap.TryGetValue(type.Table ?? throw new InvalidOperationException("A design type is missing the table name."), out var shadowTable)
                        ? shadowTable
                        : type.Table,
                    Identify = type.Identify.Clone(),
                    Fields = type.Fields.Select(field => new CouchSqlFieldDefinition
                    {
                        Column = field.Column,
                        Path = field.Path,
                        Type = field.Type,
                        Required = field.Required,
                        Transform = field.Transform is null
                            ? null
                            : new CouchSqlFieldTransformDefinition
                            {
                                Prefix = field.Transform.Prefix,
                                Append = field.Transform.Append
                            }
                    }).ToList(),
                    Indexes = type.Indexes.Select(index => new CouchSqlIndexDefinition
                    {
                        Name = index.Name,
                        Columns = index.Columns.ToList(),
                        Unique = index.Unique
                    }).ToList()
                }).ToList()
            }
        };
    }

    private static ReconcilePlan CreateReconcilePlan(
        IReadOnlyList<CouchSqlTypeDefinition> previousTypes,
        IReadOnlyList<CouchSqlTypeDefinition> nextTypes)
    {
        var plans = new List<TypePlan>();
        var previousByName = previousTypes.ToDictionary(type => type.Name ?? string.Empty, StringComparer.OrdinalIgnoreCase);
        var nextByName = nextTypes.ToDictionary(type => type.Name ?? string.Empty, StringComparer.OrdinalIgnoreCase);

        foreach (var previousType in previousTypes.OrderBy(type => type.Name, StringComparer.OrdinalIgnoreCase))
        {
            var typeName = previousType.Name ?? throw new InvalidOperationException("A stored type is missing the logical name.");
            if (!nextByName.ContainsKey(typeName))
            {
                plans.Add(TypePlan.Removed(previousType));
            }
        }

        foreach (var nextType in nextTypes.OrderBy(type => type.Name, StringComparer.OrdinalIgnoreCase))
        {
            var typeName = nextType.Name ?? throw new InvalidOperationException("A design type is missing the logical name.");
            if (!previousByName.TryGetValue(typeName, out var previousType))
            {
                plans.Add(TypePlan.Rebuild(nextType, "new type requires initial backfill"));
                continue;
            }

            plans.Add(ClassifyTypeChange(previousType, nextType));
        }

        return new ReconcilePlan(plans);
    }

    private static TypePlan ClassifyTypeChange(CouchSqlTypeDefinition previousType, CouchSqlTypeDefinition nextType)
    {
        var previousTable = previousType.Table ?? throw new InvalidOperationException("A stored type is missing the table name.");
        var nextTable = nextType.Table ?? throw new InvalidOperationException("A design type is missing the table name.");

        if (!string.Equals(previousTable, nextTable, StringComparison.OrdinalIgnoreCase))
        {
            return TypePlan.Rebuild(previousType, nextType, "target table changed");
        }

        if (!JsonElement.DeepEquals(previousType.Identify, nextType.Identify))
        {
            return TypePlan.Rebuild(previousType, nextType, "identify rule changed");
        }

        var previousFields = previousType.Fields.ToDictionary(field => field.Column ?? string.Empty, StringComparer.OrdinalIgnoreCase);
        var nextFields = nextType.Fields.ToDictionary(field => field.Column ?? string.Empty, StringComparer.OrdinalIgnoreCase);

        foreach (var sharedColumn in previousFields.Keys.Intersect(nextFields.Keys, StringComparer.OrdinalIgnoreCase))
        {
            if (!FieldDefinitionsEqual(previousFields[sharedColumn], nextFields[sharedColumn]))
            {
                return TypePlan.Rebuild(previousType, nextType, $"mapped field '{sharedColumn}' changed");
            }
        }

        var previousOnlyFields = previousType.Fields
            .Where(field => !nextFields.ContainsKey(field.Column ?? string.Empty))
            .OrderBy(field => field.Column, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        var nextOnlyFields = nextType.Fields
            .Where(field => !previousFields.ContainsKey(field.Column ?? string.Empty))
            .OrderBy(field => field.Column, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var renameMatches = MatchRenamedColumns(previousOnlyFields, nextOnlyFields);
        var renamedPreviousColumns = renameMatches.Keys.ToHashSet(StringComparer.OrdinalIgnoreCase);
        var renamedNextColumns = renameMatches.Values.ToHashSet(StringComparer.OrdinalIgnoreCase);
        var droppedColumns = previousOnlyFields
            .Select(field => field.Column ?? string.Empty)
            .Where(column => !renamedPreviousColumns.Contains(column))
            .ToArray();
        var addedColumns = nextOnlyFields
            .Select(field => field.Column ?? string.Empty)
            .Where(column => !renamedNextColumns.Contains(column))
            .ToArray();

        if (addedColumns.Length > 0)
        {
            return TypePlan.Rebuild(previousType, nextType, $"added mapped columns: {string.Join(", ", addedColumns)}");
        }

        var indexesChanged = !IndexesEqual(previousType.Indexes, nextType.Indexes);
        if (renameMatches.Count > 0 || droppedColumns.Length > 0)
        {
            return TypePlan.InPlace(
                previousType,
                nextType,
                droppedColumns,
                renameMatches,
                previousType.Indexes.Select(index => index.Name ?? string.Empty).ToArray());
        }

        if (indexesChanged)
        {
            return TypePlan.IndexOnly(previousType, nextType);
        }

        return TypePlan.Unchanged(previousType, nextType);
    }

    private static Dictionary<string, string> MatchRenamedColumns(
        IReadOnlyList<CouchSqlFieldDefinition> previousOnlyFields,
        IReadOnlyList<CouchSqlFieldDefinition> nextOnlyFields)
    {
        var renamedColumns = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var nextBySignature = nextOnlyFields
            .GroupBy(GetFieldShapeSignature, StringComparer.Ordinal)
            .ToDictionary(
                group => group.Key,
                group => new Queue<CouchSqlFieldDefinition>(group.OrderBy(field => field.Column, StringComparer.OrdinalIgnoreCase)),
                StringComparer.Ordinal);

        foreach (var previousField in previousOnlyFields.OrderBy(field => field.Column, StringComparer.OrdinalIgnoreCase))
        {
            var signature = GetFieldShapeSignature(previousField);
            if (!nextBySignature.TryGetValue(signature, out var candidates) || candidates.Count == 0)
            {
                continue;
            }

            var nextField = candidates.Dequeue();
            renamedColumns[previousField.Column ?? string.Empty] = nextField.Column ?? string.Empty;
        }

        return renamedColumns;
    }

    private static bool FieldDefinitionsEqual(CouchSqlFieldDefinition previousField, CouchSqlFieldDefinition nextField)
    {
        return string.Equals(previousField.Path, nextField.Path, StringComparison.Ordinal)
            && string.Equals(previousField.Type, nextField.Type, StringComparison.OrdinalIgnoreCase)
            && previousField.Required == nextField.Required
            && FieldTransformsEqual(previousField.Transform, nextField.Transform);
    }

    private static string GetFieldShapeSignature(CouchSqlFieldDefinition field)
    {
        return string.Join(
            "\u001f",
            field.Path ?? string.Empty,
            field.Type?.ToLowerInvariant() ?? string.Empty,
            field.Required ? "required" : "optional",
            field.Transform?.Prefix ?? string.Empty,
            field.Transform?.Append ?? string.Empty);
    }

    private static bool FieldTransformsEqual(CouchSqlFieldTransformDefinition? previousTransform, CouchSqlFieldTransformDefinition? nextTransform)
    {
        return string.Equals(previousTransform?.Prefix, nextTransform?.Prefix, StringComparison.Ordinal)
            && string.Equals(previousTransform?.Append, nextTransform?.Append, StringComparison.Ordinal);
    }

    private static bool IndexesEqual(IReadOnlyList<CouchSqlIndexDefinition> previousIndexes, IReadOnlyList<CouchSqlIndexDefinition> nextIndexes)
    {
        if (previousIndexes.Count != nextIndexes.Count)
        {
            return false;
        }

        var previousByName = previousIndexes.ToDictionary(index => index.Name ?? string.Empty, StringComparer.OrdinalIgnoreCase);
        var nextByName = nextIndexes.ToDictionary(index => index.Name ?? string.Empty, StringComparer.OrdinalIgnoreCase);
        if (!previousByName.Keys.ToHashSet(StringComparer.OrdinalIgnoreCase).SetEquals(nextByName.Keys))
        {
            return false;
        }

        foreach (var name in previousByName.Keys)
        {
            var previousIndex = previousByName[name];
            var nextIndex = nextByName[name];
            if (previousIndex.Unique != nextIndex.Unique)
            {
                return false;
            }

            if (!previousIndex.Columns.SequenceEqual(nextIndex.Columns, StringComparer.OrdinalIgnoreCase))
            {
                return false;
            }
        }

        return true;
    }

    private static string CreateShadowRevisionToken(string revision)
    {
        using var sha256 = System.Security.Cryptography.SHA256.Create();
        var bytes = sha256.ComputeHash(System.Text.Encoding.UTF8.GetBytes(revision));
        return Convert.ToHexString(bytes[..4]).ToLowerInvariant();
    }

    private static IReadOnlyList<TableStateRecord> BuildRebuildStates(
        Guid sourceId,
        IReadOnlyList<string> canonicalTables,
        IReadOnlyDictionary<string, string> shadowTableMap,
        string state,
        string snapshotMode,
        string? currentSequence,
        long? pendingChanges,
        long? processedRowCount,
        string activeDesignRevision,
        string lastAppliedDesignRevision,
        string? lastError)
    {
        var now = DateTimeOffset.UtcNow;
        return canonicalTables.Select(tableName => new TableStateRecord(
                sourceId,
                tableName,
                state,
                shadowTableMap[tableName],
                true,
                snapshotMode,
                currentSequence,
                pendingChanges,
                processedRowCount,
                activeDesignRevision,
                lastAppliedDesignRevision,
                lastError,
                now))
            .ToArray();
    }

    private static IReadOnlyList<TableStateRecord> BuildFinalStates(
        Guid sourceId,
        IReadOnlyList<CouchSqlTypeDefinition> currentTypes,
        string? currentSequence,
        IReadOnlyDictionary<string, long> rebuildCounts,
        string activeDesignRevision,
        DateTimeOffset now)
    {
        return currentTypes
            .Select(type => type.Table ?? throw new InvalidOperationException("A design type is missing the table name."))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Select(tableName => new TableStateRecord(
                sourceId,
                tableName,
                "active",
                null,
                false,
                "steady-state",
                currentSequence,
                0,
                rebuildCounts.TryGetValue(tableName, out var processedCount) ? processedCount : null,
                activeDesignRevision,
                activeDesignRevision,
                null,
                now))
            .ToArray();
    }

    private readonly record struct RebuildProgress(string Sequence, long Processed);

    private sealed record ReconcilePlan(IReadOnlyList<TypePlan> Plans)
    {
        public IReadOnlyList<TypePlan> UnchangedPlans { get; } = Plans.Where(plan => plan.Kind == TypeChangeKind.Unchanged).ToArray();

        public IReadOnlyList<TypePlan> IndexOnlyPlans { get; } = Plans.Where(plan => plan.Kind == TypeChangeKind.IndexOnly).ToArray();

        public IReadOnlyList<TypePlan> InPlacePlans { get; } = Plans.Where(plan => plan.Kind == TypeChangeKind.InPlace).ToArray();

        public IReadOnlyList<TypePlan> RebuildPlans { get; } = Plans.Where(plan => plan.Kind == TypeChangeKind.Rebuild).ToArray();

        public IReadOnlyList<TypePlan> RemovedPlans { get; } = Plans.Where(plan => plan.Kind == TypeChangeKind.Removed).ToArray();
    }

    private sealed record TypePlan(
        string TypeName,
        string CanonicalTable,
        TypeChangeKind Kind,
        CouchSqlTypeDefinition? PreviousType,
        CouchSqlTypeDefinition? NextType,
        IReadOnlyList<string> DroppedColumns,
        IReadOnlyDictionary<string, string> RenamedColumns,
        IReadOnlyList<string> PreviousIndexNames,
        string? ShadowTableName,
        string Reason)
    {
        public static TypePlan Unchanged(CouchSqlTypeDefinition previousType, CouchSqlTypeDefinition nextType)
        {
            return new TypePlan(
                nextType.Name ?? string.Empty,
                nextType.Table ?? throw new InvalidOperationException("A design type is missing the table name."),
                TypeChangeKind.Unchanged,
                previousType,
                nextType,
                Array.Empty<string>(),
                new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase),
                previousType.Indexes.Select(index => index.Name ?? string.Empty).ToArray(),
                null,
                "unchanged");
        }

        public static TypePlan IndexOnly(CouchSqlTypeDefinition previousType, CouchSqlTypeDefinition nextType)
        {
            return new TypePlan(
                nextType.Name ?? string.Empty,
                nextType.Table ?? throw new InvalidOperationException("A design type is missing the table name."),
                TypeChangeKind.IndexOnly,
                previousType,
                nextType,
                Array.Empty<string>(),
                new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase),
                previousType.Indexes.Select(index => index.Name ?? string.Empty).ToArray(),
                null,
                "managed indexes changed");
        }

        public static TypePlan InPlace(
            CouchSqlTypeDefinition previousType,
            CouchSqlTypeDefinition nextType,
            IReadOnlyList<string> droppedColumns,
            IReadOnlyDictionary<string, string> renamedColumns,
            IReadOnlyList<string> previousIndexNames)
        {
            return new TypePlan(
                nextType.Name ?? string.Empty,
                nextType.Table ?? throw new InvalidOperationException("A design type is missing the table name."),
                TypeChangeKind.InPlace,
                previousType,
                nextType,
                droppedColumns,
                renamedColumns,
                previousIndexNames,
                null,
                "metadata-only table changes");
        }

        public static TypePlan Rebuild(CouchSqlTypeDefinition nextType, string reason)
        {
            return new TypePlan(
                nextType.Name ?? string.Empty,
                nextType.Table ?? throw new InvalidOperationException("A design type is missing the table name."),
                TypeChangeKind.Rebuild,
                null,
                nextType,
                Array.Empty<string>(),
                new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase),
                Array.Empty<string>(),
                (nextType.Table ?? throw new InvalidOperationException("A design type is missing the table name.")) + "_shadow",
                reason);
        }

        public static TypePlan Rebuild(CouchSqlTypeDefinition previousType, CouchSqlTypeDefinition nextType, string reason)
        {
            return new TypePlan(
                nextType.Name ?? string.Empty,
                nextType.Table ?? throw new InvalidOperationException("A design type is missing the table name."),
                TypeChangeKind.Rebuild,
                previousType,
                nextType,
                Array.Empty<string>(),
                new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase),
                previousType.Indexes.Select(index => index.Name ?? string.Empty).ToArray(),
                (nextType.Table ?? throw new InvalidOperationException("A design type is missing the table name.")) + "_shadow",
                reason);
        }

        public static TypePlan Removed(CouchSqlTypeDefinition previousType)
        {
            return new TypePlan(
                previousType.Name ?? string.Empty,
                previousType.Table ?? throw new InvalidOperationException("A stored type is missing the table name."),
                TypeChangeKind.Removed,
                previousType,
                null,
                Array.Empty<string>(),
                new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase),
                previousType.Indexes.Select(index => index.Name ?? string.Empty).ToArray(),
                null,
                "type removed from design document");
        }
    }

    private enum TypeChangeKind
    {
        Unchanged,
        IndexOnly,
        InPlace,
        Rebuild,
        Removed
    }
}