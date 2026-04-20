using CouchSql.Core.Contracts;
using CouchSql.Core.Interfaces;
using CouchSql.Core.Models;

namespace CouchSql.Api.Endpoints;

public static class QueryEndpoints
{
    public static IEndpointRouteBuilder MapQueryEndpoints(this IEndpointRouteBuilder endpoints)
    {
        var group = endpoints.MapGroup("/api/v1")
            .WithGroupName("query")
            .WithTags("Query API");

        group.MapGet("/health", (IStartupInitializer initializer) =>
            TypedResults.Ok(new HealthResponse(
                initializer.Current.Ready,
                initializer.Current.PostgreSqlAvailable,
                initializer.Current.AdminDatabaseReady,
                initializer.Current.MigrationsApplied,
                initializer.Current.EncryptionKeyReady,
                initializer.Current.Messages)))
            .WithName("GetHealth");

        group.MapGet("/databases", async (IAdminMetadataRepository repository, CancellationToken cancellationToken) =>
            {
                var databases = await repository.GetManagedDatabasesAsync(cancellationToken);
                var response = databases
                    .Select(database => new ManagedDatabaseResponse(database.ConnectionId, database.DatabaseName, database.LogicalName, database.Status))
                    .ToArray();
                return TypedResults.Ok(response);
            })
            .WithName("GetManagedDatabases");

        group.MapGet("/databases/{databaseName}/tables", async (
                string databaseName,
                IAdminMetadataRepository repository,
                CancellationToken cancellationToken) =>
            {
                if (!await repository.DatabaseIsManagedAsync(databaseName, cancellationToken))
                {
                    return Results.NotFound();
                }

                var tables = await repository.GetQueryableTablesAsync(databaseName, cancellationToken);
                return Results.Ok(new TableListResponse(databaseName, tables));
            })
            .WithName("GetDatabaseTables");

        group.MapPost("/query", async (
                QueryRequest request,
                IAdminMetadataRepository repository,
                IQuerySettingsService querySettingsService,
                IPostgreSqlService postgreSqlService,
                CancellationToken cancellationToken) =>
            {
                if (!await repository.DatabaseIsManagedAsync(request.DatabaseName, cancellationToken))
                {
                    return Results.NotFound();
                }

                try
                {
                    var settings = await querySettingsService.GetAsync(cancellationToken);
                    var result = await postgreSqlService.ExecuteSelectAsync(request, settings, cancellationToken);
                    var response = new QueryResponse(request.DatabaseName, result.EffectiveRowLimit, result.Truncated, result.Columns, result.Rows);
                    return Results.Ok(response);
                }
                catch (InvalidOperationException exception)
                {
                    return Results.BadRequest(new { error = exception.Message });
                }
            })
            .WithName("ExecuteQuery");

        return endpoints;
    }
}