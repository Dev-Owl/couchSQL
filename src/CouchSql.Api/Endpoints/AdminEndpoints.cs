using CouchSql.Core.Contracts;
using CouchSql.Core.Interfaces;

namespace CouchSql.Api.Endpoints;

public static class AdminEndpoints
{
    public static IEndpointRouteBuilder MapAdminEndpoints(this IEndpointRouteBuilder endpoints)
    {
        var group = endpoints.MapGroup("/internal/v1")
            .WithGroupName("admin")
            .WithTags("Admin API");

        group.MapPost("/couchdb/connections", async (
                RegisterCouchDbConnectionRequest request,
                IConnectionRegistrationService registrationService,
                CancellationToken cancellationToken) =>
            {
                try
                {
                    var response = await registrationService.RegisterAsync(request, cancellationToken);
                    return Results.Created($"/internal/v1/couchdb/connections/{response.ConnectionId}", response);
                }
                catch (InvalidOperationException exception)
                {
                    return Results.BadRequest(new { error = exception.Message });
                }
            })
            .WithName("RegisterCouchDbConnection");

        group.MapDelete("/couchdb/connections/{connectionId:guid}", async (
                Guid connectionId,
                IConnectionRemovalService removalService,
                CancellationToken cancellationToken) =>
            {
                await removalService.RemoveAsync(connectionId, cancellationToken);
                return Results.NoContent();
            })
            .WithName("DeleteCouchDbConnection");

        group.MapGet("/couchdb/connections/{connectionId:guid}/tables/{tableName}/state", async (
                Guid connectionId,
                string tableName,
                IAdminMetadataRepository repository,
                CancellationToken cancellationToken) =>
            {
                var state = await repository.GetTableStateAsync(connectionId, tableName, cancellationToken);
                return state is null ? Results.NotFound() : Results.Ok(state);
            })
            .WithName("GetManagedTableState");

        group.MapGet("/settings/query", async (IQuerySettingsService querySettingsService, CancellationToken cancellationToken) =>
            {
                var settings = await querySettingsService.GetAsync(cancellationToken);
                return Results.Ok(new QuerySettingsResponse(settings.DefaultRowLimit, settings.MaxRowLimit, settings.CommandTimeoutSeconds));
            })
            .WithName("GetQuerySettings");

        group.MapPut("/settings/query", async (
                UpdateQuerySettingsRequest request,
                IQuerySettingsService querySettingsService,
                CancellationToken cancellationToken) =>
            {
                try
                {
                    var settings = await querySettingsService.UpdateAsync(request, cancellationToken);
                    return Results.Ok(new QuerySettingsResponse(settings.DefaultRowLimit, settings.MaxRowLimit, settings.CommandTimeoutSeconds));
                }
                catch (InvalidOperationException exception)
                {
                    return Results.BadRequest(new { error = exception.Message });
                }
            })
            .WithName("UpdateQuerySettings");

        return endpoints;
    }
}