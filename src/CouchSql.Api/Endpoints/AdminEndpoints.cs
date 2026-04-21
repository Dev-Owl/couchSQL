using CouchSql.Core.Contracts;
using CouchSql.Core.Design;
using CouchSql.Core.Interfaces;

namespace CouchSql.Api.Endpoints;

public static class AdminEndpoints
{
    public static IEndpointRouteBuilder MapAdminEndpoints(this IEndpointRouteBuilder endpoints)
    {
        var group = endpoints.MapGroup("/internal/v1")
            .WithGroupName("admin")
            .WithTags("Admin API");

        group.MapGet("/health", (IStartupInitializer initializer) =>
            TypedResults.Ok(new HealthResponse(
                initializer.Current.Ready,
                initializer.Current.PostgreSqlAvailable,
                initializer.Current.AdminDatabaseReady,
                initializer.Current.MigrationsApplied,
                initializer.Current.EncryptionKeyReady,
                initializer.Current.Messages)))
            .WithName("GetAdminHealth");

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
                try
                {
                    await removalService.RemoveAsync(connectionId, cancellationToken);
                    return Results.NoContent();
                }
                catch (InvalidOperationException exception)
                {
                    return Results.Conflict(new { error = exception.Message });
                }
            })
            .WithName("DeleteCouchDbConnection");

        group.MapPost("/couchdb/connections/{connectionId:guid}/resync", async (
                Guid connectionId,
                IConnectionResyncService resyncService,
                CancellationToken cancellationToken) =>
            {
                var response = await resyncService.ForceResyncAsync(connectionId, cancellationToken);
                return response is null ? Results.NotFound() : Results.Ok(response);
            })
            .WithName("ForceResyncCouchDbConnection");

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

        group.MapGet("/design-documents", () => Results.Redirect("/internal/v1/design-documents/builder"))
            .WithName("DesignDocumentBuilderRedirect");

        group.MapGet("/design-documents/builder", () => Results.Content(AdminDesignDocumentBuilderPage.BuildHtml(AdminDesignDocumentBuilderPage.CreateSampleDocument()), "text/html"))
            .WithName("DesignDocumentBuilderPage");

        group.MapGet("/design-documents/template", () => Results.Json(AdminDesignDocumentBuilderPage.CreateSampleDocument()))
            .WithName("GetDesignDocumentTemplate");

        group.MapPost("/design-documents/validate", (
                CouchSqlDesignDocument document,
                IDesignContractValidator validator) =>
            {
                try
                {
                    validator.Validate(document);
                    return Results.Ok(new { valid = true });
                }
                catch (InvalidOperationException exception)
                {
                    return Results.BadRequest(new { valid = false, error = exception.Message });
                }
            })
            .WithName("ValidateDesignDocument");

        return endpoints;
    }
}