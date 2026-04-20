using CouchSql.Api.Endpoints;
using CouchSql.Api.Middleware;
using CouchSql.Core.Options;
using CouchSql.Core.Interfaces;
using CouchSql.Infrastructure;
using Microsoft.OpenApi;
using Serilog;

var builder = WebApplication.CreateBuilder(args);

builder.Host.UseSerilog((context, services, configuration) =>
{
    configuration
        .ReadFrom.Configuration(context.Configuration)
        .ReadFrom.Services(services)
        .Enrich.FromLogContext();
});

builder.Services.AddProblemDetails();

builder.Services.AddOptions<PostgreSqlOptions>()
    .Bind(builder.Configuration.GetSection("PostgreSql"))
    .ValidateDataAnnotations()
    .ValidateOnStart();

builder.Services.AddOptions<EndpointOptions>()
    .Bind(builder.Configuration.GetSection("Endpoints"))
    .ValidateDataAnnotations()
    .ValidateOnStart();

builder.Services.AddOptions<QueryOptions>()
    .Bind(builder.Configuration.GetSection("Query"))
    .ValidateDataAnnotations()
    .Validate(options => options.DefaultRowLimit <= options.MaxRowLimit, "DefaultRowLimit must be less than or equal to MaxRowLimit.")
    .ValidateOnStart();

builder.Services.AddOptions<SyncOptions>()
    .Bind(builder.Configuration.GetSection("Sync"))
    .ValidateDataAnnotations()
    .ValidateOnStart();

builder.Services.AddOptions<SecurityOptions>()
    .Bind(builder.Configuration.GetSection("Security"))
    .ValidateDataAnnotations()
    .ValidateOnStart();

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(options =>
{
    options.SwaggerDoc("query", new OpenApiInfo
    {
        Title = "couchSQL Query API",
        Version = "v1"
    });

    options.SwaggerDoc("admin", new OpenApiInfo
    {
        Title = "couchSQL Admin API",
        Version = "v1"
    });

    options.DocInclusionPredicate((documentName, apiDescription) =>
        string.Equals(apiDescription.GroupName, documentName, StringComparison.OrdinalIgnoreCase));
});

builder.Services.AddCouchSqlInfrastructure();

var publicEndpoint = builder.Configuration["Endpoints:Public"] ?? "http://0.0.0.0:8080";
var adminEndpoint = builder.Configuration["Endpoints:Admin"] ?? "http://127.0.0.1:8081";
builder.WebHost.UseUrls(publicEndpoint, adminEndpoint);

var app = builder.Build();

app.UseExceptionHandler();
app.UseSerilogRequestLogging();
app.UseMiddleware<LocalAdminOnlyMiddleware>();

app.UseSwagger(options =>
{
    options.RouteTemplate = "swagger/{documentName}/swagger.json";
});

app.UseSwaggerUI(options =>
{
    options.RoutePrefix = "swagger/query";
    options.SwaggerEndpoint("/swagger/query/swagger.json", "couchSQL Query API");
});

app.UseSwaggerUI(options =>
{
    options.RoutePrefix = "swagger/admin";
    options.SwaggerEndpoint("/swagger/admin/swagger.json", "couchSQL Admin API");
});

app.MapQueryEndpoints();
app.MapAdminEndpoints();

using (var scope = app.Services.CreateScope())
{
    var initializer = scope.ServiceProvider.GetRequiredService<IStartupInitializer>();
    await initializer.InitializeAsync(CancellationToken.None);
}

app.Run();

public partial class Program;
