using CouchSql.Core.Interfaces;
using CouchSql.Infrastructure.CouchDb;
using CouchSql.Infrastructure.PostgreSql;
using CouchSql.Infrastructure.Query;
using CouchSql.Infrastructure.Schema;
using CouchSql.Infrastructure.Security;
using CouchSql.Infrastructure.Services;
using CouchSql.Infrastructure.Sync;
using Microsoft.Extensions.DependencyInjection;

namespace CouchSql.Infrastructure;

public static class DependencyInjection
{
    public static IServiceCollection AddCouchSqlInfrastructure(this IServiceCollection services)
    {
        services.AddHttpClient<ICouchDbClient, CouchDbClient>(client =>
        {
            client.Timeout = Timeout.InfiniteTimeSpan;
        });

        services.AddSingleton<ICredentialProtector, FileKeyCredentialProtector>();
        services.AddSingleton<ISqlQueryValidator, SqlQueryValidator>();
        services.AddSingleton<IDesignContractValidator, DesignContractValidator>();
        services.AddSingleton<IAdminMetadataRepository, AdminMetadataRepository>();
        services.AddSingleton<IPostgreSqlService, PostgreSqlService>();
        services.AddSingleton<PostgreSqlProjectionWriter>();
        services.AddSingleton<SyncStateRepository>();
        services.AddSingleton<SchemaReconciler>();
        services.AddSingleton<IQuerySettingsService, QuerySettingsService>();
        services.AddSingleton<IStartupInitializer, StartupInitializer>();
        services.AddSingleton<ISyncSupervisor, CouchDbSyncSupervisor>();
        services.AddHostedService(provider => (CouchDbSyncSupervisor)provider.GetRequiredService<ISyncSupervisor>());
        services.AddScoped<IConnectionRegistrationService, ConnectionRegistrationService>();
        services.AddScoped<IConnectionRemovalService, ConnectionRemovalService>();
        services.AddScoped<IConnectionResyncService, ConnectionResyncService>();

        return services;
    }
}