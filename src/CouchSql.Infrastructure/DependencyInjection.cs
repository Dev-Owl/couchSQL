using CouchSql.Core.Interfaces;
using CouchSql.Infrastructure.CouchDb;
using CouchSql.Infrastructure.PostgreSql;
using CouchSql.Infrastructure.Query;
using CouchSql.Infrastructure.Schema;
using CouchSql.Infrastructure.Security;
using CouchSql.Infrastructure.Services;
using Microsoft.Extensions.DependencyInjection;

namespace CouchSql.Infrastructure;

public static class DependencyInjection
{
    public static IServiceCollection AddCouchSqlInfrastructure(this IServiceCollection services)
    {
        services.AddHttpClient<ICouchDbClient, CouchDbClient>(client =>
        {
            client.Timeout = TimeSpan.FromSeconds(30);
        });

        services.AddSingleton<ICredentialProtector, FileKeyCredentialProtector>();
        services.AddSingleton<ISqlQueryValidator, SqlQueryValidator>();
        services.AddSingleton<IDesignContractValidator, DesignContractValidator>();
        services.AddSingleton<IAdminMetadataRepository, AdminMetadataRepository>();
        services.AddSingleton<IPostgreSqlService, PostgreSqlService>();
        services.AddSingleton<IQuerySettingsService, QuerySettingsService>();
        services.AddSingleton<IStartupInitializer, StartupInitializer>();
        services.AddScoped<IConnectionRegistrationService, ConnectionRegistrationService>();
        services.AddScoped<IConnectionRemovalService, ConnectionRemovalService>();

        return services;
    }
}