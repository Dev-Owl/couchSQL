using System.Net;

namespace CouchSql.Api.Middleware;

public sealed class LocalAdminOnlyMiddleware(RequestDelegate next)
{
    public async Task InvokeAsync(HttpContext context)
    {
        if (RequiresLocalAccess(context.Request.Path) && !IsLocalRequest(context))
        {
            context.Response.StatusCode = StatusCodes.Status404NotFound;
            return;
        }

        await next(context);
    }

    private static bool RequiresLocalAccess(PathString path)
    {
        return path.StartsWithSegments("/internal", StringComparison.OrdinalIgnoreCase) ||
               path.StartsWithSegments("/swagger/admin", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsLocalRequest(HttpContext context)
    {
        var address = context.Connection.RemoteIpAddress;
        return address is null || IPAddress.IsLoopback(address);
    }
}