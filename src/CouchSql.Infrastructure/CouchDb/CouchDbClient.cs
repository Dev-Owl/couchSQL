using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using CouchSql.Core.Design;
using CouchSql.Core.Interfaces;

namespace CouchSql.Infrastructure.CouchDb;

public sealed class CouchDbClient(HttpClient httpClient) : ICouchDbClient
{
    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public async Task ValidateDatabaseAsync(
        string baseUrl,
        string databaseName,
        string username,
        string passwordOrToken,
        CancellationToken cancellationToken)
    {
        using var request = CreateRequest(HttpMethod.Get, BuildUri(baseUrl, databaseName), username, passwordOrToken);
        using var response = await httpClient.SendAsync(request, cancellationToken);

        if (response.IsSuccessStatusCode)
        {
            return;
        }

        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        throw new InvalidOperationException($"CouchDB database validation failed with status {(int)response.StatusCode}: {body}");
    }

    public async Task<CouchSqlDesignDocument> GetDesignDocumentAsync(
        string baseUrl,
        string databaseName,
        string username,
        string passwordOrToken,
        CancellationToken cancellationToken)
    {
        using var request = CreateRequest(HttpMethod.Get, BuildUri(baseUrl, $"{databaseName}/_design/couchsql"), username, passwordOrToken);
        using var response = await httpClient.SendAsync(request, cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            var body = await response.Content.ReadAsStringAsync(cancellationToken);
            throw new InvalidOperationException($"Failed to load _design/couchsql with status {(int)response.StatusCode}: {body}");
        }

        await using var responseStream = await response.Content.ReadAsStreamAsync(cancellationToken);
        var document = await JsonSerializer.DeserializeAsync<CouchSqlDesignDocument>(responseStream, SerializerOptions, cancellationToken);

        return document ?? throw new InvalidOperationException("CouchDB returned an empty design document payload.");
    }

    private static HttpRequestMessage CreateRequest(HttpMethod method, Uri uri, string username, string passwordOrToken)
    {
        var request = new HttpRequestMessage(method, uri);
        var credentials = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:{passwordOrToken}"));
        request.Headers.Authorization = new AuthenticationHeaderValue("Basic", credentials);
        return request;
    }

    private static Uri BuildUri(string baseUrl, string relativePath)
    {
        if (!Uri.TryCreate(baseUrl, UriKind.Absolute, out var rootUri))
        {
            throw new InvalidOperationException("The CouchDB base URL is not a valid absolute URI.");
        }

        var trimmed = relativePath.TrimStart('/');
        return new Uri(rootUri.ToString().TrimEnd('/') + "/" + trimmed);
    }
}