using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using CouchSql.Core.Design;
using CouchSql.Core.Interfaces;
using CouchSql.Core.Models;
using Microsoft.Extensions.Logging;

namespace CouchSql.Infrastructure.CouchDb;

public sealed class CouchDbClient(HttpClient httpClient, ILogger<CouchDbClient> logger) : ICouchDbClient
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

    public async Task<CouchDbChangesResponse> GetChangesAsync(
        string baseUrl,
        string databaseName,
        string username,
        string passwordOrToken,
        string since,
        JsonElement selector,
        string feed,
        bool includeDocs,
        int? limit,
        int? seqInterval,
        int? heartbeatMilliseconds,
        CancellationToken cancellationToken)
    {
        var query = new List<string>
        {
            $"feed={Uri.EscapeDataString(feed)}",
            "filter=_selector",
            $"include_docs={includeDocs.ToString().ToLowerInvariant()}",
            $"since={Uri.EscapeDataString(since)}"
        };

        if (limit.HasValue)
        {
            query.Add($"limit={limit.Value}");
        }

        if (seqInterval.HasValue)
        {
            query.Add($"seq_interval={seqInterval.Value}");
        }

        if (heartbeatMilliseconds.HasValue)
        {
            query.Add($"heartbeat={heartbeatMilliseconds.Value}");
        }

        var uri = BuildUri(baseUrl, $"{databaseName}/_changes?{string.Join("&", query)}");
        var selectorJson = JsonSerializer.Serialize(new { selector });
        logger.LogInformation(
            "CouchDB _changes request: {Method} {Uri} feed={Feed} since={Since} includeDocs={IncludeDocs} limit={Limit} seqInterval={SeqInterval} heartbeat={HeartbeatMilliseconds} selector={Selector}",
            HttpMethod.Post,
            uri,
            feed,
            since,
            includeDocs,
            limit,
            seqInterval,
            heartbeatMilliseconds,
            selectorJson);

        using var request = CreateRequest(HttpMethod.Post, uri, username, passwordOrToken);
        request.Content = new StringContent(selectorJson, Encoding.UTF8, "application/json");

        using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        if (!response.IsSuccessStatusCode)
        {
            var body = await response.Content.ReadAsStringAsync(cancellationToken);
            throw new InvalidOperationException($"CouchDB changes feed request failed with status {(int)response.StatusCode}: {body}");
        }

        await using var responseStream = await response.Content.ReadAsStreamAsync(cancellationToken);
        using var payload = await JsonDocument.ParseAsync(responseStream, cancellationToken: cancellationToken);
        var root = payload.RootElement;

        var results = new List<CouchDbChangeResult>();
        foreach (var item in root.GetProperty("results").EnumerateArray())
        {
            item.TryGetProperty("doc", out var documentElement);
            JsonElement? document = documentElement.ValueKind is JsonValueKind.Undefined or JsonValueKind.Null
                ? null
                : documentElement.Clone();

            results.Add(new CouchDbChangeResult(
                ReadSequence(item.GetProperty("seq")),
                item.GetProperty("id").GetString() ?? string.Empty,
                ReadRevision(item, document),
                item.TryGetProperty("deleted", out var deletedElement) && deletedElement.ValueKind == JsonValueKind.True,
                document));
        }

        long? pending = root.TryGetProperty("pending", out var pendingElement) && pendingElement.TryGetInt64(out var pendingValue)
            ? pendingValue
            : null;

        return new CouchDbChangesResponse(results, ReadSequence(root.GetProperty("last_seq")), pending);
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

    private static string ReadSequence(JsonElement value)
    {
        return value.ValueKind == JsonValueKind.String
            ? value.GetString() ?? string.Empty
            : value.GetRawText();
    }

    private static string? ReadRevision(JsonElement item, JsonElement? document)
    {
        if (document.HasValue && document.Value.TryGetProperty("_rev", out var revisionElement))
        {
            return revisionElement.GetString();
        }

        if (!item.TryGetProperty("changes", out var changesElement) || changesElement.ValueKind != JsonValueKind.Array)
        {
            return null;
        }

        foreach (var change in changesElement.EnumerateArray())
        {
            if (change.TryGetProperty("rev", out var revision))
            {
                return revision.GetString();
            }
        }

        return null;
    }
}