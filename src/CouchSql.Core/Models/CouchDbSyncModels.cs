using System.Text.Json;

namespace CouchSql.Core.Models;

public sealed record CouchDbChangesResponse(
    IReadOnlyList<CouchDbChangeResult> Results,
    string LastSequence,
    long? Pending);

public sealed record CouchDbChangeResult(
    string Sequence,
    string Id,
    string? Revision,
    bool Deleted,
    JsonElement? Document);