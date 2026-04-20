using System.Text.Json;
using System.Text.Json.Serialization;

namespace CouchSql.Core.Design;

public sealed class CouchSqlDesignDocument
{
    [JsonPropertyName("_id")]
    public string? Id { get; set; }

    [JsonPropertyName("_rev")]
    public string? Revision { get; set; }

    [JsonPropertyName("couchsql")]
    public CouchSqlDesignConfiguration? CouchSql { get; set; }
}

public sealed class CouchSqlDesignConfiguration
{
    [JsonPropertyName("schemaVersion")]
    public int SchemaVersion { get; set; }

    [JsonPropertyName("types")]
    public List<CouchSqlTypeDefinition> Types { get; set; } = new();
}

public sealed class CouchSqlTypeDefinition
{
    [JsonPropertyName("name")]
    public string? Name { get; set; }

    [JsonPropertyName("table")]
    public string? Table { get; set; }

    [JsonPropertyName("identify")]
    public JsonElement Identify { get; set; }

    [JsonPropertyName("fields")]
    public List<CouchSqlFieldDefinition> Fields { get; set; } = new();

    [JsonPropertyName("indexes")]
    public List<CouchSqlIndexDefinition> Indexes { get; set; } = new();
}

public sealed class CouchSqlFieldDefinition
{
    [JsonPropertyName("column")]
    public string? Column { get; set; }

    [JsonPropertyName("path")]
    public string? Path { get; set; }

    [JsonPropertyName("type")]
    public string? Type { get; set; }

    [JsonPropertyName("required")]
    public bool Required { get; set; }
}

public sealed class CouchSqlIndexDefinition
{
    [JsonPropertyName("name")]
    public string? Name { get; set; }

    [JsonPropertyName("columns")]
    public List<string> Columns { get; set; } = new();

    [JsonPropertyName("unique")]
    public bool Unique { get; set; }
}

public abstract record IdentifyRule;

public sealed record AllRule(IReadOnlyList<IdentifyRule> Children) : IdentifyRule;

public sealed record AnyRule(IReadOnlyList<IdentifyRule> Children) : IdentifyRule;

public sealed record EqualsRule(string Path, JsonElement Expected) : IdentifyRule;

public sealed record ExistsRule(string Path) : IdentifyRule;

public sealed record ContainsRule(string Path, JsonElement Expected) : IdentifyRule;