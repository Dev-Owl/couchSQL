using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using CouchSql.Core.Design;

namespace CouchSql.Infrastructure.Sync;

public static class SyncProjectionCompiler
{
    public static IReadOnlyList<CompiledTypeDefinition> Compile(CouchSqlDesignDocument document)
    {
        var configuration = document.CouchSql ?? throw new InvalidOperationException("The design document did not contain the couchsql configuration object.");
        return configuration.Types
            .Select(type => new CompiledTypeDefinition(
                type.Name ?? throw new InvalidOperationException("Type name is required."),
                type.Table ?? throw new InvalidOperationException("Table name is required."),
                type.Fields,
                ParseIdentifyRule(type.Identify, 0)))
            .ToArray();
    }

    public static JsonElement BuildSelector(IReadOnlyList<CompiledTypeDefinition> types)
    {
        if (types.Count == 1)
        {
            return JsonDocument.Parse(BuildRuleNode(types[0].Rule).ToJsonString()).RootElement.Clone();
        }

        var branches = new JsonArray();
        foreach (var type in types)
        {
            branches.Add(BuildRuleNode(type.Rule));
        }

        var selector = new JsonObject
        {
            ["$or"] = branches
        };

        return JsonDocument.Parse(selector.ToJsonString()).RootElement.Clone();
    }

    public static CompiledTypeDefinition? MatchType(IReadOnlyList<CompiledTypeDefinition> types, JsonElement document)
    {
        var matches = types.Where(type => Evaluate(type.Rule, document)).ToArray();
        return matches.Length switch
        {
            0 => null,
            1 => matches[0],
            _ => throw new InvalidOperationException($"Document '{ReadString(document, "_id") ?? "<unknown>"}' matched more than one configured type.")
        };
    }

    public static IReadOnlyDictionary<string, object?> ProjectFields(CompiledTypeDefinition type, JsonElement document)
    {
        var values = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var field in type.Fields)
        {
            values[field.Column ?? string.Empty] = ConvertFieldValue(field, document);
        }

        return values;
    }

    public static bool Evaluate(IdentifyRule rule, JsonElement document)
    {
        return rule switch
        {
            AllRule all => all.Children.All(child => Evaluate(child, document)),
            AnyRule any => any.Children.Any(child => Evaluate(child, document)),
            EqualsRule equals => TryResolvePath(document, equals.Path, out var resolvedEquals) && JsonValuesEqual(resolvedEquals, equals.Expected),
            ExistsRule exists => TryResolvePath(document, exists.Path, out _),
            ContainsRule contains => EvaluateContains(contains, document),
            _ => false
        };
    }

    public static bool TryResolvePath(JsonElement document, string path, out JsonElement value)
    {
        value = document;
        var index = 0;

        while (index < path.Length)
        {
            if (path[index] == '.')
            {
                index++;
                continue;
            }

            var propertyStart = index;
            while (index < path.Length && path[index] is not '.' and not '[')
            {
                index++;
            }

            if (index > propertyStart)
            {
                var propertyName = path[propertyStart..index];
                if (value.ValueKind != JsonValueKind.Object || !value.TryGetProperty(propertyName, out value))
                {
                    value = default;
                    return false;
                }
            }

            while (index < path.Length && path[index] == '[')
            {
                index++;
                var arrayStart = index;
                while (index < path.Length && char.IsDigit(path[index]))
                {
                    index++;
                }

                if (index == arrayStart || index >= path.Length || path[index] != ']' || !int.TryParse(path[arrayStart..index], NumberStyles.None, CultureInfo.InvariantCulture, out var arrayIndex))
                {
                    value = default;
                    return false;
                }

                if (value.ValueKind != JsonValueKind.Array)
                {
                    value = default;
                    return false;
                }

                var arrayItems = value.EnumerateArray().ToArray();
                if (arrayIndex < 0 || arrayIndex >= arrayItems.Length)
                {
                    value = default;
                    return false;
                }

                value = arrayItems[arrayIndex];
                index++;
            }
        }

        return true;
    }

    private static IdentifyRule ParseIdentifyRule(JsonElement element, int depth)
    {
        if (depth > 16)
        {
            throw new InvalidOperationException("Identify rule nesting exceeded the supported depth.");
        }

        if (element.TryGetProperty("all", out var allElement))
        {
            return new AllRule(allElement.EnumerateArray().Select(child => ParseIdentifyRule(child, depth + 1)).ToArray());
        }

        if (element.TryGetProperty("any", out var anyElement))
        {
            return new AnyRule(anyElement.EnumerateArray().Select(child => ParseIdentifyRule(child, depth + 1)).ToArray());
        }

        var path = element.GetProperty("path").GetString() ?? throw new InvalidOperationException("Identify path is required.");

        if (element.TryGetProperty("equals", out var equalsElement))
        {
            return new EqualsRule(path, equalsElement.Clone());
        }

        if (element.TryGetProperty("exists", out _))
        {
            return new ExistsRule(path);
        }

        if (element.TryGetProperty("contains", out var containsElement))
        {
            return new ContainsRule(path, containsElement.Clone());
        }

        throw new InvalidOperationException("Unsupported identify rule.");
    }

    private static JsonNode BuildRuleNode(IdentifyRule rule)
    {
        return rule switch
        {
            AllRule all => new JsonObject
            {
                ["$and"] = new JsonArray(all.Children.Select(BuildRuleNode).ToArray())
            },
            AnyRule any => new JsonObject
            {
                ["$or"] = new JsonArray(any.Children.Select(BuildRuleNode).ToArray())
            },
            EqualsRule equals => new JsonObject
            {
                [equals.Path] = JsonNode.Parse(equals.Expected.GetRawText())
            },
            ExistsRule exists => new JsonObject
            {
                [exists.Path] = new JsonObject
                {
                    ["$exists"] = true
                }
            },
            ContainsRule contains => BuildContainsNode(contains),
            _ => throw new InvalidOperationException("Unsupported identify rule.")
        };
    }

    private static JsonNode BuildContainsNode(ContainsRule contains)
    {
        if (contains.Expected.ValueKind == JsonValueKind.String)
        {
            return new JsonObject
            {
                [contains.Path] = new JsonObject
                {
                    ["$regex"] = Regex.Escape(contains.Expected.GetString()!)
                }
            };
        }

        return new JsonObject
        {
            [contains.Path] = new JsonObject
            {
                ["$elemMatch"] = new JsonObject
                {
                    ["$eq"] = JsonNode.Parse(contains.Expected.GetRawText())
                }
            }
        };
    }

    private static bool EvaluateContains(ContainsRule contains, JsonElement document)
    {
        if (!TryResolvePath(document, contains.Path, out var resolvedContains))
        {
            return false;
        }

        if (resolvedContains.ValueKind == JsonValueKind.Array)
        {
            return resolvedContains.EnumerateArray().Any(item => JsonValuesEqual(item, contains.Expected));
        }

        if (resolvedContains.ValueKind == JsonValueKind.String && contains.Expected.ValueKind == JsonValueKind.String)
        {
            return resolvedContains.GetString()?.Contains(contains.Expected.GetString()!, StringComparison.Ordinal) == true;
        }

        return false;
    }

    private static object? ConvertFieldValue(CouchSqlFieldDefinition field, JsonElement document)
    {
        if (!TryResolvePath(document, field.Path ?? string.Empty, out var resolved) || resolved.ValueKind == JsonValueKind.Null)
        {
            if (field.Required)
            {
                throw new InvalidOperationException($"Required field '{field.Column}' is missing from the source document.");
            }

            return null;
        }

        var configuredType = (field.Type ?? string.Empty).ToLowerInvariant();
        try
        {
            return configuredType switch
            {
                "text" when resolved.ValueKind == JsonValueKind.String => ApplyTextTransform(resolved.GetString(), field.Transform),
                "integer" when resolved.ValueKind == JsonValueKind.Number && resolved.TryGetInt32(out var intValue) => intValue,
                "bigint" when resolved.ValueKind == JsonValueKind.Number && resolved.TryGetInt64(out var longValue) => longValue,
                "numeric" when resolved.ValueKind == JsonValueKind.Number => decimal.Parse(resolved.GetRawText(), CultureInfo.InvariantCulture),
                "boolean" when resolved.ValueKind is JsonValueKind.True or JsonValueKind.False => resolved.GetBoolean(),
                "date" when resolved.ValueKind == JsonValueKind.String => DateOnly.Parse(resolved.GetString()!, CultureInfo.InvariantCulture),
                "timestamp" when resolved.ValueKind == JsonValueKind.String => DateTime.Parse(resolved.GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
                "timestamptz" when resolved.ValueKind == JsonValueKind.String => DateTimeOffset.Parse(resolved.GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
                "uuid" when resolved.ValueKind == JsonValueKind.String => Guid.Parse(resolved.GetString()!),
                "jsonb" => resolved.GetRawText(),
                "double precision" when resolved.ValueKind == JsonValueKind.Number => double.Parse(resolved.GetRawText(), CultureInfo.InvariantCulture),
                _ => throw new InvalidOperationException($"Field '{field.Column}' could not be converted to PostgreSQL type '{field.Type}'.")
            };
        }
        catch (Exception exception) when (exception is FormatException or OverflowException or InvalidOperationException)
        {
            throw new InvalidOperationException($"Field '{field.Column}' could not be converted to PostgreSQL type '{field.Type}'.", exception);
        }
    }

    private static string? ApplyTextTransform(string? value, CouchSqlFieldTransformDefinition? transform)
    {
        if (value is null || transform is null)
        {
            return value;
        }

        return string.Concat(transform.Prefix ?? string.Empty, value, transform.Append ?? string.Empty);
    }

    private static bool JsonValuesEqual(JsonElement left, JsonElement right)
    {
        if (left.ValueKind == JsonValueKind.Number && right.ValueKind == JsonValueKind.Number)
        {
            return decimal.Parse(left.GetRawText(), CultureInfo.InvariantCulture) == decimal.Parse(right.GetRawText(), CultureInfo.InvariantCulture);
        }

        return left.ValueKind == right.ValueKind && left.GetRawText() == right.GetRawText();
    }

    private static string? ReadString(JsonElement document, string propertyName)
    {
        return document.TryGetProperty(propertyName, out var property) ? property.GetString() : null;
    }
}

public sealed record CompiledTypeDefinition(
    string Name,
    string Table,
    IReadOnlyList<CouchSqlFieldDefinition> Fields,
    IdentifyRule Rule);