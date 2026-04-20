using System.Text.Json;
using CouchSql.Core.Design;
using CouchSql.Core.Interfaces;

namespace CouchSql.Infrastructure.Schema;

public sealed class DesignContractValidator : IDesignContractValidator
{
    private static readonly HashSet<string> SupportedPostgreSqlTypes = new(StringComparer.OrdinalIgnoreCase)
    {
        "text",
        "integer",
        "bigint",
        "numeric",
        "boolean",
        "timestamp",
        "timestamptz",
        "jsonb",
        "uuid",
        "double precision"
    };

    public void Validate(CouchSqlDesignDocument document)
    {
        if (!string.Equals(document.Id, "_design/couchsql", StringComparison.Ordinal))
        {
            throw new InvalidOperationException("The design document _id must be _design/couchsql.");
        }

        if (document.CouchSql is null)
        {
            throw new InvalidOperationException("The design document must contain a couchsql configuration object.");
        }

        if (document.CouchSql.SchemaVersion != 1)
        {
            throw new InvalidOperationException("Only schemaVersion 1 is supported in the first delivery.");
        }

        if (document.CouchSql.Types.Count == 0)
        {
            throw new InvalidOperationException("At least one type must be declared in the design document.");
        }

        EnsureUnique(document.CouchSql.Types.Select(type => type.Name), "Type names must be unique.");
        EnsureUnique(document.CouchSql.Types.Select(type => type.Table), "Table names must be unique.");

        foreach (var type in document.CouchSql.Types)
        {
            ValidateType(type);
        }
    }

    private static void ValidateType(CouchSqlTypeDefinition type)
    {
        if (string.IsNullOrWhiteSpace(type.Name))
        {
            throw new InvalidOperationException("Each type must define a non-empty logical name.");
        }

        if (!IsSafeIdentifier(type.Table))
        {
            throw new InvalidOperationException($"The table name '{type.Table}' is not a safe PostgreSQL identifier.");
        }

        if (type.Fields.Count == 0)
        {
            throw new InvalidOperationException($"Type '{type.Name}' must declare at least one mapped field.");
        }

        _ = ParseIdentifyRule(type.Identify);

        EnsureUnique(type.Fields.Select(field => field.Column), $"Mapped columns must be unique for type '{type.Name}'.");

        foreach (var field in type.Fields)
        {
            if (!IsSafeIdentifier(field.Column))
            {
                throw new InvalidOperationException($"Field column '{field.Column}' is not a safe PostgreSQL identifier.");
            }

            if (string.IsNullOrWhiteSpace(field.Path))
            {
                throw new InvalidOperationException($"Field '{field.Column}' must declare a non-empty path.");
            }

            if (string.IsNullOrWhiteSpace(field.Type) || !SupportedPostgreSqlTypes.Contains(field.Type))
            {
                throw new InvalidOperationException($"Field '{field.Column}' uses unsupported PostgreSQL type '{field.Type}'.");
            }
        }

        EnsureUnique(type.Indexes.Select(index => index.Name), $"Index names must be unique for type '{type.Name}'.");

        var knownColumns = type.Fields.Select(field => field.Column ?? string.Empty)
            .Append("_id")
            .Append("_rev")
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        foreach (var index in type.Indexes)
        {
            if (!IsSafeIdentifier(index.Name))
            {
                throw new InvalidOperationException($"Index name '{index.Name}' is not a safe PostgreSQL identifier.");
            }

            if (index.Columns.Count == 0)
            {
                throw new InvalidOperationException($"Index '{index.Name}' must reference at least one column.");
            }

            foreach (var column in index.Columns)
            {
                if (!knownColumns.Contains(column))
                {
                    throw new InvalidOperationException($"Index '{index.Name}' references unknown column '{column}'.");
                }
            }
        }
    }

    private static IdentifyRule ParseIdentifyRule(JsonElement element)
    {
        if (element.ValueKind != JsonValueKind.Object)
        {
            throw new InvalidOperationException("Each identify rule must be a JSON object.");
        }

        if (element.TryGetProperty("all", out var allElement))
        {
            EnsureOnlyProperties(element, "all");
            return new AllRule(ParseChildren(allElement));
        }

        if (element.TryGetProperty("any", out var anyElement))
        {
            EnsureOnlyProperties(element, "any");
            return new AnyRule(ParseChildren(anyElement));
        }

        if (!element.TryGetProperty("path", out var pathElement) || string.IsNullOrWhiteSpace(pathElement.GetString()))
        {
            throw new InvalidOperationException("Leaf identify rules must declare a non-empty path.");
        }

        var path = pathElement.GetString()!;

        if (element.TryGetProperty("equals", out var equalsElement))
        {
            EnsureOnlyProperties(element, "path", "equals");
            return new EqualsRule(path, equalsElement.Clone());
        }

        if (element.TryGetProperty("exists", out var existsElement))
        {
            EnsureOnlyProperties(element, "path", "exists");
            if (existsElement.ValueKind != JsonValueKind.True)
            {
                throw new InvalidOperationException("The exists predicate only supports the literal value true in the first delivery.");
            }

            return new ExistsRule(path);
        }

        if (element.TryGetProperty("contains", out var containsElement))
        {
            EnsureOnlyProperties(element, "path", "contains");
            return new ContainsRule(path, containsElement.Clone());
        }

        throw new InvalidOperationException("Unsupported identify predicate. Supported predicates are all, any, equals, exists, and contains.");
    }

    private static IReadOnlyList<IdentifyRule> ParseChildren(JsonElement element)
    {
        if (element.ValueKind != JsonValueKind.Array || element.GetArrayLength() == 0)
        {
            throw new InvalidOperationException("The all and any identify predicates must contain at least one child rule.");
        }

        return element.EnumerateArray().Select(ParseIdentifyRule).ToArray();
    }

    private static void EnsureOnlyProperties(JsonElement element, params string[] allowedNames)
    {
        var allowed = allowedNames.ToHashSet(StringComparer.Ordinal);
        foreach (var property in element.EnumerateObject())
        {
            if (!allowed.Contains(property.Name))
            {
                throw new InvalidOperationException($"Unsupported property '{property.Name}' in identify rule.");
            }
        }
    }

    private static void EnsureUnique(IEnumerable<string?> values, string errorMessage)
    {
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var value in values)
        {
            if (string.IsNullOrWhiteSpace(value) || !seen.Add(value))
            {
                throw new InvalidOperationException(errorMessage);
            }
        }
    }

    private static bool IsSafeIdentifier(string? identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
        {
            return false;
        }

        if (!(char.IsLetter(identifier[0]) || identifier[0] == '_'))
        {
            return false;
        }

        return identifier.All(character => char.IsLetterOrDigit(character) || character == '_');
    }
}