using System.Text;
using System.Text.RegularExpressions;
using CouchSql.Core.Interfaces;
using CouchSql.Core.Models;

namespace CouchSql.Infrastructure.Query;

public sealed class SqlQueryValidator : ISqlQueryValidator
{
    private static readonly Regex ForbiddenKeywordRegex = new(
        @"\b(insert|update|delete|alter|drop|create|grant|truncate|merge|call|copy|refresh|vacuum|reindex|analyze|comment|do)\b",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);

    public ValidatedSqlQuery Validate(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
        {
            throw new InvalidOperationException("SQL text is required.");
        }

        var normalized = Normalize(sql);
        if (string.IsNullOrWhiteSpace(normalized))
        {
            throw new InvalidOperationException("SQL text did not contain an executable statement.");
        }

        if (normalized.Contains(';'))
        {
            throw new InvalidOperationException("Only a single SELECT statement is allowed.");
        }

        var firstToken = normalized.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).FirstOrDefault();
        if (!string.Equals(firstToken, "select", StringComparison.OrdinalIgnoreCase) &&
            !string.Equals(firstToken, "with", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException("Only SELECT statements are allowed.");
        }

        if (ForbiddenKeywordRegex.IsMatch(normalized))
        {
            throw new InvalidOperationException("The supplied SQL contains disallowed commands.");
        }

        var wrappedSql = $"select * from ({sql}) as couchsql_query limit @__couchsql_limit";
        return new ValidatedSqlQuery(normalized, wrappedSql);
    }

    private static string Normalize(string sql)
    {
        var builder = new StringBuilder(sql.Length);
        var inSingleQuotedString = false;
        var inDoubleQuotedIdentifier = false;
        var inLineComment = false;
        var inBlockComment = false;

        for (var index = 0; index < sql.Length; index++)
        {
            var current = sql[index];
            var next = index + 1 < sql.Length ? sql[index + 1] : '\0';

            if (inLineComment)
            {
                if (current == '\n')
                {
                    inLineComment = false;
                    builder.Append(' ');
                }

                continue;
            }

            if (inBlockComment)
            {
                if (current == '*' && next == '/')
                {
                    inBlockComment = false;
                    index++;
                    builder.Append(' ');
                }

                continue;
            }

            if (inSingleQuotedString)
            {
                if (current == '\'' && next == '\'')
                {
                    index++;
                    continue;
                }

                if (current == '\'')
                {
                    inSingleQuotedString = false;
                }

                continue;
            }

            if (inDoubleQuotedIdentifier)
            {
                if (current == '"' && next == '"')
                {
                    index++;
                    continue;
                }

                if (current == '"')
                {
                    inDoubleQuotedIdentifier = false;
                }

                continue;
            }

            if (current == '-' && next == '-')
            {
                inLineComment = true;
                index++;
                continue;
            }

            if (current == '/' && next == '*')
            {
                inBlockComment = true;
                index++;
                continue;
            }

            if (current == '\'')
            {
                inSingleQuotedString = true;
                continue;
            }

            if (current == '"')
            {
                inDoubleQuotedIdentifier = true;
                continue;
            }

            builder.Append(char.ToLowerInvariant(current));
        }

        return Regex.Replace(builder.ToString(), @"\s+", " ").Trim();
    }
}