using System.Text;

namespace CouchSql.Infrastructure.Services;

public static class DatabaseNameNormalizer
{
    public static string Normalize(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new InvalidOperationException("A database name is required.");
        }

        var builder = new StringBuilder(value.Length);
        var previousWasUnderscore = false;

        foreach (var character in value.Trim().ToLowerInvariant())
        {
            var normalized = char.IsLetterOrDigit(character) ? character : '_';
            if (normalized == '_')
            {
                if (previousWasUnderscore)
                {
                    continue;
                }

                previousWasUnderscore = true;
            }
            else
            {
                previousWasUnderscore = false;
            }

            builder.Append(normalized);
        }

        var cleaned = builder.ToString().Trim('_');
        if (string.IsNullOrWhiteSpace(cleaned))
        {
            throw new InvalidOperationException("The database name is empty after normalization.");
        }

        if (!(char.IsLetter(cleaned[0]) || cleaned[0] == '_'))
        {
            cleaned = "db_" + cleaned;
        }

        return cleaned.Length <= 63 ? cleaned : cleaned[..63];
    }
}