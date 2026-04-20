using CouchSql.Infrastructure.Services;

namespace CouchSql.Tests.Unit;

public sealed class DatabaseNameNormalizerTests
{
    [Theory]
    [InlineData("Sales-Db", "sales_db")]
    [InlineData("123 Orders", "db_123_orders")]
    [InlineData("customer__events", "customer_events")]
    public void Normalize_Produces_Safe_Database_Name(string input, string expected)
    {
        var normalized = DatabaseNameNormalizer.Normalize(input);

        Assert.Equal(expected, normalized);
    }

    [Fact]
    public void Normalize_Rejects_Empty_Result()
    {
        var exception = Assert.Throws<InvalidOperationException>(() => DatabaseNameNormalizer.Normalize("---"));

        Assert.Contains("empty after normalization", exception.Message);
    }
}