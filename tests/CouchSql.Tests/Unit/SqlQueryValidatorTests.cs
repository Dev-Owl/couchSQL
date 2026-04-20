using CouchSql.Infrastructure.Query;

namespace CouchSql.Tests.Unit;

public sealed class SqlQueryValidatorTests
{
    private readonly SqlQueryValidator _validator = new();

    [Fact]
    public void Validate_Allows_Select_Query()
    {
        var result = _validator.Validate("select id, name from customers order by name");

        Assert.Contains("select id, name from customers order by name", result.NormalizedSql);
        Assert.Contains("limit @__couchsql_limit", result.WrappedSql);
    }

    [Fact]
    public void Validate_Allows_With_Query()
    {
        var result = _validator.Validate("with source as (select 1 as id) select id from source");

        Assert.StartsWith("with", result.NormalizedSql);
    }

    [Fact]
    public void Validate_Rejects_NonSelect_Query()
    {
        var exception = Assert.Throws<InvalidOperationException>(() => _validator.Validate("delete from customers"));

        Assert.Contains("Only SELECT statements are allowed", exception.Message);
    }

    [Fact]
    public void Validate_Rejects_Multiple_Statements()
    {
        var exception = Assert.Throws<InvalidOperationException>(() => _validator.Validate("select 1; select 2;"));

        Assert.Contains("single SELECT statement", exception.Message);
    }
}