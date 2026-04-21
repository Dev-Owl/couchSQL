using System.Reflection;
using CouchSql.Core.Design;
using CouchSql.Infrastructure.PostgreSql;

namespace CouchSql.Tests.Unit;

public sealed class PostgreSqlServiceDdlTests
{
    [Fact]
    public void BuildCreateTableSql_Uses_Primary_Key_On_Id()
    {
        var method = typeof(PostgreSqlService).GetMethod("BuildCreateTableSql", BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(method);

        var sql = (string)method!.Invoke(null, new object[]
        {
            "customers",
            new List<CouchSqlFieldDefinition>
            {
                new()
                {
                    Column = "customer_id",
                    Path = "customer.id",
                    Type = "text",
                    Required = true
                }
            }
        })!;

        Assert.Contains("\"_id\" text primary key", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("\"_source_seq\" text not null", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("\"_synced_at\" timestamptz not null", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void BuildCreateIndexSql_Uses_User_Defined_Index_Request()
    {
        var method = typeof(PostgreSqlService).GetMethod("BuildCreateIndexSql", BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(method);

        var knownColumns = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["customer_id"] = "customer_id",
            ["_id"] = "_id"
        };

        var sql = (string)method!.Invoke(null, new object[]
        {
            "customers",
            new CouchSqlIndexDefinition
            {
                Name = "ix_customers_customer_id",
                Columns = ["customer_id"],
                Unique = true
            },
            knownColumns
        })!;

        Assert.Equal(
            "create unique index if not exists \"ix_customers_customer_id\" on \"customers\" (\"customer_id\")",
            sql);
    }

    [Fact]
    public void BuildCreateIndexSql_Uses_Canonical_Column_Casing()
    {
        var method = typeof(PostgreSqlService).GetMethod("BuildCreateIndexSql", BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(method);

        var knownColumns = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["lastEdited"] = "lastEdited"
        };

        var sql = (string)method!.Invoke(null, new object[]
        {
            "components",
            new CouchSqlIndexDefinition
            {
                Name = "ix_LastEdited",
                Columns = ["LastEdited"],
                Unique = false
            },
            knownColumns
        })!;

        Assert.Equal(
            "create index if not exists \"ix_LastEdited\" on \"components\" (\"lastEdited\")",
            sql);
    }
}