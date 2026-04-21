using System.Text.Json;
using CouchSql.Core.Design;
using CouchSql.Infrastructure.Schema;

namespace CouchSql.Tests.Unit;

public sealed class DesignContractValidatorTests
{
    private readonly DesignContractValidator _validator = new();

    [Fact]
    public void Validate_Accepts_Valid_Design_Document()
    {
        var document = CreateValidDocument();

        _validator.Validate(document);
    }

    [Fact]
    public void Validate_Rejects_Unsupported_PostgreSql_Type()
    {
        var document = CreateValidDocument();
        document.CouchSql!.Types[0].Fields[0].Type = "money";

        var exception = Assert.Throws<InvalidOperationException>(() => _validator.Validate(document));

        Assert.Contains("unsupported PostgreSQL type", exception.Message);
    }

    [Fact]
    public void Validate_Rejects_Unsupported_Identify_Predicate()
    {
        var document = CreateValidDocument();
        document.CouchSql!.Types[0].Identify = JsonDocument.Parse("{\"path\":\"meta.entity\",\"regex\":\"customer\"}").RootElement.Clone();

        var exception = Assert.Throws<InvalidOperationException>(() => _validator.Validate(document));

        Assert.Contains("Unsupported identify predicate", exception.Message);
    }

    [Fact]
    public void Validate_Allows_Contains_On_Id()
    {
        var document = CreateValidDocument();
        document.CouchSql!.Types[0].Identify = JsonDocument.Parse("{\"path\":\"_id\",\"contains\":\"component\"}").RootElement.Clone();

        _validator.Validate(document);
    }

    [Fact]
    public void Validate_Rejects_Empty_Field_Transform()
    {
        var document = CreateValidDocument();
        document.CouchSql!.Types[0].Fields[0].Transform = new CouchSqlFieldTransformDefinition();

        var exception = Assert.Throws<InvalidOperationException>(() => _validator.Validate(document));

        Assert.Contains("transform must define", exception.Message);
    }

    private static CouchSqlDesignDocument CreateValidDocument()
    {
        return new CouchSqlDesignDocument
        {
            Id = "_design/couchsql",
            Revision = "1-test",
            CouchSql = new CouchSqlDesignConfiguration
            {
                SchemaVersion = 1,
                Types =
                [
                    new CouchSqlTypeDefinition
                    {
                        Name = "customer",
                        Table = "customers",
                        Identify = JsonDocument.Parse("{\"all\":[{\"path\":\"meta.entity\",\"equals\":\"customer\"},{\"path\":\"customer.id\",\"exists\":true}]}").RootElement.Clone(),
                        Fields =
                        [
                            new CouchSqlFieldDefinition
                            {
                                Column = "customer_id",
                                Path = "customer.id",
                                Type = "text",
                                Required = true
                            }
                        ],
                        Indexes =
                        [
                            new CouchSqlIndexDefinition
                            {
                                Name = "ix_customers_customer_id",
                                Columns = ["customer_id"],
                                Unique = true
                            }
                        ]
                    }
                ]
            }
        };
    }
}