using System.Text.Json;
using CouchSql.Core.Design;
using CouchSql.Infrastructure.Sync;

namespace CouchSql.Tests.Unit;

public sealed class SyncProjectionCompilerTests
{
    [Fact]
    public void BuildSelector_Generates_Or_Filter_For_Multiple_Types()
    {
        var document = CreateDesignDocument();

        var compiled = SyncProjectionCompiler.Compile(document);
        var selector = SyncProjectionCompiler.BuildSelector(compiled);

      Assert.True(selector.TryGetProperty("$or", out var orElement));
        Assert.Equal(2, orElement.GetArrayLength());
    }

    [Fact]
    public void MatchType_And_ProjectFields_Use_Identify_And_Field_Paths()
    {
        var document = CreateDesignDocument();
        var source = JsonDocument.Parse("""
        {
          "_id": "customer-1",
          "_rev": "2-abc",
          "meta": { "entity": "customer" },
          "customer": {
            "id": "C-100",
            "profile": {
              "addresses": [
                { "city": "Berlin" }
              ]
            }
          }
        }
        """).RootElement;

        var compiled = SyncProjectionCompiler.Compile(document);
        var matched = SyncProjectionCompiler.MatchType(compiled, source);
        var projected = SyncProjectionCompiler.ProjectFields(matched!, source);

        Assert.Equal("customer", matched!.Name);
        Assert.Equal("C-100", projected["customer_id"]);
        Assert.Equal("City: Berlin", projected["city"]);
        Assert.Null(projected["created_on"]);
    }

    [Fact]
    public void ProjectFields_Applies_Prefix_And_Append_For_Text_Values()
    {
        var document = CreateDesignDocument();
        var source = JsonDocument.Parse("""
        {
          "_id": "customer-3",
          "_rev": "4-ghi",
          "meta": { "entity": "customer" },
          "customer": {
            "id": "C-300",
            "nickname": "North"
          }
        }
        """).RootElement;

        var compiled = SyncProjectionCompiler.Compile(document);
        var matched = SyncProjectionCompiler.MatchType(compiled, source);
        var projected = SyncProjectionCompiler.ProjectFields(matched!, source);

        Assert.Equal("[North]", projected["nickname"]);
    }

    [Fact]
    public void ProjectFields_Does_Not_Apply_Text_Transform_To_Null_Value()
    {
        var document = CreateDesignDocument();
        var source = JsonDocument.Parse("""
        {
          "_id": "customer-4",
          "_rev": "5-jkl",
          "meta": { "entity": "customer" },
          "customer": {
            "id": "C-400",
            "nickname": null
          }
        }
        """).RootElement;

        var compiled = SyncProjectionCompiler.Compile(document);
        var matched = SyncProjectionCompiler.MatchType(compiled, source);
        var projected = SyncProjectionCompiler.ProjectFields(matched!, source);

        Assert.Null(projected["nickname"]);
    }

    [Fact]
    public void ProjectFields_Ignores_Text_Transform_For_Non_Text_Types()
    {
        var design = CreateDesignDocument();
        design.CouchSql!.Types[0].Fields.Add(new CouchSqlFieldDefinition
        {
            Column = "score",
            Path = "customer.score",
            Type = "integer",
            Required = false,
            Transform = new CouchSqlFieldTransformDefinition
            {
                Prefix = "ignored-",
                Append = "-ignored"
            }
        });

        var source = JsonDocument.Parse("""
        {
          "_id": "customer-5",
          "_rev": "6-mno",
          "meta": { "entity": "customer" },
          "customer": {
            "id": "C-500",
            "score": 42
          }
        }
        """).RootElement;

        var compiled = SyncProjectionCompiler.Compile(design);
        var matched = SyncProjectionCompiler.MatchType(compiled, source);
        var projected = SyncProjectionCompiler.ProjectFields(matched!, source);

        Assert.Equal(42, projected["score"]);
    }

    [Fact]
    public void ProjectFields_Rejects_Invalid_Type_Conversions()
    {
        var document = CreateDesignDocument();
        var source = JsonDocument.Parse("""
        {
          "_id": "customer-2",
          "_rev": "3-def",
          "meta": { "entity": "customer" },
          "customer": {
            "id": "C-200",
            "profile": {
              "addresses": [
                { "city": 42 }
              ]
            }
          }
        }
        """).RootElement;

        var compiled = SyncProjectionCompiler.Compile(document);
        var matched = SyncProjectionCompiler.MatchType(compiled, source);

        var exception = Assert.Throws<InvalidOperationException>(() => SyncProjectionCompiler.ProjectFields(matched!, source));
        Assert.Contains("could not be converted", exception.Message);
    }

      [Fact]
      public void MatchType_Supports_String_Contains_On_Id()
      {
        var design = new CouchSqlDesignDocument
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
                Name = "component",
                Table = "components",
                Identify = JsonDocument.Parse("{\"path\":\"_id\",\"contains\":\"component\"}").RootElement.Clone(),
                Fields =
                [
                  new CouchSqlFieldDefinition { Column = "component_id", Path = "_id", Type = "text", Required = true }
                ]
              }
            ]
          }
        };

        var source = JsonDocument.Parse("""
        {
          "_id": "abc-component-001",
          "_rev": "1-xyz"
        }
        """).RootElement;

        var compiled = SyncProjectionCompiler.Compile(design);
        var matched = SyncProjectionCompiler.MatchType(compiled, source);
        var selector = SyncProjectionCompiler.BuildSelector(compiled);

        Assert.Equal("component", matched!.Name);
        Assert.Equal("component", selector.GetProperty("_id").GetProperty("$regex").GetString());
      }

    private static CouchSqlDesignDocument CreateDesignDocument()
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
                        Identify = JsonDocument.Parse("""
                        {
                          "all": [
                            { "path": "meta.entity", "equals": "customer" },
                            { "path": "customer.id", "exists": true }
                          ]
                        }
                        """).RootElement.Clone(),
                        Fields =
                        [
                            new CouchSqlFieldDefinition { Column = "customer_id", Path = "customer.id", Type = "text", Required = true },
                            new CouchSqlFieldDefinition
                            {
                              Column = "city",
                              Path = "customer.profile.addresses[0].city",
                              Type = "text",
                              Required = false,
                              Transform = new CouchSqlFieldTransformDefinition
                              {
                                Prefix = "City: "
                              }
                            },
                            new CouchSqlFieldDefinition { Column = "created_on", Path = "customer.createdOn", Type = "date", Required = false },
                            new CouchSqlFieldDefinition
                            {
                              Column = "nickname",
                              Path = "customer.nickname",
                              Type = "text",
                              Required = false,
                              Transform = new CouchSqlFieldTransformDefinition
                              {
                                Prefix = "[",
                                Append = "]"
                              }
                            }
                        ]
                    },
                    new CouchSqlTypeDefinition
                    {
                        Name = "invoice",
                        Table = "invoices",
                        Identify = JsonDocument.Parse("""
                        {
                          "all": [
                            { "path": "meta.entity", "equals": "invoice" },
                            { "path": "invoice.id", "exists": true }
                          ]
                        }
                        """).RootElement.Clone(),
                        Fields =
                        [
                            new CouchSqlFieldDefinition { Column = "invoice_id", Path = "invoice.id", Type = "text", Required = true }
                        ]
                    }
                ]
            }
        };
    }
}