using System.Reflection;
using System.Text.Json;
using CouchSql.Core.Design;
using CouchSql.Infrastructure.Sync;

namespace CouchSql.Tests.Unit;

public sealed class SchemaReconcilerTests
{
    [Fact]
    public void BuildShadowDesign_Uses_Shadow_Table_Names_And_Unique_Index_Names()
    {
        var method = typeof(SchemaReconciler).GetMethod("BuildShadowDesign", BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(method);

        var design = new CouchSqlDesignDocument
        {
            Id = "_design/couchsql",
            Revision = "4-abcdef",
            CouchSql = new CouchSqlDesignConfiguration
            {
                SchemaVersion = 1,
                Types =
                [
                    new CouchSqlTypeDefinition
                    {
                        Name = "components",
                        Table = "components",
                        Identify = JsonDocument.Parse("{\"path\":\"_id\",\"contains\":\"component\"}").RootElement.Clone(),
                        Fields =
                        [
                            new CouchSqlFieldDefinition { Column = "name", Path = "Name", Type = "text", Required = false }
                        ],
                        Indexes =
                        [
                            new CouchSqlIndexDefinition { Name = "ix_components_name", Columns = ["name"], Unique = false }
                        ]
                    },
                    new CouchSqlTypeDefinition
                    {
                        Name = "customers",
                        Table = "customers",
                        Identify = JsonDocument.Parse("{\"path\":\"kind\",\"equals\":\"customer\"}").RootElement.Clone(),
                        Fields =
                        [
                            new CouchSqlFieldDefinition { Column = "display_name", Path = "DisplayName", Type = "text", Required = false }
                        ],
                        Indexes = []
                    }
                ]
            }
        };

        var shadowTableMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["components"] = "components_shadow"
        };

        var result = (CouchSqlDesignDocument)method!.Invoke(null, [design, shadowTableMap, "4-abcdef"] )!;
        var type = Assert.Single(result.CouchSql!.Types);
        var index = Assert.Single(type.Indexes);

        Assert.Equal("components_shadow", type.Table);
        Assert.StartsWith("ix_components_name__sh_", index.Name, StringComparison.Ordinal);
        Assert.NotEqual("ix_components_name", index.Name);
    }

    [Fact]
    public void CreateReconcilePlan_Only_Rebuilds_Changed_Or_New_Types()
    {
        var method = typeof(SchemaReconciler).GetMethod("CreateReconcilePlan", BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(method);

        var previousTypes = new[]
        {
            new CouchSqlTypeDefinition
            {
                Name = "components",
                Table = "components",
                Identify = JsonDocument.Parse("{\"path\":\"_id\",\"contains\":\"component\"}").RootElement.Clone(),
                Fields =
                [
                    new CouchSqlFieldDefinition { Column = "name", Path = "Name", Type = "text", Required = false }
                ],
                Indexes =
                [
                    new CouchSqlIndexDefinition { Name = "ix_components_name", Columns = ["name"], Unique = false }
                ]
            }
        };

        var nextTypes = new[]
        {
            new CouchSqlTypeDefinition
            {
                Name = "components",
                Table = "components",
                Identify = JsonDocument.Parse("{\"path\":\"_id\",\"contains\":\"component\"}").RootElement.Clone(),
                Fields =
                [
                    new CouchSqlFieldDefinition { Column = "name", Path = "Name", Type = "text", Required = false }
                ],
                Indexes =
                [
                    new CouchSqlIndexDefinition { Name = "ix_components_name", Columns = ["name"], Unique = false }
                ]
            },
            new CouchSqlTypeDefinition
            {
                Name = "customers",
                Table = "customers",
                Identify = JsonDocument.Parse("{\"path\":\"kind\",\"equals\":\"customer\"}").RootElement.Clone(),
                Fields =
                [
                    new CouchSqlFieldDefinition { Column = "display_name", Path = "DisplayName", Type = "text", Required = false }
                ],
                Indexes = []
            }
        };

        var plan = method!.Invoke(null, [previousTypes, nextTypes]);
        Assert.NotNull(plan);

        var planType = plan!.GetType();
        var unchangedPlans = (System.Collections.IEnumerable)planType.GetProperty("UnchangedPlans")!.GetValue(plan)!;
        var rebuildPlans = (System.Collections.IEnumerable)planType.GetProperty("RebuildPlans")!.GetValue(plan)!;

        Assert.Single(unchangedPlans.Cast<object>());
        Assert.Single(rebuildPlans.Cast<object>());

        var rebuildPlan = rebuildPlans.Cast<object>().Single();
        var rebuildTypeName = (string)rebuildPlan.GetType().GetProperty("TypeName")!.GetValue(rebuildPlan)!;
        Assert.Equal("customers", rebuildTypeName);
    }

    [Fact]
    public void CreateReconcilePlan_Treats_Field_Transform_Change_As_Rebuild_Required()
    {
        var method = typeof(SchemaReconciler).GetMethod("CreateReconcilePlan", BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(method);

        var previousTypes = new[]
        {
            new CouchSqlTypeDefinition
            {
                Name = "components",
                Table = "components",
                Identify = JsonDocument.Parse("{\"path\":\"_id\",\"contains\":\"component\"}").RootElement.Clone(),
                Fields =
                [
                    new CouchSqlFieldDefinition { Column = "name", Path = "Name", Type = "text", Required = false }
                ],
                Indexes = []
            }
        };

        var nextTypes = new[]
        {
            new CouchSqlTypeDefinition
            {
                Name = "components",
                Table = "components",
                Identify = JsonDocument.Parse("{\"path\":\"_id\",\"contains\":\"component\"}").RootElement.Clone(),
                Fields =
                [
                    new CouchSqlFieldDefinition
                    {
                        Column = "name",
                        Path = "Name",
                        Type = "text",
                        Required = false,
                        Transform = new CouchSqlFieldTransformDefinition
                        {
                            Prefix = "Component: "
                        }
                    }
                ],
                Indexes = []
            }
        };

        var plan = method!.Invoke(null, [previousTypes, nextTypes]);
        Assert.NotNull(plan);

        var rebuildPlans = (System.Collections.IEnumerable)plan!.GetType().GetProperty("RebuildPlans")!.GetValue(plan)!;
        var rebuildPlan = Assert.Single(rebuildPlans.Cast<object>());
        var rebuildTypeName = (string)rebuildPlan.GetType().GetProperty("TypeName")!.GetValue(rebuildPlan)!;

        Assert.Equal("components", rebuildTypeName);
    }
}