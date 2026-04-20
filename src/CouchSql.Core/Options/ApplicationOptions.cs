using System.ComponentModel.DataAnnotations;

namespace CouchSql.Core.Options;

public sealed class PostgreSqlOptions
{
    [Required]
    public string Host { get; set; } = "localhost";

    [Range(1, 65535)]
    public int Port { get; set; } = 5432;

    [Required]
    public string SystemDatabase { get; set; } = "postgres";

    [Required]
    public string AdminDatabase { get; set; } = "couchsql_admin";

    [Required]
    public string Username { get; set; } = string.Empty;

    [Required]
    public string Password { get; set; } = string.Empty;
}

public sealed class EndpointOptions
{
    [Required]
    public string Public { get; set; } = "http://0.0.0.0:8080";

    [Required]
    public string Admin { get; set; } = "http://127.0.0.1:8081";
}

public sealed class QueryOptions
{
    [Range(1, 1000000)]
    public int DefaultRowLimit { get; set; } = 1000;

    [Range(1, 1000000)]
    public int MaxRowLimit { get; set; } = 10000;

    [Range(1, 600)]
    public int CommandTimeoutSeconds { get; set; } = 30;
}

public sealed class SyncOptions
{
    [Range(1, 1000000)]
    public int SnapshotBatchSize { get; set; } = 1000;

    [Range(1, 1000000)]
    public int SnapshotSeqInterval { get; set; } = 1000;

    [Range(1000, 600000)]
    public int LongpollHeartbeatMilliseconds { get; set; } = 60000;

    [Range(1, 600)]
    public int LongpollReadTimeoutSeconds { get; set; } = 90;
}

public sealed class SecurityOptions
{
    [Required]
    public string EncryptionKeyPath { get; set; } = "./keys/couchsql.key";
}