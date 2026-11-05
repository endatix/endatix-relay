using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace Endatix.Outbox.Engine.IntegrationTests;

/// <summary>
/// Per-test SQL Server outbox table + a real <see cref="SqlOutboxClaimStore"/> over it. Column types mirror
/// endatix's SQL Server mapping of the outbox: <c>datetime2</c> timestamps, <c>nvarchar</c> text.
/// </summary>
internal sealed class SqlServerOutboxTestStore : IAsyncDisposable
{
    private readonly string _connectionString;

    public string Table { get; } = "outbox_" + Guid.NewGuid().ToString("N");

    public SqlOutboxClaimStore ClaimStore { get; }

    public SqlServerOutboxTestStore(string connectionString, TimeProvider clock)
    {
        _connectionString = connectionString;
        ClaimStore = new SqlOutboxClaimStore(
            new DelegateOutboxConnectionFactory(() => new SqlConnection(_connectionString)),
            Options.Create(new OutboxSqlOptions { Dialect = OutboxSqlDialect.SqlServer, TableName = Table }),
            NullLogger<SqlOutboxClaimStore>.Instance,
            clock);
    }

    public async Task CreateTableAsync()
    {
        var sql = $"""
            CREATE TABLE [{Table}] (
                [{OutboxSchema.Id}]            bigint         NOT NULL PRIMARY KEY,
                [{OutboxSchema.EventType}]     nvarchar(128)  NOT NULL,
                [{OutboxSchema.Payload}]       nvarchar(max)  NOT NULL,
                [{OutboxSchema.TenantId}]      bigint         NOT NULL,
                [{OutboxSchema.OccurredAt}]    datetime2      NOT NULL,
                [{OutboxSchema.SchemaVersion}] int            NOT NULL,
                [{OutboxSchema.Status}]        int            NOT NULL,
                [{OutboxSchema.Attempts}]      int            NOT NULL,
                [{OutboxSchema.TraceId}]       nvarchar(128)  NULL,
                [{OutboxSchema.LockedUntil}]   datetime2      NULL,
                [{OutboxSchema.LockedBy}]      nvarchar(128)  NULL,
                [{OutboxSchema.NextAttemptAt}] datetime2      NULL,
                [{OutboxSchema.ProcessedAt}]   datetime2      NULL,
                [{OutboxSchema.FailureReason}] nvarchar(2000) NULL
            );
            """;
        await ExecuteAsync(sql);
    }

    public async Task InsertAsync(long id)
    {
        var sql = $"""
            INSERT INTO [{Table}] (
                [{OutboxSchema.Id}], [{OutboxSchema.EventType}], [{OutboxSchema.Payload}], [{OutboxSchema.TenantId}],
                [{OutboxSchema.OccurredAt}], [{OutboxSchema.SchemaVersion}], [{OutboxSchema.Status}], [{OutboxSchema.Attempts}])
            VALUES (@id, 'form.created', '{"{}"}', 1, SYSUTCDATETIME(), 1, {(int)OutboxStatus.Pending}, 0);
            """;

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@id", id);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<FailureState> GetFailureAsync(long id)
    {
        var sql = $"""
            SELECT [{OutboxSchema.Status}], [{OutboxSchema.Attempts}], [{OutboxSchema.FailureReason}]
            FROM [{Table}] WHERE [{OutboxSchema.Id}] = @id;
            """;

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@id", id);
        await using var reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync(), $"Row {id} not found in {Table}.");

        return new FailureState(
            (OutboxStatus)reader.GetInt32(0),
            reader.GetInt32(1),
            reader.IsDBNull(2) ? null : reader.GetString(2));
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await ExecuteAsync($"DROP TABLE IF EXISTS [{Table}];");
        }
        catch (Exception ex) when (ex is SqlException or InvalidOperationException)
        {
            // Best-effort cleanup: the container is torn down with the collection anyway. Log instead of
            // throwing, so a cleanup failure (e.g. a pool timeout) can't replace the test's own failure.
            Console.Error.WriteLine($"Dropping outbox test table {Table} failed: {ex.Message}");
        }
    }

    private async Task ExecuteAsync(string sql)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    internal sealed record FailureState(OutboxStatus Status, int Attempts, string? FailureReason);
}
