using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;

namespace Endatix.Outbox.Engine.IntegrationTests;

/// <summary>
/// Per-test PostgreSQL outbox table + a real <see cref="SqlOutboxClaimStore"/> over it. The table is created
/// with a unique name (isolation) and dropped on dispose. Column NAMES come from <see cref="OutboxSchema"/>
/// so a rename can't silently drift; the TYPES mirror endatix's merged migration
/// (AddOutboxMessageAndAggregateRevision): timestamptz / jsonb / bigint / integer / varchar(128).
/// </summary>
internal sealed class OutboxTestStore : IAsyncDisposable
{
    private readonly string _connectionString;

    public string Table { get; } = "outbox_" + Guid.NewGuid().ToString("N");

    public SqlOutboxClaimStore ClaimStore { get; }

    public OutboxTestStore(string connectionString)
    {
        _connectionString = connectionString;
        ClaimStore = new SqlOutboxClaimStore(
            new DelegateOutboxConnectionFactory(() => new NpgsqlConnection(_connectionString)),
            Options.Create(new OutboxSqlOptions { Dialect = OutboxSqlDialect.PostgreSql, TableName = Table }),
            NullLogger<SqlOutboxClaimStore>.Instance);
    }

    public async Task CreateTableAsync()
    {
        var sql = $"""
            CREATE TABLE "{Table}" (
                "{OutboxSchema.Id}"            bigint        NOT NULL PRIMARY KEY,
                "{OutboxSchema.EventType}"     varchar(128)  NOT NULL,
                "{OutboxSchema.Payload}"       jsonb         NOT NULL,
                "{OutboxSchema.TenantId}"      bigint        NOT NULL,
                "{OutboxSchema.OccurredAt}"    timestamptz   NOT NULL,
                "{OutboxSchema.SchemaVersion}" integer       NOT NULL,
                "{OutboxSchema.Status}"        integer       NOT NULL,
                "{OutboxSchema.Attempts}"      integer       NOT NULL,
                "{OutboxSchema.TraceId}"       varchar(128)  NULL,
                "{OutboxSchema.LockedUntil}"   timestamptz   NULL,
                "{OutboxSchema.LockedBy}"      varchar(128)  NULL,
                "{OutboxSchema.NextAttemptAt}" timestamptz   NULL,
                "{OutboxSchema.ProcessedAt}"   timestamptz   NULL
            );
            """;
        await ExecuteAsync(sql);
    }

    public async Task InsertAsync(
        long id,
        OutboxStatus status = OutboxStatus.Pending,
        string payload = "{}",
        int attempts = 0,
        DateTime? occurredAt = null,
        DateTime? lockedUntil = null,
        string? lockedBy = null,
        DateTime? nextAttemptAt = null,
        string eventType = "form.created",
        long tenantId = 1)
    {
        var sql = $"""
            INSERT INTO "{Table}" (
                "{OutboxSchema.Id}", "{OutboxSchema.EventType}", "{OutboxSchema.Payload}", "{OutboxSchema.TenantId}",
                "{OutboxSchema.OccurredAt}", "{OutboxSchema.SchemaVersion}", "{OutboxSchema.Status}", "{OutboxSchema.Attempts}",
                "{OutboxSchema.TraceId}", "{OutboxSchema.LockedUntil}", "{OutboxSchema.LockedBy}", "{OutboxSchema.NextAttemptAt}",
                "{OutboxSchema.ProcessedAt}")
            VALUES (@id, @eventType, @payload::jsonb, @tenantId, @occurredAt, 1, @status, @attempts,
                NULL, @lockedUntil, @lockedBy, @nextAttemptAt, NULL);
            """;

        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("@id", id);
        command.Parameters.AddWithValue("@eventType", eventType);
        command.Parameters.AddWithValue("@payload", payload);
        command.Parameters.AddWithValue("@tenantId", tenantId);
        command.Parameters.AddWithValue("@occurredAt", DateTime.SpecifyKind(occurredAt ?? DateTime.UtcNow, DateTimeKind.Utc));
        command.Parameters.AddWithValue("@status", (int)status);
        command.Parameters.AddWithValue("@attempts", attempts);
        command.Parameters.AddWithValue("@lockedUntil", (object?)Utc(lockedUntil) ?? DBNull.Value);
        command.Parameters.AddWithValue("@lockedBy", (object?)lockedBy ?? DBNull.Value);
        command.Parameters.AddWithValue("@nextAttemptAt", (object?)Utc(nextAttemptAt) ?? DBNull.Value);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<RowState> GetAsync(long id)
    {
        var sql = $"""
            SELECT "{OutboxSchema.Status}", "{OutboxSchema.Attempts}", "{OutboxSchema.LockedBy}",
                   "{OutboxSchema.LockedUntil}", "{OutboxSchema.NextAttemptAt}", "{OutboxSchema.ProcessedAt}",
                   "{OutboxSchema.Payload}"::text, "{OutboxSchema.OccurredAt}"
            FROM "{Table}" WHERE "{OutboxSchema.Id}" = @id;
            """;

        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("@id", id);
        await using var reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync(), $"Row {id} not found in {Table}.");

        return new RowState(
            Status: (OutboxStatus)reader.GetInt32(0),
            Attempts: reader.GetInt32(1),
            LockedBy: reader.IsDBNull(2) ? null : reader.GetString(2),
            LockedUntil: reader.IsDBNull(3) ? null : reader.GetDateTime(3),
            NextAttemptAt: reader.IsDBNull(4) ? null : reader.GetDateTime(4),
            ProcessedAt: reader.IsDBNull(5) ? null : reader.GetDateTime(5),
            Payload: reader.GetString(6),
            OccurredAt: reader.GetDateTime(7));
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await ExecuteAsync($"DROP TABLE IF EXISTS \"{Table}\";");
        }
        catch
        {
            // best-effort cleanup; the container is torn down with the collection anyway
        }
    }

    private async Task ExecuteAsync(string sql)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    // Npgsql binds DateTime to timestamptz only when Kind=Utc.
    private static DateTime? Utc(DateTime? value) =>
        value is null ? null : DateTime.SpecifyKind(value.Value, DateTimeKind.Utc);

    internal sealed record RowState(
        OutboxStatus Status,
        int Attempts,
        string? LockedBy,
        DateTime? LockedUntil,
        DateTime? NextAttemptAt,
        DateTime? ProcessedAt,
        string Payload,
        DateTime OccurredAt);
}
