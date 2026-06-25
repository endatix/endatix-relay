using System.Data.Common;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Endatix.Outbox.Engine;

/// <summary>
/// Default <see cref="IOutboxClaimStore"/> — provider-agnostic ADO.NET over a host-supplied
/// <see cref="IOutboxConnectionFactory"/>. Owns the relay's entire database surface (claim + mark/reschedule/fail)
/// so both the in-process API host and the standalone worker share one implementation against the same table;
/// the host contributes only the connection, dialect, and table name. Uses single-statement claims
/// (skip-locked + lease) — no explicit transaction needed — and provider connection pooling, so a fresh
/// connection per call is cheap.
/// </summary>
public sealed class SqlOutboxClaimStore : IOutboxClaimStore
{
    private readonly IOutboxConnectionFactory _connectionFactory;
    private readonly OutboxSqlBuilder _sql;
    private readonly ILogger<SqlOutboxClaimStore> _logger;

    /// <summary>Creates the claim store for the configured dialect/table.</summary>
    public SqlOutboxClaimStore(
        IOutboxConnectionFactory connectionFactory,
        IOptions<OutboxSqlOptions> options,
        ILogger<SqlOutboxClaimStore> logger)
    {
        _connectionFactory = connectionFactory;
        _logger = logger;
        var opts = options.Value;
        _sql = new OutboxSqlBuilder(opts.Dialect, opts.TableName);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<IOutboxMessage>> ClaimBatchAsync(
        string instanceId, TimeSpan lease, int batchSize, CancellationToken cancellationToken)
    {
        var now = DateTime.UtcNow;
        await using var connection = _connectionFactory.Create();
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = _sql.ClaimSql;
        AddParameter(command, "@now", now);
        AddParameter(command, "@batchSize", batchSize);
        AddParameter(command, "@lockedBy", instanceId);
        AddParameter(command, "@lockedUntil", now + lease);

        var rows = new List<IOutboxMessage>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(ReadRow(reader));
        }

        return rows;
    }

    /// <inheritdoc />
    public async Task MarkSentAsync(IOutboxMessage message, string instanceId, CancellationToken cancellationToken)
    {
        var affected = await ExecuteAsync(_sql.MarkSentSql, cancellationToken,
            ("@id", message.Id), ("@lockedBy", instanceId), ("@now", DateTime.UtcNow));
        WarnIfLeaseLost(affected, message, instanceId, "MarkSent");
    }

    /// <inheritdoc />
    public async Task RescheduleAsync(IOutboxMessage message, DateTime nextAttemptAt, string instanceId, CancellationToken cancellationToken)
    {
        var affected = await ExecuteAsync(_sql.RescheduleSql, cancellationToken,
            ("@id", message.Id), ("@lockedBy", instanceId), ("@nextAttemptAt", nextAttemptAt));
        WarnIfLeaseLost(affected, message, instanceId, "Reschedule");
    }

    /// <inheritdoc />
    public async Task MarkFailedAsync(IOutboxMessage message, string instanceId, CancellationToken cancellationToken)
    {
        var affected = await ExecuteAsync(_sql.MarkFailedSql, cancellationToken,
            ("@id", message.Id), ("@lockedBy", instanceId), ("@now", DateTime.UtcNow));
        WarnIfLeaseLost(affected, message, instanceId, "MarkFailed");
    }

    private async Task<int> ExecuteAsync(string sql, CancellationToken cancellationToken, params (string Name, object Value)[] parameters)
    {
        await using var connection = _connectionFactory.Create();
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var (name, value) in parameters)
        {
            AddParameter(command, name, value);
        }

        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    // A 0-row update means the lease was lost (row re-claimed by another instance, or already terminal).
    // At-least-once still holds — surface it for observability instead of failing silently.
    private void WarnIfLeaseLost(int affected, IOutboxMessage message, string instanceId, string operation)
    {
        if (affected == 0)
        {
            _logger.LogWarning(
                "Outbox {Operation} for message {MessageId} affected 0 rows — lease no longer held by {InstanceId} (re-claimed or already terminal).",
                operation, message.Id, instanceId);
        }
    }

    private static void AddParameter(DbCommand command, string name, object value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value is DateTime dt ? ToUtc(dt) : value;
        command.Parameters.Add(parameter);
    }

    // All outbox timestamps are persisted as UTC (PostgreSQL `timestamptz`, SQL Server `datetime2`).
    // Normalize every DateTime parameter to Kind=Utc at the boundary so a Local/Unspecified value from a
    // caller can't (a) throw under Npgsql's strict timestamptz mode or (b) be compared in a different frame
    // than the always-UTC `@now`. Unspecified is assumed to already be UTC (the engine's own values are).
    internal static DateTime ToUtc(DateTime value) => value.Kind switch
    {
        DateTimeKind.Utc => value,
        DateTimeKind.Local => value.ToUniversalTime(),
        _ => DateTime.SpecifyKind(value, DateTimeKind.Utc),
    };

    private static OutboxMessageRow ReadRow(DbDataReader reader) => new()
    {
        Id = reader.GetInt64(0),
        EventType = reader.GetString(1),
        Payload = reader.GetString(2),
        TenantId = reader.GetInt64(3),
        OccurredAt = DateTime.SpecifyKind(reader.GetDateTime(4), DateTimeKind.Utc),
        SchemaVersion = reader.GetInt32(5),
        Attempts = reader.GetInt32(6),
        TraceId = reader.IsDBNull(7) ? null : reader.GetString(7),
    };
}
