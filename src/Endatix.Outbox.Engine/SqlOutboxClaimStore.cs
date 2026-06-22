using System.Data.Common;
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

    /// <summary>Creates the claim store for the configured dialect/table.</summary>
    public SqlOutboxClaimStore(IOutboxConnectionFactory connectionFactory, IOptions<OutboxSqlOptions> options)
    {
        _connectionFactory = connectionFactory;
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
    public Task MarkSentAsync(IOutboxMessage message, CancellationToken cancellationToken) =>
        ExecuteAsync(_sql.MarkSentSql, cancellationToken,
            ("@id", message.Id), ("@now", DateTime.UtcNow));

    /// <inheritdoc />
    public Task RescheduleAsync(IOutboxMessage message, DateTime nextAttemptAt, CancellationToken cancellationToken) =>
        ExecuteAsync(_sql.RescheduleSql, cancellationToken,
            ("@id", message.Id), ("@nextAttemptAt", nextAttemptAt));

    /// <inheritdoc />
    public Task MarkFailedAsync(IOutboxMessage message, CancellationToken cancellationToken) =>
        ExecuteAsync(_sql.MarkFailedSql, cancellationToken,
            ("@id", message.Id), ("@now", DateTime.UtcNow));

    private async Task ExecuteAsync(string sql, CancellationToken cancellationToken, params (string Name, object Value)[] parameters)
    {
        await using var connection = _connectionFactory.Create();
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var (name, value) in parameters)
        {
            AddParameter(command, name, value);
        }

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static void AddParameter(DbCommand command, string name, object value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }

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
