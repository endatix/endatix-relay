namespace Endatix.Outbox.Engine;

/// <summary>
/// The <b>canonical storage contract</b> for the outbox table — the single source of truth for the table's
/// column names, shared by the engine's claim SQL and by the host's EF mapping. The host's
/// <c>IEntityTypeConfiguration&lt;OutboxMessage&gt;</c> should map each column with these constants
/// (<c>HasColumnName(OutboxSchema.Status)</c>, …) so a rename is a single compile-time edit on both sides
/// instead of a silent runtime mismatch. The physical <b>table name</b> is supplied by the host
/// (see <see cref="OutboxSqlOptions.TableName"/>) because hosts may prefix/qualify it; it defaults to
/// <see cref="DefaultTable"/>.
/// </summary>
public static class OutboxSchema
{
    /// <summary>Default unqualified table name. Hosts may override (prefix/schema) via <see cref="OutboxSqlOptions.TableName"/>.</summary>
    public const string DefaultTable = "OutboxMessages";

#pragma warning disable CS1591 // Column-name constants are self-documenting; they mirror the outbox row's fields.
    public const string Id = nameof(Id);
    public const string EventType = nameof(EventType);
    public const string Payload = nameof(Payload);
    public const string TenantId = nameof(TenantId);
    public const string OccurredAt = nameof(OccurredAt);
    public const string SchemaVersion = nameof(SchemaVersion);
    public const string Status = nameof(Status);
    public const string Attempts = nameof(Attempts);
    public const string TraceId = nameof(TraceId);
    public const string LockedUntil = nameof(LockedUntil);
    public const string LockedBy = nameof(LockedBy);
    public const string NextAttemptAt = nameof(NextAttemptAt);
    public const string ProcessedAt = nameof(ProcessedAt);
#pragma warning restore CS1591
}

/// <summary>
/// Persisted <see cref="IOutboxMessage"/>-row lifecycle states, stored as <c>int</c>. The engine's claim SQL
/// filters on these literal values, so the host's status mapping <b>must</b> use the same integers
/// (verify with a conformance test).
/// </summary>
public enum OutboxStatus
{
    /// <summary>Captured, awaiting delivery.</summary>
    Pending = 0,

    /// <summary>Delivered successfully.</summary>
    Sent = 1,

    /// <summary>Max attempts exhausted; needs operator attention.</summary>
    Failed = 2,
}
