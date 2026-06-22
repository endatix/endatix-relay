namespace Endatix.Outbox.Engine;

/// <summary>Materialized claim-store row — the engine's own <see cref="IOutboxMessage"/> carrier.</summary>
internal sealed class OutboxMessageRow : IOutboxMessage
{
    public long Id { get; init; }
    public string EventType { get; init; } = string.Empty;
    public string Payload { get; init; } = string.Empty;
    public long TenantId { get; init; }
    public DateTime OccurredAt { get; init; }
    public int SchemaVersion { get; init; }
    public int Attempts { get; init; }
    public string? TraceId { get; init; }
}
