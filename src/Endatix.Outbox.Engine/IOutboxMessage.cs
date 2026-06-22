namespace Endatix.Outbox.Engine;

/// <summary>
/// The read contract the relay loop and publishers operate on. Kept deliberately minimal and
/// storage-agnostic so the engine never references a concrete persistence model — the host's outbox
/// row type (e.g. Endatix's <c>OutboxMessage</c> entity) implements this.
/// </summary>
public interface IOutboxMessage
{
    /// <summary>Stable, monotonic identifier of the outbox row. Used as the cross-process dedup key.</summary>
    long Id { get; }

    /// <summary>Stable, broker-facing contract name (also the published topic), e.g. <c>"form.created"</c>.</summary>
    string EventType { get; }

    /// <summary>Serialized event payload (opaque JSON to the engine).</summary>
    string Payload { get; }

    /// <summary>Owning tenant, carried on the row so consumers can re-establish tenant context.</summary>
    long TenantId { get; }

    /// <summary>When the originating business event occurred (UTC).</summary>
    DateTime OccurredAt { get; }

    /// <summary>Version of the payload/contract shape.</summary>
    int SchemaVersion { get; }

    /// <summary>Number of delivery attempts made so far (before the current one).</summary>
    int Attempts { get; }

    /// <summary>Originating distributed-trace id, if any, for cross-process correlation.</summary>
    string? TraceId { get; }
}
