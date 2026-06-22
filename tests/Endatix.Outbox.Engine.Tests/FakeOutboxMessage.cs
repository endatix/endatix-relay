using Endatix.Outbox.Engine;

namespace Endatix.Outbox.Engine.Tests;

/// <summary>Minimal <see cref="IOutboxMessage"/> for tests.</summary>
internal sealed record FakeOutboxMessage(
    long Id,
    string EventType = "form.created",
    string Payload = "{}",
    long TenantId = 1,
    int SchemaVersion = 1,
    int Attempts = 0,
    string? TraceId = null) : IOutboxMessage
{
    public DateTime OccurredAt { get; init; } = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
}
