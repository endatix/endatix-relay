namespace Endatix.Outbox.Engine.IntegrationTests;

/// <summary>A real claim store whose <see cref="MarkFailedAsync"/> throws, as when the database drops mid-tick.</summary>
internal sealed class MarkFailedThrowsClaimStore(IOutboxClaimStore inner) : IOutboxClaimStore
{
    public Task<IReadOnlyList<IOutboxMessage>> ClaimBatchAsync(
        string instanceId, TimeSpan lease, int batchSize, CancellationToken cancellationToken) =>
        inner.ClaimBatchAsync(instanceId, lease, batchSize, cancellationToken);

    public Task MarkSentAsync(IOutboxMessage message, string instanceId, CancellationToken cancellationToken) =>
        inner.MarkSentAsync(message, instanceId, cancellationToken);

    public Task RescheduleAsync(
        IOutboxMessage message, DateTimeOffset nextAttemptAt, string instanceId, CancellationToken cancellationToken) =>
        inner.RescheduleAsync(message, nextAttemptAt, instanceId, cancellationToken);

    public Task MarkFailedAsync(IOutboxMessage message, string instanceId, CancellationToken cancellationToken) =>
        Task.FromException(new InvalidOperationException("store down"));
}
