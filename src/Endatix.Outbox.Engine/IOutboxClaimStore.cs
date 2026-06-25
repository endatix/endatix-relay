namespace Endatix.Outbox.Engine;

/// <summary>
/// Persistence seam for the relay. Implementations live in the host (they own the EF model and the
/// provider-specific claim SQL). The claim uses a DB-arbitrated skip-locked update + a lease so any
/// number of relay instances can run without double-claiming a row.
/// </summary>
public interface IOutboxClaimStore
{
    /// <summary>
    /// Atomically claims up to <paramref name="batchSize"/> pending rows for this instance, setting a
    /// lease that expires after <paramref name="lease"/>. Returns the claimed rows (or empty).
    /// </summary>
    Task<IReadOnlyList<IOutboxMessage>> ClaimBatchAsync(
        string instanceId, TimeSpan lease, int batchSize, CancellationToken cancellationToken);

    /// <summary>
    /// Marks a successfully-published row as sent and releases its lease. The update is guarded by lease
    /// ownership (<paramref name="instanceId"/> must still hold the lease); if the lease was lost to another
    /// instance it is a no-op (implementations should surface the 0-row case).
    /// </summary>
    Task MarkSentAsync(IOutboxMessage message, string instanceId, CancellationToken cancellationToken);

    /// <summary>
    /// Returns a failed row to pending for a later retry: increments attempts, sets the next-attempt
    /// gate to <paramref name="nextAttemptAt"/>, and releases the lease. Guarded by lease ownership
    /// (<paramref name="instanceId"/>).
    /// </summary>
    Task RescheduleAsync(IOutboxMessage message, DateTimeOffset nextAttemptAt, string instanceId, CancellationToken cancellationToken);

    /// <summary>
    /// Marks a row as terminally failed (max attempts exhausted) and releases its lease. Guarded by lease
    /// ownership (<paramref name="instanceId"/>).
    /// </summary>
    Task MarkFailedAsync(IOutboxMessage message, string instanceId, CancellationToken cancellationToken);
}
