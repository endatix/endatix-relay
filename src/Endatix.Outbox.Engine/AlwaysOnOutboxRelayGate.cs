namespace Endatix.Outbox.Engine;

/// <summary>
/// An <see cref="IOutboxRelayGate"/> that always permits the relay — it ignores any feature flag. Use it in a
/// dedicated relay host (e.g. the standalone worker) whose entire purpose is to relay: gating it on the
/// switchable <see cref="OutboxFlags.RelayInProcess"/> flag would defeat that. With this gate the host needs
/// no OpenFeature provider at all.
/// </summary>
public sealed class AlwaysOnOutboxRelayGate : IOutboxRelayGate
{
    /// <inheritdoc />
    public Task<bool> IsRelayEnabledAsync(CancellationToken cancellationToken) => Task.FromResult(true);
}
