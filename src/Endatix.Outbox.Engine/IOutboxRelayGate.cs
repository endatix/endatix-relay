namespace Endatix.Outbox.Engine;

/// <summary>
/// The on/off switch for the in-process relay, evaluated once per poll tick. The default implementation
/// (<see cref="OpenFeatureOutboxRelayGate"/>) reads the <see cref="OutboxFlags.RelayInProcess"/> feature
/// flag, so the relay can be paused or handed over to a standalone worker by flipping a flag — no restart
/// needed when backed by a dynamic provider.
/// </summary>
public interface IOutboxRelayGate
{
    /// <summary>Returns whether the in-process relay should claim and publish this tick.</summary>
    Task<bool> IsRelayEnabledAsync(CancellationToken cancellationToken);
}
