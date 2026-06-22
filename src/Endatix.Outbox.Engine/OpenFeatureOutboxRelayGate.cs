using OpenFeature;

namespace Endatix.Outbox.Engine;

/// <summary>
/// Default <see cref="IOutboxRelayGate"/> backed by OpenFeature. Evaluates
/// <see cref="OutboxFlags.RelayInProcess"/> (default <c>true</c>) via the host-configured provider, so the
/// switch is vendor-neutral: in-memory/config today, flagd or a SaaS later with no code change.
/// </summary>
public sealed class OpenFeatureOutboxRelayGate(IFeatureClient featureClient) : IOutboxRelayGate
{
    private readonly IFeatureClient _featureClient = featureClient;

    /// <inheritdoc />
    public Task<bool> IsRelayEnabledAsync(CancellationToken cancellationToken) =>
        _featureClient.GetBooleanValueAsync(OutboxFlags.RelayInProcess, true, null, null, cancellationToken);
}
