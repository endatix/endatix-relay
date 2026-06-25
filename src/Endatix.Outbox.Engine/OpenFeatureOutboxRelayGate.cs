using Microsoft.Extensions.Logging;
using OpenFeature;

namespace Endatix.Outbox.Engine;

/// <summary>
/// Default <see cref="IOutboxRelayGate"/> backed by OpenFeature. Evaluates
/// <see cref="OutboxFlags.RelayInProcess"/> (default <c>true</c>) via the host-configured provider, so the
/// switch is vendor-neutral: in-memory/config today, flagd or a SaaS later with no code change.
/// </summary>
/// <remarks>
/// If the host registered the relay but no OpenFeature <c>IFeatureClient</c> (forgot the provider), the gate
/// is constructed with a null client and fails <b>open</b> — it returns the documented default <c>true</c>
/// (relay runs) and logs a one-time warning. This is deliberate: silently failing closed would leave the
/// relay never delivering, contradicting the "default on" contract.
/// </remarks>
public sealed class OpenFeatureOutboxRelayGate(
    ILogger<OpenFeatureOutboxRelayGate> logger, IFeatureClient? featureClient = null) : IOutboxRelayGate
{
    private static int _warnedNoClient;

    /// <inheritdoc />
    public Task<bool> IsRelayEnabledAsync(CancellationToken cancellationToken)
    {
        if (featureClient is null)
        {
            WarnOnceNoClient();
            return Task.FromResult(true); // documented default — fail OPEN, don't silently never run
        }

        return featureClient.GetBooleanValueAsync(
            OutboxFlags.RelayInProcess, true, null, null, cancellationToken);
    }

    private void WarnOnceNoClient()
    {
        if (Interlocked.Exchange(ref _warnedNoClient, 1) == 0)
        {
            logger.LogWarning(
                "No OpenFeature IFeatureClient is registered; the outbox relay '{Flag}' gate defaults to ON. " +
                "Register an OpenFeature provider (e.g. AddEndatixOpenFeature) to control the relay switch.",
                OutboxFlags.RelayInProcess);
        }
    }
}
