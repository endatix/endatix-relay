using Endatix.Outbox.Engine;
using Microsoft.Extensions.Logging.Abstractions;
using OpenFeature;
using OpenFeature.Providers.Memory;

namespace Endatix.Outbox.Engine.Tests;

/// <summary>
/// Verifies the default gate consults the OpenFeature provider (not just the hardcoded default), and that it
/// fails OPEN (defaults to on) when no client is available. Each test binds its own OpenFeature <b>domain</b>
/// and resolves a client for that domain, so the global <c>Api.Instance</c> default provider is never mutated
/// — no shared state leaks between tests (the old shared-default-provider approach needed a serialized
/// collection; domain isolation removes that).
/// </summary>
public class OpenFeatureOutboxRelayGateTests
{
    private static Dictionary<string, Flag> Flags(string defaultVariant) => new()
    {
        [OutboxFlags.RelayInProcess] = new Flag<bool>(
            new Dictionary<string, bool> { ["on"] = true, ["off"] = false }, defaultVariant),
    };

    private static async Task<IFeatureClient> ClientForAsync(string domain, string defaultVariant)
    {
        await Api.Instance.SetProviderAsync(domain, new InMemoryProvider(Flags(defaultVariant)));
        return Api.Instance.GetClient(domain);
    }

    [Fact]
    public async Task Reads_false_from_the_provider_overriding_the_true_default()
    {
        var client = await ClientForAsync("gate-reads-false", "off");
        var gate = new OpenFeatureOutboxRelayGate(NullLogger<OpenFeatureOutboxRelayGate>.Instance, client);

        Assert.False(await gate.IsRelayEnabledAsync(CancellationToken.None));
    }

    [Fact]
    public async Task Reads_true_from_the_provider()
    {
        var client = await ClientForAsync("gate-reads-true", "on");
        var gate = new OpenFeatureOutboxRelayGate(NullLogger<OpenFeatureOutboxRelayGate>.Instance, client);

        Assert.True(await gate.IsRelayEnabledAsync(CancellationToken.None));
    }

    [Fact]
    public async Task Defaults_to_enabled_when_no_feature_client_is_available()
    {
        var gate = new OpenFeatureOutboxRelayGate(NullLogger<OpenFeatureOutboxRelayGate>.Instance, featureClient: null);

        Assert.True(await gate.IsRelayEnabledAsync(CancellationToken.None));
    }
}
