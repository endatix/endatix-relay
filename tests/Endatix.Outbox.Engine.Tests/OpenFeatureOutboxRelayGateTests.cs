using Endatix.Outbox.Engine;
using OpenFeature;
using OpenFeature.Providers.Memory;

namespace Endatix.Outbox.Engine.Tests;

/// <summary>
/// Smoke test that the default gate actually consults the OpenFeature provider (not just the hardcoded
/// default). Uses the SDK's in-memory provider via the global Api; isolated to this class.
/// </summary>
[Collection("OpenFeature")]
public class OpenFeatureOutboxRelayGateTests
{
    private static Dictionary<string, Flag> Flags(string defaultVariant) => new()
    {
        [OutboxFlags.RelayInProcess] = new Flag<bool>(
            new Dictionary<string, bool> { ["on"] = true, ["off"] = false }, defaultVariant),
    };

    [Fact]
    public async Task Reads_false_from_the_provider_overriding_the_true_default()
    {
        await Api.Instance.SetProviderAsync(new InMemoryProvider(Flags("off")));
        var gate = new OpenFeatureOutboxRelayGate(Api.Instance.GetClient());

        Assert.False(await gate.IsRelayEnabledAsync(CancellationToken.None));
    }

    [Fact]
    public async Task Reads_true_from_the_provider()
    {
        await Api.Instance.SetProviderAsync(new InMemoryProvider(Flags("on")));
        var gate = new OpenFeatureOutboxRelayGate(Api.Instance.GetClient());

        Assert.True(await gate.IsRelayEnabledAsync(CancellationToken.None));
    }
}

[CollectionDefinition("OpenFeature", DisableParallelization = true)]
public class OpenFeatureCollection;
