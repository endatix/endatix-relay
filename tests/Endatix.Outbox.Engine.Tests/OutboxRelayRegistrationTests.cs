using Endatix.Outbox.Engine;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace Endatix.Outbox.Engine.Tests;

public class OutboxRelayRegistrationTests
{
    [Fact]
    public void Default_registers_the_OpenFeature_gate()
    {
        var services = new ServiceCollection();

        services.AddOutboxRelay();

        var gate = Assert.Single(GateDescriptors(services));
        Assert.Equal(typeof(OpenFeatureOutboxRelayGate), gate.ImplementationType);
    }

    [Fact]
    public void Generic_overload_overrides_the_default_gate_and_leaves_exactly_one()
    {
        var services = new ServiceCollection();

        services.AddOutboxRelay<AlwaysOnOutboxRelayGate>();

        var gate = Assert.Single(GateDescriptors(services)); // TryAdd default skipped → exactly one
        Assert.Equal(typeof(AlwaysOnOutboxRelayGate), gate.ImplementationType);

        Assert.Equal(typeof(AlwaysOnOutboxRelayGate), ResolveGateType(services));
    }

    [Fact]
    public void Factory_overload_overrides_the_default_gate_and_leaves_exactly_one()
    {
        var services = new ServiceCollection();

        services.AddOutboxRelay(_ => new AlwaysOnOutboxRelayGate());

        var gate = Assert.Single(GateDescriptors(services));
        Assert.NotNull(gate.ImplementationFactory);

        Assert.Equal(typeof(AlwaysOnOutboxRelayGate), ResolveGateType(services));
    }

    [Fact]
    public void Override_does_not_leave_the_OpenFeature_gate_registered()
    {
        var services = new ServiceCollection();

        services.AddOutboxRelay<AlwaysOnOutboxRelayGate>();

        Assert.DoesNotContain(GateDescriptors(services), d => d.ImplementationType == typeof(OpenFeatureOutboxRelayGate));
    }

    [Fact]
    public async Task AlwaysOnOutboxRelayGate_is_always_enabled()
    {
        Assert.True(await new AlwaysOnOutboxRelayGate().IsRelayEnabledAsync(CancellationToken.None));
    }

    [Fact]
    public async Task Default_gate_resolves_and_fails_open_when_OpenFeature_is_not_registered()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>)); // logging, but no OpenFeature provider
        services.AddOutboxRelay();
        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        var gate = scope.ServiceProvider.GetRequiredService<IOutboxRelayGate>();

        Assert.True(await gate.IsRelayEnabledAsync(CancellationToken.None)); // default ON, no throw
    }

    [Fact]
    public void Valid_options_pass_validation()
    {
        var services = new ServiceCollection();
        services.AddOutboxRelay();
        using var provider = services.BuildServiceProvider();

        var options = provider.GetRequiredService<IOptions<OutboxOptions>>().Value;

        Assert.Equal(50, options.BatchSize);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Nonpositive_tuning_value_fails_validation(int badBatchSize)
    {
        var services = new ServiceCollection();
        services.AddOutboxRelay(o => o.BatchSize = badBatchSize);
        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<OutboxOptions>>();

        Assert.Throws<OptionsValidationException>(() => _ = options.Value);
    }

    [Fact]
    public void Backoff_cap_below_base_fails_validation()
    {
        var services = new ServiceCollection();
        services.AddOutboxRelay(o => { o.BackoffBaseSeconds = 100; o.BackoffCapSeconds = 10; });
        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<OutboxOptions>>();

        Assert.Throws<OptionsValidationException>(() => _ = options.Value);
    }

    private static IEnumerable<ServiceDescriptor> GateDescriptors(IServiceCollection services) =>
        services.Where(d => d.ServiceType == typeof(IOutboxRelayGate));

    // Resolve and return the gate's TYPE inside the scope — never hand back a service whose provider/scope
    // has already been disposed (the assertion would run against an invalid instance).
    private static Type ResolveGateType(IServiceCollection services)
    {
        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        return scope.ServiceProvider.GetRequiredService<IOutboxRelayGate>().GetType();
    }
}
