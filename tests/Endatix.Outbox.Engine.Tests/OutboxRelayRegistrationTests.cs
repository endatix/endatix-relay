using Endatix.Outbox.Engine;
using Microsoft.Extensions.DependencyInjection;

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

        Assert.IsType<AlwaysOnOutboxRelayGate>(ResolveGate(services));
    }

    [Fact]
    public void Factory_overload_overrides_the_default_gate_and_leaves_exactly_one()
    {
        var services = new ServiceCollection();

        services.AddOutboxRelay(_ => new AlwaysOnOutboxRelayGate());

        var gate = Assert.Single(GateDescriptors(services));
        Assert.NotNull(gate.ImplementationFactory);

        Assert.IsType<AlwaysOnOutboxRelayGate>(ResolveGate(services));
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

    private static IEnumerable<ServiceDescriptor> GateDescriptors(IServiceCollection services) =>
        services.Where(d => d.ServiceType == typeof(IOutboxRelayGate));

    private static IOutboxRelayGate ResolveGate(IServiceCollection services)
    {
        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        return scope.ServiceProvider.GetRequiredService<IOutboxRelayGate>();
    }
}
