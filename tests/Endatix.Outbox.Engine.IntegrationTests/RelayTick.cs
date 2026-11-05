using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace Endatix.Outbox.Engine.IntegrationTests;

/// <summary>
/// Runs one relay tick over a real claim store, with a publisher that throws the given exception. Shared by the
/// provider suites to prove what a failed publish leaves in the row.
/// </summary>
internal static class RelayTick
{
    public static async Task RunWithFailingPublisherAsync(IOutboxClaimStore claimStore, Exception failure)
    {
        await using var services = new ServiceCollection()
            .AddSingleton(claimStore)
            .AddSingleton<IIntegrationEventPublisher>(new ThrowingPublisher(failure))
            .AddSingleton<IOutboxRelayGate, AlwaysOnOutboxRelayGate>()
            .BuildServiceProvider();

        var relay = new OutboxRelayBackgroundService(
            services.GetRequiredService<IServiceScopeFactory>(),
            Options.Create(new OutboxOptions()),
            NullLogger<OutboxRelayBackgroundService>.Instance);

        await relay.ProcessOnceAsync(services, CancellationToken.None);
    }

    private sealed class ThrowingPublisher(Exception failure) : IIntegrationEventPublisher
    {
        public Task PublishAsync(IOutboxMessage message, CancellationToken cancellationToken) =>
            Task.FromException(failure);
    }
}
