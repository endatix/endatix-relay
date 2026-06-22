using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Endatix.Outbox.Engine;

/// <summary>DI registration for the outbox relay engine.</summary>
public static class OutboxRelayServiceCollectionExtensions
{
    /// <summary>
    /// Registers the relay loop (<see cref="OutboxRelayBackgroundService"/>), its <see cref="OutboxOptions"/>,
    /// and the default OpenFeature-backed gate. The host must additionally register an
    /// <see cref="IOutboxClaimStore"/>, an <see cref="IIntegrationEventPublisher"/>, and an OpenFeature
    /// provider (which supplies <c>IFeatureClient</c>) — the engine deliberately does not, so it stays
    /// transport- and storage-agnostic.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configureOptions">Optional tuning overrides (poll interval, batch size, lease, attempts).</param>
    public static IServiceCollection AddOutboxRelay(
        this IServiceCollection services, Action<OutboxOptions>? configureOptions = null)
    {
        var optionsBuilder = services.AddOptions<OutboxOptions>();
        if (configureOptions is not null)
        {
            optionsBuilder.Configure(configureOptions);
        }

        services.TryAddScoped<IOutboxRelayGate, OpenFeatureOutboxRelayGate>();
        services.AddHostedService<OutboxRelayBackgroundService>();

        return services;
    }
}
