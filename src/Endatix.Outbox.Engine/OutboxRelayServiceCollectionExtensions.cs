using System.Data.Common;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

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

        // Validate tuning values (positive ranges + backoff cap >= base). Materialized at startup via the
        // relay's ctor (reads IOptions.Value), so bad config fails fast instead of silently misbehaving.
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<OutboxOptions>, OutboxOptionsValidator>());

        services.TryAddScoped<IOutboxRelayGate, OpenFeatureOutboxRelayGate>();
        services.TryAddSingleton(OutboxClock.System);
        services.TryAddSingleton<OutboxRelaySignal>();
        services.TryAddSingleton<IOutboxRelaySignal>(sp => sp.GetRequiredService<OutboxRelaySignal>());

        // Built by hand so the relay always gets the engine clock, never a TimeProvider the host registered.
        services.AddHostedService(sp => new OutboxRelayBackgroundService(
            sp.GetRequiredService<IServiceScopeFactory>(),
            sp.GetRequiredService<IOptions<OutboxOptions>>(),
            sp.GetRequiredService<ILogger<OutboxRelayBackgroundService>>(),
            sp.GetRequiredService<OutboxClock>().Provider,
            sp.GetRequiredService<OutboxRelaySignal>(),
            sp.GetService<OutboxPollInterval>()?.Value));

        return services;
    }

    /// <summary>
    /// Replaces the engine's clock, which defaults to <see cref="TimeProvider.System"/>. Meant for tests: with a
    /// fake clock, advancing it runs the next poll and moves the claim, lease, and backoff times. The relay
    /// logs a warning at startup when it runs on any other clock.
    /// </summary>
    /// <remarks>
    /// Only this call changes the engine's clock; a <see cref="TimeProvider"/> registered in DI for other
    /// reasons doesn't. Never let the engine's clock run behind the clock the host writes
    /// <c>NextAttemptAt</c> with, or due messages look like they're in the future: use the same clock for
    /// both, or only move the engine's forward. A clock that runs ahead claims early and treats other
    /// instances' leases as expired.
    /// </remarks>
    public static IServiceCollection UseOutboxClock(this IServiceCollection services, TimeProvider clock)
    {
        ArgumentNullException.ThrowIfNull(clock);
        services.RemoveAll<OutboxClock>();
        services.AddSingleton(new OutboxClock(clock));
        return services;
    }

    /// <summary>
    /// Sets the relay's poll interval in code, down to 10 ms. Meant for tests; the relay logs a warning at
    /// startup when the interval is under a second. It wins over <see cref="OutboxOptions.PollIntervalSeconds"/>,
    /// and no configuration key can reach it, so production settings can't poll faster than once a second.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="interval"/> is under 10 ms.</exception>
    public static IServiceCollection UseOutboxPollInterval(this IServiceCollection services, TimeSpan interval)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(interval, OutboxPollInterval.Minimum);
        services.RemoveAll<OutboxPollInterval>();
        services.AddSingleton(new OutboxPollInterval(interval));
        return services;
    }

    /// <summary>
    /// Registers the relay with a specific <see cref="IOutboxRelayGate"/> implementation instead of the default
    /// OpenFeature gate — e.g. <c>AddOutboxRelay&lt;AlwaysOnOutboxRelayGate&gt;()</c> for a dedicated worker.
    /// The gate is registered before the base call, so the base method's <c>TryAdd</c> default is skipped.
    /// </summary>
    /// <typeparam name="TGate">The gate implementation to use.</typeparam>
    public static IServiceCollection AddOutboxRelay<TGate>(
        this IServiceCollection services, Action<OutboxOptions>? configureOptions = null)
        where TGate : class, IOutboxRelayGate
    {
        services.AddScoped<IOutboxRelayGate, TGate>();
        return services.AddOutboxRelay(configureOptions);
    }

    /// <summary>
    /// Registers the relay with a custom <see cref="IOutboxRelayGate"/> built by <paramref name="gateFactory"/>
    /// (for gates needing custom construction). The gate is registered before the base call, so the base
    /// method's <c>TryAdd</c> default is skipped.
    /// </summary>
    public static IServiceCollection AddOutboxRelay(
        this IServiceCollection services,
        Func<IServiceProvider, IOutboxRelayGate> gateFactory,
        Action<OutboxOptions>? configureOptions = null)
    {
        services.AddScoped(gateFactory);
        return services.AddOutboxRelay(configureOptions);
    }

    /// <summary>
    /// Registers the default ADO.NET <see cref="SqlOutboxClaimStore"/> over a host-supplied connection. This
    /// is the engine's shared database layer — the same implementation backs the in-process API relay and the
    /// standalone worker. The host supplies the <paramref name="dialect"/>, a <paramref name="connectionFactory"/>
    /// (returning an unopened provider connection, e.g. <c>new NpgsqlConnection(connString)</c>), and optionally
    /// the <paramref name="tableName"/> (default <see cref="OutboxSchema.DefaultTable"/>; both hosts must agree).
    /// </summary>
    public static IServiceCollection AddSqlOutboxClaimStore(
        this IServiceCollection services,
        OutboxSqlDialect dialect,
        Func<IServiceProvider, DbConnection> connectionFactory,
        string? tableName = null)
    {
        services.Configure<OutboxSqlOptions>(options =>
        {
            options.Dialect = dialect;
            options.TableName = tableName ?? OutboxSchema.DefaultTable;
        });
        // Scoped, not singleton: the factory closes over `sp`, so it must bind the per-tick scope's provider
        // — a singleton would pin the root container and break any host factory that resolves scoped services.
        services.TryAddScoped<IOutboxConnectionFactory>(
            sp => new DelegateOutboxConnectionFactory(() => connectionFactory(sp)));
        services.TryAddSingleton(OutboxClock.System);
        // Built by hand so the store always gets the engine clock, never a TimeProvider the host registered.
        services.TryAddScoped<IOutboxClaimStore>(sp => new SqlOutboxClaimStore(
            sp.GetRequiredService<IOutboxConnectionFactory>(),
            sp.GetRequiredService<IOptions<OutboxSqlOptions>>(),
            sp.GetRequiredService<ILogger<SqlOutboxClaimStore>>(),
            sp.GetRequiredService<OutboxClock>().Provider));

        return services;
    }
}
