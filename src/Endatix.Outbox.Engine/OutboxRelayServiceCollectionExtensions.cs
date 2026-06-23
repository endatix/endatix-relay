using System.Data.Common;
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
        services.TryAddSingleton<IOutboxConnectionFactory>(
            sp => new DelegateOutboxConnectionFactory(() => connectionFactory(sp)));
        services.TryAddSingleton<IOutboxClaimStore, SqlOutboxClaimStore>();

        return services;
    }
}
