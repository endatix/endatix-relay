using Endatix.Outbox.Engine;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using NSubstitute;

namespace Endatix.Outbox.Engine.Tests;

/// <summary>Which clock and poll interval the engine gets from DI and from configuration.</summary>
public class OutboxClockRegistrationTests
{
    private readonly FakeTimeProvider _fake = new();

    private static ServiceCollection Services()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddScoped(_ => Substitute.For<System.Data.Common.DbConnection>());
        return services;
    }

    private static (OutboxRelayBackgroundService relay, SqlOutboxClaimStore store) Resolve(IServiceCollection services)
    {
        var provider = services.BuildServiceProvider();
        var relay = provider.GetServices<IHostedService>().OfType<OutboxRelayBackgroundService>().Single();
        var store = (SqlOutboxClaimStore)provider.CreateScope().ServiceProvider.GetRequiredService<IOutboxClaimStore>();
        return (relay, store);
    }

    private static IServiceCollection AddEngine(IServiceCollection services) => services
        .AddOutboxRelay()
        .AddSqlOutboxClaimStore(OutboxSqlDialect.PostgreSql, sp => sp.GetRequiredService<System.Data.Common.DbConnection>());

    [Fact]
    public void The_engine_uses_the_system_clock_by_default()
    {
        var (relay, store) = Resolve(AddEngine(Services()));

        Assert.Same(TimeProvider.System, relay.Clock);
        Assert.Same(TimeProvider.System, store.Clock);
    }

    [Fact]
    public void A_TimeProvider_the_host_registers_does_not_change_the_engine_clock()
    {
        var services = Services();
        services.AddSingleton<TimeProvider>(_fake);

        var (relay, store) = Resolve(AddEngine(services));

        Assert.Same(TimeProvider.System, relay.Clock);
        Assert.Same(TimeProvider.System, store.Clock);
    }

    [Fact]
    public void Registrations_keep_their_implementation_types_so_hosts_can_detect_them()
    {
        var services = AddEngine(Services()).UseOutboxClock(_fake).UseOutboxPollInterval(TimeSpan.FromMilliseconds(50));

        Assert.Contains(services, d => d.ServiceType == typeof(IHostedService) && d.ImplementationType == typeof(OutboxRelayBackgroundService));
        Assert.Contains(services, d => d.ServiceType == typeof(IOutboxClaimStore) && d.ImplementationType == typeof(SqlOutboxClaimStore));
    }

    [Fact]
    public void A_store_the_host_registers_itself_does_not_take_the_hosts_TimeProvider()
    {
        var services = Services();
        services.AddSingleton<TimeProvider>(_fake);
        services.AddSingleton(Substitute.For<IOutboxConnectionFactory>());
        services.Configure<OutboxSqlOptions>(_ => { });
        services.AddScoped<IOutboxClaimStore, SqlOutboxClaimStore>();
        using var provider = services.BuildServiceProvider();

        var store = (SqlOutboxClaimStore)provider.CreateScope().ServiceProvider.GetRequiredService<IOutboxClaimStore>();

        Assert.Same(TimeProvider.System, store.Clock);
    }

    [Fact]
    public void UseOutboxClock_replaces_the_clock_for_the_relay_and_the_claim_store()
    {
        var (relay, store) = Resolve(AddEngine(Services()).UseOutboxClock(_fake));

        Assert.Same(_fake, relay.Clock);
        Assert.Same(_fake, store.Clock);
    }

    [Fact]
    public void UseOutboxClock_before_AddOutboxRelay_still_wins()
    {
        var services = Services();
        services.UseOutboxClock(_fake);

        var (relay, store) = Resolve(AddEngine(services));

        Assert.Same(_fake, relay.Clock);
        Assert.Same(_fake, store.Clock);
    }

    [Fact]
    public void Existing_public_constructors_still_compile_and_use_the_system_clock()
    {
        var provider = new ServiceCollection().BuildServiceProvider();
        var relay = new OutboxRelayBackgroundService(
            provider.GetRequiredService<Microsoft.Extensions.DependencyInjection.IServiceScopeFactory>(),
            Microsoft.Extensions.Options.Options.Create(new OutboxOptions()),
            NullLogger<OutboxRelayBackgroundService>.Instance);
        var store = new SqlOutboxClaimStore(
            Substitute.For<IOutboxConnectionFactory>(),
            Microsoft.Extensions.Options.Options.Create(new OutboxSqlOptions()),
            NullLogger<SqlOutboxClaimStore>.Instance);

        Assert.Same(TimeProvider.System, relay.Clock);
        Assert.Same(TimeProvider.System, store.Clock);
        Assert.Equal(TimeSpan.FromSeconds(5), relay.PollInterval);
    }

    public static TheoryData<string, string> SubSecondKeys() => new()
    {
        { "PollInterval", "00:00:00.010" },
        { "PollIntervalMilliseconds", "10" },
        { "PollIntervalMs", "10" },
        { "MinimumPollInterval", "00:00:00.010" },
        { "OutboxPollInterval", "00:00:00.010" },
        { "OutboxPollInterval:Value", "00:00:00.010" },
        { "Value", "00:00:00.010" },
    };

    [Theory]
    [MemberData(nameof(SubSecondKeys))]
    public void No_configuration_key_sets_a_sub_second_interval(string key, string value)
    {
        const string prefix = "R16TEST_";
        var variable = $"{prefix}Endatix__Outbox__{key.Replace(":", "__")}";
        Environment.SetEnvironmentVariable(variable, value);
        try
        {
            var configuration = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string?> { [$"Endatix:Outbox:{key}"] = value })
                .AddEnvironmentVariables(prefix)
                .Build();
            var section = configuration.GetSection("Endatix:Outbox");
            var services = Services();
            services.AddOptions<OutboxOptions>().Bind(section);
            AddEngine(services);

            var (relay, _) = Resolve(services);

            Assert.True(relay.PollInterval >= TimeSpan.FromSeconds(1), $"{key}={value} set {relay.PollInterval}.");
        }
        finally
        {
            Environment.SetEnvironmentVariable(variable, null);
        }
    }

    [Fact]
    public void OutboxOptions_has_no_bindable_TimeSpan_or_fractional_property()
    {
        var bindable = typeof(OutboxOptions).GetProperties()
            .Where(p => p.CanWrite && p.SetMethod!.IsPublic)
            .Select(p => p.PropertyType)
            .Distinct();

        Assert.Equal([typeof(int)], bindable);
    }

    [Fact]
    public void PollIntervalSeconds_keeps_its_one_second_minimum_and_five_second_default()
    {
        var range = typeof(OutboxOptions).GetProperty(nameof(OutboxOptions.PollIntervalSeconds))!
            .GetCustomAttributes(typeof(System.ComponentModel.DataAnnotations.RangeAttribute), false)
            .Cast<System.ComponentModel.DataAnnotations.RangeAttribute>()
            .Single();

        Assert.Equal(1, range.Minimum);
        Assert.Equal(5, new OutboxOptions().PollIntervalSeconds);
    }
}
