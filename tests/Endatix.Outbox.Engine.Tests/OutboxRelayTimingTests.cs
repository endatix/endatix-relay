using System.Diagnostics;
using Endatix.Outbox.Engine;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using NSubstitute;

namespace Endatix.Outbox.Engine.Tests;

/// <summary>The relay loop's timing seams: the engine clock, the poll interval, and the wake signal.</summary>
public sealed class OutboxRelayTimingTests : IAsyncDisposable
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan TaskDelayMaximum = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    private readonly IOutboxClaimStore _claimStore = Substitute.For<IOutboxClaimStore>();
    private readonly IIntegrationEventPublisher _publisher = Substitute.For<IIntegrationEventPublisher>();
    private readonly IOutboxRelayGate _gate = Substitute.For<IOutboxRelayGate>();
    private readonly FakeTimeProvider _clock = new(DateTimeOffset.UtcNow);
    private readonly ClaimCounter _claims = new();
    private ServiceProvider? _provider;

    public OutboxRelayTimingTests()
    {
        _gate.IsRelayEnabledAsync(Arg.Any<CancellationToken>()).Returns(true);
        _claimStore.ClaimBatchAsync(Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(_ => _claims.OnClaimAsync());
    }

    public async ValueTask DisposeAsync()
    {
        if (_provider is not null)
        {
            await _provider.DisposeAsync();
        }
    }

    private (OutboxRelayBackgroundService relay, IOutboxRelaySignal signal) Build(
        Action<IServiceCollection>? configure = null, ILogger<OutboxRelayBackgroundService>? logger = null)
    {
        var services = new ServiceCollection()
            .AddSingleton(_claimStore)
            .AddSingleton(_publisher)
            .AddSingleton(_gate)
            .AddSingleton(logger ?? NullLogger<OutboxRelayBackgroundService>.Instance)
            .AddOutboxRelay(o => o.PollIntervalSeconds = 3600)
            .UseOutboxClock(_clock);
        configure?.Invoke(services);
        _provider = services.BuildServiceProvider();

        var relay = _provider.GetServices<IHostedService>().OfType<OutboxRelayBackgroundService>().Single();
        return (relay, _provider.GetRequiredService<IOutboxRelaySignal>());
    }

    [Fact]
    public async Task Advancing_the_engine_clock_runs_the_next_poll_with_no_real_wait()
    {
        var (relay, _) = Build();
        var stopwatch = Stopwatch.StartNew();

        await relay.StartAsync(CancellationToken.None);
        await _claims.WaitForAsync(1, Patience);
        await AdvanceUntilAsync(() => _claims.Count >= 2, relay.PollInterval);
        await relay.StopAsync(CancellationToken.None);

        Assert.True(stopwatch.Elapsed < Patience, $"Took {stopwatch.Elapsed}; the poll interval is {relay.PollInterval}.");
    }

    [Fact]
    public async Task A_message_written_then_woken_is_delivered_without_waiting_out_the_interval()
    {
        var (relay, signal) = Build();
        var message = new FakeOutboxMessage(1);
        var published = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _publisher.PublishAsync(message, Arg.Any<CancellationToken>())
            .Returns(_ => { published.TrySetResult(); return Task.CompletedTask; });
        var start = _clock.GetUtcNow();
        await relay.StartAsync(CancellationToken.None);
        await _claims.WaitForAsync(1, Patience);

        _claims.NextBatch = [message];
        signal.Wake();

        await published.Task.WaitAsync(Patience);
        await relay.StopAsync(CancellationToken.None);
        await _publisher.Received(1).PublishAsync(message, Arg.Any<CancellationToken>());
        Assert.Equal(start, _clock.GetUtcNow()); // the clock never moved: only the wake ran the poll
    }

    [Fact]
    public async Task Wakes_during_a_poll_cause_exactly_one_extra_poll_as_soon_as_it_finishes()
    {
        var (relay, signal) = Build();
        var firstPoll = _claims.HoldNextClaim();
        await relay.StartAsync(CancellationToken.None);
        await firstPoll.Entered.WaitAsync(Patience);

        signal.Wake();
        signal.Wake();
        signal.Wake();
        firstPoll.Release();

        await _claims.WaitForAsync(2, Patience);
        var thirdPoll = _claims.WaitForAsync(3, TimeSpan.FromMilliseconds(300));
        await Assert.ThrowsAsync<TimeoutException>(() => thirdPoll);
        await relay.StopAsync(CancellationToken.None);
        Assert.Equal(2, _claims.Count);
    }

    [Fact]
    public void Code_poll_interval_wins_over_PollIntervalSeconds()
    {
        var (relay, _) = Build(s => s.UseOutboxPollInterval(TimeSpan.FromMilliseconds(50)));

        Assert.Equal(TimeSpan.FromMilliseconds(50), relay.PollInterval);
    }

    [Fact]
    public void PollIntervalSeconds_sets_the_interval_without_a_code_value()
    {
        var (relay, _) = Build();

        Assert.Equal(TimeSpan.FromHours(1), relay.PollInterval);
    }

    [Theory]
    [InlineData(9)]
    [InlineData(0)]
    [InlineData(-1)]
    public void Code_poll_interval_under_10_ms_is_rejected(int milliseconds)
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentOutOfRangeException>(
            () => services.UseOutboxPollInterval(TimeSpan.FromMilliseconds(milliseconds)));
    }

    [Fact]
    public void Code_poll_interval_above_Task_Delay_maximum_is_rejected_naming_both_bounds()
    {
        var services = new ServiceCollection();
        var tooLong = TaskDelayMaximum + TimeSpan.FromMilliseconds(1);

        var ex = Assert.Throws<ArgumentOutOfRangeException>(() => services.UseOutboxPollInterval(tooLong));

        Assert.Contains("10 ms", ex.Message);
        Assert.Contains(TaskDelayMaximum.ToString(), ex.Message);
    }

    [Fact]
    public void Code_poll_interval_of_60_days_is_rejected()
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentOutOfRangeException>(() => services.UseOutboxPollInterval(TimeSpan.FromDays(60)));
    }

    [Fact]
    public void Infinite_code_poll_interval_is_rejected_with_its_own_message()
    {
        var services = new ServiceCollection();

        var ex = Assert.Throws<ArgumentOutOfRangeException>(() => services.UseOutboxPollInterval(Timeout.InfiniteTimeSpan));

        Assert.Contains("infinite", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("under 10 ms", ex.Message);
    }

    [Fact]
    public async Task Code_poll_interval_at_the_maximum_runs_the_relay_without_faulting()
    {
        var (relay, _) = Build(s => s.UseOutboxPollInterval(TaskDelayMaximum));

        await relay.StartAsync(CancellationToken.None);
        await _claims.WaitForAsync(1, Patience);
        await Task.Delay(50);
        Assert.False(relay.ExecuteTask!.IsFaulted, relay.ExecuteTask.Exception?.ToString());
        await relay.StopAsync(CancellationToken.None);
    }

    [Fact]
    public void Code_poll_interval_of_10_ms_is_accepted()
    {
        var (relay, _) = Build(s => s.UseOutboxPollInterval(TimeSpan.FromMilliseconds(10)));

        Assert.Equal(TimeSpan.FromMilliseconds(10), relay.PollInterval);
    }

    [Fact]
    public async Task Startup_warns_about_a_clock_other_than_the_system_clock()
    {
        var logger = new RecordingLogger<OutboxRelayBackgroundService>();
        var (relay, _) = Build(logger: logger);

        await relay.StartAsync(CancellationToken.None);
        await _claims.WaitForAsync(1, Patience);
        await relay.StopAsync(CancellationToken.None);

        Assert.Single(logger.Messages, m => m.Contains("FakeTimeProvider") && m.Contains("TimeProvider.System"));
        Assert.DoesNotContain(logger.Messages, m => m.Contains("poll interval"));
    }

    [Fact]
    public async Task Startup_warns_about_a_sub_second_poll_interval()
    {
        var logger = new RecordingLogger<OutboxRelayBackgroundService>();
        var (relay, _) = Build(s => s.UseOutboxClock(TimeProvider.System).UseOutboxPollInterval(TimeSpan.FromMilliseconds(50)), logger);

        await relay.StartAsync(CancellationToken.None);
        await _claims.WaitForAsync(1, Patience);
        await relay.StopAsync(CancellationToken.None);

        Assert.Single(logger.Messages, m => m.Contains("poll interval") && m.Contains("50 ms"));
        Assert.DoesNotContain(logger.Messages, m => m.Contains("TimeProvider.System"));
    }

    [Fact]
    public async Task Default_settings_log_no_warning()
    {
        var logger = new RecordingLogger<OutboxRelayBackgroundService>();
        var (relay, _) = Build(s => s.UseOutboxClock(TimeProvider.System), logger);

        await relay.StartAsync(CancellationToken.None);
        await _claims.WaitForAsync(1, Patience);
        await relay.StopAsync(CancellationToken.None);

        Assert.DoesNotContain(logger.Entries, e => e.Level == LogLevel.Warning);
    }

    // Task.Delay registers its timer only once the loop reaches the wait, so keep advancing until the poll lands.
    private async Task AdvanceUntilAsync(Func<bool> done, TimeSpan step)
    {
        using var timeout = new CancellationTokenSource(Patience);
        while (!done())
        {
            timeout.Token.ThrowIfCancellationRequested();
            _clock.Advance(step);
            await Task.Delay(5, timeout.Token);
        }
    }
}
