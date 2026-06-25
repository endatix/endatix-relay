using Endatix.Outbox.Engine;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Endatix.Outbox.Engine.Tests;

public class OutboxRelayBackgroundServiceTests
{
    private readonly IOutboxClaimStore _claimStore = Substitute.For<IOutboxClaimStore>();
    private readonly IIntegrationEventPublisher _publisher = Substitute.For<IIntegrationEventPublisher>();
    private readonly IOutboxRelayGate _gate = Substitute.For<IOutboxRelayGate>();

    private (OutboxRelayBackgroundService relay, IServiceProvider services) Build(OutboxOptions? options = null)
    {
        var services = new ServiceCollection()
            .AddSingleton(_claimStore)
            .AddSingleton(_publisher)
            .AddSingleton(_gate)
            .BuildServiceProvider();

        var relay = new OutboxRelayBackgroundService(
            services.GetRequiredService<IServiceScopeFactory>(),
            Options.Create(options ?? new OutboxOptions()),
            NullLogger<OutboxRelayBackgroundService>.Instance);

        return (relay, services);
    }

    private void GateReturns(params bool[] values) =>
        _gate.IsRelayEnabledAsync(Arg.Any<CancellationToken>()).Returns(values[0], values[1..]);

    private void ClaimReturns(params IOutboxMessage[] messages) =>
        _claimStore.ClaimBatchAsync(Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns((IReadOnlyList<IOutboxMessage>)messages);

    [Fact]
    public async Task Enabled_publishes_and_marks_sent_for_each_claimed_row()
    {
        GateReturns(true);
        var m1 = new FakeOutboxMessage(1);
        var m2 = new FakeOutboxMessage(2);
        ClaimReturns(m1, m2);
        var (relay, services) = Build();

        var processed = await relay.ProcessOnceAsync(services, CancellationToken.None);

        Assert.Equal(2, processed);
        await _publisher.Received(1).PublishAsync(m1, Arg.Any<CancellationToken>());
        await _publisher.Received(1).PublishAsync(m2, Arg.Any<CancellationToken>());
        await _claimStore.Received(1).MarkSentAsync(m1, Arg.Any<string>(), Arg.Any<CancellationToken>());
        await _claimStore.Received(1).MarkSentAsync(m2, Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Disabled_claims_nothing()
    {
        GateReturns(false);
        var (relay, services) = Build();

        var processed = await relay.ProcessOnceAsync(services, CancellationToken.None);

        Assert.Equal(0, processed);
        await _claimStore.DidNotReceive().ClaimBatchAsync(
            Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<int>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Flag_flip_resumes_without_restart()
    {
        GateReturns(false, true);          // first tick off, second tick on
        ClaimReturns(new FakeOutboxMessage(1));
        var (relay, services) = Build();

        var firstTick = await relay.ProcessOnceAsync(services, CancellationToken.None);
        Assert.Equal(0, firstTick);
        await _claimStore.DidNotReceive().ClaimBatchAsync(
            Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<int>(), Arg.Any<CancellationToken>());

        var secondTick = await relay.ProcessOnceAsync(services, CancellationToken.None);
        Assert.Equal(1, secondTick);
        await _claimStore.Received(1).ClaimBatchAsync(
            Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<int>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Publish_failure_below_max_reschedules_with_future_time()
    {
        GateReturns(true);
        var message = new FakeOutboxMessage(1, Attempts: 0);
        ClaimReturns(message);
        _publisher.PublishAsync(message, Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new InvalidOperationException("boom")));
        var (relay, services) = Build(new OutboxOptions { MaxAttempts = 8 });

        await relay.ProcessOnceAsync(services, CancellationToken.None);

        await _claimStore.Received(1).RescheduleAsync(
            message, Arg.Is<DateTime>(d => d > DateTime.UtcNow), Arg.Any<string>(), Arg.Any<CancellationToken>());
        await _claimStore.DidNotReceive().MarkFailedAsync(message, Arg.Any<string>(), Arg.Any<CancellationToken>());
        await _claimStore.DidNotReceive().MarkSentAsync(message, Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Publish_failure_at_max_marks_failed()
    {
        GateReturns(true);
        var message = new FakeOutboxMessage(1, Attempts: 7); // attempts+1 == MaxAttempts
        ClaimReturns(message);
        _publisher.PublishAsync(message, Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new InvalidOperationException("boom")));
        var (relay, services) = Build(new OutboxOptions { MaxAttempts = 8 });

        await relay.ProcessOnceAsync(services, CancellationToken.None);

        await _claimStore.Received(1).MarkFailedAsync(message, Arg.Any<string>(), Arg.Any<CancellationToken>());
        await _claimStore.DidNotReceive().RescheduleAsync(
            message, Arg.Any<DateTime>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task MarkSent_failure_is_not_treated_as_a_publish_failure()
    {
        GateReturns(true);
        var message = new FakeOutboxMessage(1, Attempts: 0);
        ClaimReturns(message);
        _claimStore.MarkSentAsync(message, Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new InvalidOperationException("mark boom")));
        var (relay, services) = Build(new OutboxOptions { MaxAttempts = 1 }); // would dead-letter if misrouted

        var processed = await relay.ProcessOnceAsync(services, CancellationToken.None);

        Assert.Equal(1, processed); // publish succeeded; mark failure logged, batch not aborted
        await _publisher.Received(1).PublishAsync(message, Arg.Any<CancellationToken>());
        await _claimStore.Received(1).MarkSentAsync(message, Arg.Any<string>(), Arg.Any<CancellationToken>());
        await _claimStore.DidNotReceive().RescheduleAsync(message, Arg.Any<DateTime>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
        await _claimStore.DidNotReceive().MarkFailedAsync(message, Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Theory]
    [InlineData(0, 5)]      // base * 2^0 = 5
    [InlineData(1, 10)]     // 5 * 2^1 = 10
    [InlineData(3, 40)]     // 5 * 2^3 = 40
    [InlineData(10, 300)]   // 5 * 1024 = 5120 -> capped at 300
    public void ComputeNextAttempt_is_capped_exponential(int attempts, int expectedSeconds)
    {
        var now = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var options = new OutboxOptions { BackoffBaseSeconds = 5, BackoffCapSeconds = 300 };

        var next = OutboxRelayBackgroundService.ComputeNextAttempt(attempts, now, options);

        Assert.Equal(now.AddSeconds(expectedSeconds), next);
    }
}
