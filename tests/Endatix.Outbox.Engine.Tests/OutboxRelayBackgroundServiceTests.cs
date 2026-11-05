using Endatix.Outbox.Engine;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Endatix.Outbox.Engine.Tests;

public class OutboxRelayBackgroundServiceTests
{
    private readonly IOutboxClaimStore _claimStore = Substitute.For<IOutboxClaimStore>();
    private readonly IIntegrationEventPublisher _publisher = Substitute.For<IIntegrationEventPublisher>();
    private readonly IOutboxRelayGate _gate = Substitute.For<IOutboxRelayGate>();

    private (OutboxRelayBackgroundService relay, IServiceProvider services) Build(
        OutboxOptions? options = null, ILogger<OutboxRelayBackgroundService>? logger = null)
    {
        var services = new ServiceCollection()
            .AddSingleton(_claimStore)
            .AddSingleton(_publisher)
            .AddSingleton(_gate)
            .BuildServiceProvider();

        var relay = new OutboxRelayBackgroundService(
            services.GetRequiredService<IServiceScopeFactory>(),
            Options.Create(options ?? new OutboxOptions()),
            logger ?? NullLogger<OutboxRelayBackgroundService>.Instance);

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
            message, Arg.Is<DateTimeOffset>(d => d > DateTimeOffset.UtcNow), Arg.Any<string>(), Arg.Any<CancellationToken>());
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
            message, Arg.Any<DateTimeOffset>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Permanent_failure_marks_failed_at_once()
    {
        GateReturns(true);
        var message = new FakeOutboxMessage(1, Attempts: 0);
        ClaimReturns(message);
        _publisher.PublishAsync(message, Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new OutboxPermanentFailureException("Event type 'form.retired' is retired.")));
        var (relay, services) = Build(new OutboxOptions { MaxAttempts = 8 });

        await relay.ProcessOnceAsync(services, CancellationToken.None);

        await _claimStore.Received(1).MarkFailedAsync(message, Arg.Any<string>(), Arg.Any<CancellationToken>());
        await _claimStore.DidNotReceive().RescheduleAsync(
            message, Arg.Any<DateTimeOffset>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
        await _claimStore.DidNotReceive().MarkSentAsync(message, Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Permanent_failure_is_logged_as_an_error_with_the_message_and_reason()
    {
        GateReturns(true);
        var message = new FakeOutboxMessage(5, EventType: "form.retired");
        ClaimReturns(message);
        _publisher.PublishAsync(message, Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new OutboxPermanentFailureException("no longer published")));
        var logger = new RecordingLogger<OutboxRelayBackgroundService>();
        var (relay, services) = Build(logger: logger);

        await relay.ProcessOnceAsync(services, CancellationToken.None);

        var entry = Assert.Single(logger.Entries, e => e.Level == LogLevel.Error);
        Assert.Contains("5", entry.Message);
        Assert.Contains("form.retired", entry.Message);
        Assert.Contains("no longer published", entry.Message);
    }

    [Fact]
    public void Permanent_failure_keeps_its_reason_and_cause()
    {
        var cause = new InvalidOperationException("unknown type");

        var ex = new OutboxPermanentFailureException("retired", cause);

        Assert.Equal("retired", ex.Message);
        Assert.Same(cause, ex.InnerException);
    }

    [Fact]
    public async Task MarkSent_failure_aborts_the_tick_without_dead_lettering_or_storming_the_batch()
    {
        GateReturns(true);
        var m1 = new FakeOutboxMessage(1, Attempts: 0);
        var m2 = new FakeOutboxMessage(2, Attempts: 0);
        ClaimReturns(m1, m2);
        _claimStore.MarkSentAsync(m1, Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new InvalidOperationException("mark boom")));
        var (relay, services) = Build(new OutboxOptions { MaxAttempts = 1 }); // would dead-letter if misrouted

        // The mark failure propagates, ending the tick (the ExecuteAsync loop logs + retries next poll).
        var aborted = await Assert.ThrowsAsync<InvalidOperationException>(
            () => relay.ProcessOnceAsync(services, CancellationToken.None));
        Assert.Equal("mark boom", aborted.InnerException?.Message);
        Assert.Contains("MarkSent failed", aborted.Message);

        await _publisher.Received(1).PublishAsync(m1, Arg.Any<CancellationToken>());
        await _claimStore.Received(1).MarkSentAsync(m1, Arg.Any<string>(), Arg.Any<CancellationToken>());
        // Batch aborted: the rest of the claimed rows are NOT published (no duplicate storm)...
        await _publisher.DidNotReceive().PublishAsync(m2, Arg.Any<CancellationToken>());
        // ...and a MarkSent failure is never rerouted to reschedule/dead-letter.
        await _claimStore.DidNotReceive().RescheduleAsync(m1, Arg.Any<DateTimeOffset>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
        await _claimStore.DidNotReceive().MarkFailedAsync(m1, Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Gate_changes_are_logged_once_per_transition()
    {
        GateReturns(true, true, false);
        ClaimReturns();
        var logger = new RecordingLogger<OutboxRelayBackgroundService>();
        var (relay, services) = Build(logger: logger);

        for (var tick = 0; tick < 3; tick++)
        {
            await relay.ProcessOnceAsync(services, CancellationToken.None);
        }

        Assert.Equal(
            [
                $"Outbox relay enabled (flag '{OutboxFlags.RelayInProcess}').",
                $"Outbox relay disabled (flag '{OutboxFlags.RelayInProcess}').",
            ],
            logger.Messages);
    }

    [Fact]
    public async Task Start_and_stop_are_logged_with_the_instance()
    {
        // ExecuteAsync runs on a background thread: stop only once the loop has polled the gate.
        var polled = new TaskCompletionSource();
        _gate.IsRelayEnabledAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            polled.TrySetResult();
            return false;
        });
        var logger = new RecordingLogger<OutboxRelayBackgroundService>();
        var (relay, _) = Build(logger: logger);

        await relay.StartAsync(CancellationToken.None);
        await polled.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await relay.StopAsync(CancellationToken.None);

        Assert.StartsWith("Outbox relay started (instance ", logger.Messages[0]);
        Assert.StartsWith("Outbox relay stopping (instance ", logger.Messages[^1]);
    }

    [Fact]
    public void EnsureRequiredServices_throws_when_a_required_service_is_missing()
    {
        using var services = new ServiceCollection()
            .AddSingleton(_gate)
            .AddSingleton(_claimStore)
            // IIntegrationEventPublisher deliberately not registered
            .BuildServiceProvider();

        Assert.Throws<InvalidOperationException>(
            () => OutboxRelayBackgroundService.EnsureRequiredServices(services));
    }

    [Fact]
    public void EnsureRequiredServices_passes_when_all_present()
    {
        using var services = new ServiceCollection()
            .AddSingleton(_gate)
            .AddSingleton(_claimStore)
            .AddSingleton(_publisher)
            .BuildServiceProvider();

        var exception = Record.Exception(() => OutboxRelayBackgroundService.EnsureRequiredServices(services));

        Assert.Null(exception);
    }

    [Theory]
    [InlineData(0, 5)]      // base * 2^0 = 5
    [InlineData(1, 10)]     // 5 * 2^1 = 10
    [InlineData(3, 40)]     // 5 * 2^3 = 40
    [InlineData(10, 300)]   // 5 * 1024 = 5120 -> capped at 300
    public void ComputeNextAttempt_is_capped_exponential(int attempts, int expectedSeconds)
    {
        var now = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var options = new OutboxOptions { BackoffBaseSeconds = 5, BackoffCapSeconds = 300 };

        var next = OutboxRelayBackgroundService.ComputeNextAttempt(attempts, now, options);

        Assert.Equal(now.AddSeconds(expectedSeconds), next);
    }
}
