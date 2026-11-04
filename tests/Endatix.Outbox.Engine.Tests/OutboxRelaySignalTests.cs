using Endatix.Outbox.Engine;
using Microsoft.Extensions.Time.Testing;

namespace Endatix.Outbox.Engine.Tests;

public class OutboxRelaySignalTests
{
    private static readonly TimeSpan Interval = TimeSpan.FromHours(1);
    private readonly FakeTimeProvider _clock = new();
    private readonly OutboxRelaySignal _signal = new();

    [Fact]
    public async Task Wait_ends_when_the_engine_clock_reaches_the_interval()
    {
        var wait = _signal.WaitAsync(Interval, _clock, CancellationToken.None);
        Assert.False(wait.IsCompleted);

        _clock.Advance(Interval);

        await wait.WaitAsync(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task Wake_ends_the_current_wait()
    {
        var wait = _signal.WaitAsync(Interval, _clock, CancellationToken.None);

        _signal.Wake();

        await wait.WaitAsync(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task Wakes_before_a_wait_are_remembered_as_exactly_one()
    {
        _signal.Wake();
        _signal.Wake();
        _signal.Wake();

        await _signal.WaitAsync(Interval, _clock, CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(5));
        var second = _signal.WaitAsync(Interval, _clock, CancellationToken.None);

        Assert.False(second.IsCompleted);
        _clock.Advance(Interval);
        await second.WaitAsync(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task Cancellation_ends_the_wait_without_throwing()
    {
        using var cts = new CancellationTokenSource();
        var wait = _signal.WaitAsync(Interval, _clock, cts.Token);

        await cts.CancelAsync();

        await wait.WaitAsync(TimeSpan.FromSeconds(5));
    }
}
