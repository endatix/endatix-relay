using System.Threading.Channels;

namespace Endatix.Outbox.Engine;

/// <summary>
/// The relay's wait between polls: it ends when the engine clock reaches the interval, or at a wake. Wakes
/// are kept in one slot, so a wake during a poll ends the next wait at once and extra wakes are dropped.
/// </summary>
internal sealed class OutboxRelaySignal : IOutboxRelaySignal
{
    private readonly Channel<bool> _wakes = Channel.CreateBounded<bool>(
        new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.DropWrite, SingleReader = true });

    public void Wake() => _wakes.Writer.TryWrite(true);

    /// <summary>Waits <paramref name="interval"/> on <paramref name="clock"/>, a wake, or cancellation, whichever is first.</summary>
    public async Task WaitAsync(TimeSpan interval, TimeProvider clock, CancellationToken cancellationToken)
    {
        if (_wakes.Reader.TryRead(out _))
        {
            return;
        }

        using var waiting = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var elapsed = Task.Delay(interval, clock, waiting.Token);
        var woken = _wakes.Reader.WaitToReadAsync(waiting.Token).AsTask();

        var first = await Task.WhenAny(elapsed, woken);
        await waiting.CancelAsync();

        // Take the wake only when it ended the wait. When the interval did, a wake that lands now stays in
        // the slot: the poll that follows may already have missed its write, so it earns one more.
        if (first == woken)
        {
            _wakes.Reader.TryRead(out _);
        }
    }
}
