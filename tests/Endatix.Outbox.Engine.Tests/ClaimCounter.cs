using Endatix.Outbox.Engine;

namespace Endatix.Outbox.Engine.Tests;

/// <summary>
/// Stands behind a substitute claim store: counts claims, lets a test wait for the Nth one, hands out a
/// one-shot batch, and can hold a claim open to act while a poll is running.
/// </summary>
internal sealed class ClaimCounter
{
    private readonly Lock _lock = new();
    private readonly List<(int Count, TaskCompletionSource Reached)> _waiters = [];
    private Hold? _hold;
    private int _count;

    public int Count => Volatile.Read(ref _count);

    /// <summary>The batch the next claim returns; later claims return nothing.</summary>
    public IOutboxMessage[] NextBatch { get; set; } = [];

    public async Task<IReadOnlyList<IOutboxMessage>> OnClaimAsync()
    {
        var hold = Interlocked.Exchange(ref _hold, null);
        if (hold is not null)
        {
            hold.EnteredSource.TrySetResult();
            await hold.Released.Task;
        }

        IOutboxMessage[] batch;
        lock (_lock)
        {
            batch = NextBatch;
            NextBatch = [];
            _count++;
            foreach (var waiter in _waiters.Where(w => w.Count <= _count))
            {
                waiter.Reached.TrySetResult();
            }
        }

        return batch;
    }

    /// <summary>Completes once the Nth claim has finished; throws <see cref="TimeoutException"/> after <paramref name="timeout"/>.</summary>
    public Task WaitForAsync(int count, TimeSpan timeout)
    {
        var reached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (_lock)
        {
            if (_count >= count)
            {
                return Task.CompletedTask;
            }

            _waiters.Add((count, reached));
        }

        return reached.Task.WaitAsync(timeout);
    }

    /// <summary>Holds the next claim open until <see cref="Hold.Release"/>.</summary>
    public Hold HoldNextClaim()
    {
        var hold = new Hold();
        _hold = hold;
        return hold;
    }

    internal sealed class Hold
    {
        internal TaskCompletionSource EnteredSource { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal TaskCompletionSource Released { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task Entered => EnteredSource.Task;

        public void Release() => Released.TrySetResult();
    }
}
