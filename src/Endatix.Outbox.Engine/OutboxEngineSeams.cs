namespace Endatix.Outbox.Engine;

/// <summary>
/// The engine's clock and code-only poll interval, set only by <c>UseOutboxClock</c> and
/// <c>UseOutboxPollInterval</c> through <c>services.Configure</c>. Internal, so no host binds it from
/// configuration: no config key can set an interval under a second, and a <see cref="TimeProvider"/> the host
/// registers in DI for other reasons doesn't reach the relay.
/// </summary>
internal sealed class OutboxTimingOptions
{
    /// <summary>The smallest code-only interval. Speed comes from the clock and the wake, not a tighter loop.</summary>
    public static readonly TimeSpan MinimumPollInterval = TimeSpan.FromMilliseconds(10);

    /// <summary>The longest wait <see cref="Task.Delay(TimeSpan, TimeProvider, CancellationToken)"/> takes, about 49.7 days.</summary>
    public static readonly TimeSpan MaximumPollInterval = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    /// <summary>The engine's clock; <see cref="TimeProvider.System"/> unless <c>UseOutboxClock</c> set one.</summary>
    public TimeProvider Clock { get; set; } = TimeProvider.System;

    /// <summary>The code-only poll interval; when set, it wins over <see cref="OutboxOptions.PollIntervalSeconds"/>.</summary>
    public TimeSpan? PollInterval { get; set; }

    /// <summary>Rejects an interval the relay's wait can't take, before it can fault the relay at runtime.</summary>
    public static void ValidatePollInterval(TimeSpan interval)
    {
        if (interval == Timeout.InfiniteTimeSpan)
        {
            throw new ArgumentOutOfRangeException(nameof(interval), interval,
                $"The outbox poll interval can't be infinite. For a wake-driven test, pass a long finite interval such as TimeSpan.FromDays(1); it must be between {MinimumPollInterval} (10 ms) and {MaximumPollInterval}.");
        }

        if (interval < MinimumPollInterval || interval > MaximumPollInterval)
        {
            throw new ArgumentOutOfRangeException(nameof(interval), interval,
                $"The outbox poll interval must be between {MinimumPollInterval} (10 ms) and {MaximumPollInterval} (Task.Delay's maximum, about 49.7 days).");
        }
    }
}

/// <summary>
/// The engine's timing: its clock, the relay's wake signal, and the code-only poll interval when one is set.
/// <c>AddOutboxRelay</c> and <c>AddSqlOutboxClaimStore</c> register it, and DI passes it to the relay and the
/// claim store. Only the engine can build one, so a host's own <see cref="TimeProvider"/> never reaches them.
/// Change it with <c>UseOutboxClock</c> and <c>UseOutboxPollInterval</c>.
/// </summary>
public sealed class OutboxTiming
{
    internal OutboxTiming(OutboxTimingOptions options, OutboxRelaySignal signal)
    {
        Clock = options.Clock;
        Signal = signal;
        PollInterval = options.PollInterval;
    }

    internal TimeProvider Clock { get; }

    internal OutboxRelaySignal Signal { get; }

    internal TimeSpan? PollInterval { get; }
}
