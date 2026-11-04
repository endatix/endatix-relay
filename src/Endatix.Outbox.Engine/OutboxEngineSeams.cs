namespace Endatix.Outbox.Engine;

/// <summary>
/// The engine's own clock. Registered by the engine with <see cref="TimeProvider.System"/>; only
/// <c>UseOutboxClock</c> replaces it, so a <see cref="TimeProvider"/> the host registers for other reasons
/// doesn't reach the relay.
/// </summary>
internal sealed record OutboxClock(TimeProvider Provider)
{
    public static OutboxClock System { get; } = new(TimeProvider.System);
}

/// <summary>
/// A poll interval set in code by <c>UseOutboxPollInterval</c>. A separate type, so the configuration binder
/// never sees it: no config key can set an interval under a second. When present, it wins over
/// <see cref="OutboxOptions.PollIntervalSeconds"/>.
/// </summary>
internal sealed record OutboxPollInterval(TimeSpan Value)
{
    /// <summary>The smallest code-only interval. Speed comes from the clock and the wake, not a tighter loop.</summary>
    public static readonly TimeSpan Minimum = TimeSpan.FromMilliseconds(10);
}

/// <summary>
/// The engine's timing: its clock, the relay's wake signal, and the code-only poll interval when one is set.
/// <c>AddOutboxRelay</c> and <c>AddSqlOutboxClaimStore</c> register it, and DI passes it to the relay and the
/// claim store. Only the engine can build one, so a host's own <see cref="TimeProvider"/> never reaches them.
/// Change it with <c>UseOutboxClock</c> and <c>UseOutboxPollInterval</c>.
/// </summary>
public sealed class OutboxTiming
{
    internal OutboxTiming(TimeProvider clock, OutboxRelaySignal signal, TimeSpan? pollInterval)
    {
        Clock = clock;
        Signal = signal;
        PollInterval = pollInterval;
    }

    internal TimeProvider Clock { get; }

    internal OutboxRelaySignal Signal { get; }

    internal TimeSpan? PollInterval { get; }
}
