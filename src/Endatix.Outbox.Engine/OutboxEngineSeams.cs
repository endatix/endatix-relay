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
