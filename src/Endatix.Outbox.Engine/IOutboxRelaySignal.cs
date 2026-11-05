namespace Endatix.Outbox.Engine;

/// <summary>
/// Wakes the relay so it polls now instead of at the end of its interval. Meant for tests: write a message,
/// call <see cref="Wake"/>, and it is delivered without waiting out a poll. Production never needs it; if
/// it is called there, the only effect is an early poll.
/// </summary>
public interface IOutboxRelaySignal
{
    /// <summary>
    /// Ends the current wait between polls. A wake that arrives while a poll is running is remembered, and
    /// the next poll starts as soon as that one finishes; several wakes during one poll cause one extra poll.
    /// </summary>
    void Wake();
}
