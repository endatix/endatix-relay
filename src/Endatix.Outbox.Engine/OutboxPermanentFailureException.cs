namespace Endatix.Outbox.Engine;

/// <summary>
/// Thrown by an <see cref="IIntegrationEventPublisher"/>, or by a handler it calls, for a message that can never be
/// delivered, such as a row of a retired event type. The relay marks the row failed at once, with no further
/// attempts, and logs <see cref="Exception.Message"/> as the reason. Any other exception keeps the usual backoff and
/// retry budget, and so does this one when it reaches the relay wrapped in another exception, so a publisher that
/// wraps its handlers' errors must let this one through unwrapped.
/// Derive from this type to signal your own never-retry failure; the relay treats derived types the same way.
/// </summary>
public class OutboxPermanentFailureException : Exception
{
    /// <summary>Creates the signal with the reason the relay logs.</summary>
    public OutboxPermanentFailureException(string reason)
        : base(reason)
    {
    }

    /// <summary>Creates the signal with the reason the relay logs, and the error that caused it.</summary>
    public OutboxPermanentFailureException(string reason, Exception? innerException)
        : base(reason, innerException)
    {
    }
}
