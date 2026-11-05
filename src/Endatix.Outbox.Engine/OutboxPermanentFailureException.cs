namespace Endatix.Outbox.Engine;

/// <summary>
/// Thrown by an <see cref="IIntegrationEventPublisher"/> for a message that can never be delivered, such as a row
/// of a retired event type. The relay marks the row failed at once, with no further attempts, and stores
/// <see cref="Exception.Message"/> as its <see cref="OutboxSchema.FailureReason"/>. Any other exception keeps the
/// usual backoff and retry budget, and so does this one when it reaches the relay wrapped in another exception.
/// </summary>
public class OutboxPermanentFailureException : Exception
{
    /// <summary>Creates the signal with the reason the relay stores on the row.</summary>
    public OutboxPermanentFailureException(string reason)
        : base(reason)
    {
    }

    /// <summary>Creates the signal with the reason the relay stores on the row, and the error that caused it.</summary>
    public OutboxPermanentFailureException(string reason, Exception? innerException)
        : base(reason, innerException)
    {
    }
}
