namespace Endatix.Outbox.Engine;

/// <summary>
/// Delivers a claimed outbox message to its destination. The relay loop is agnostic to the transport:
/// the in-process host supplies a webhook publisher; a standalone worker supplies a DAPR publisher.
/// </summary>
public interface IIntegrationEventPublisher
{
    /// <summary>Publishes the message. Throwing signals failure; the relay then reschedules or fails it.</summary>
    Task PublishAsync(IOutboxMessage message, CancellationToken cancellationToken);
}
