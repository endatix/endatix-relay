namespace Endatix.Outbox.Engine.Tests;

public class OutboxClaimStoreDefaultsTests
{
    [Fact]
    public async Task A_store_without_the_reason_overload_still_marks_the_row_failed()
    {
        var store = new StoreWithoutReason();
        var message = new FakeOutboxMessage(3);

        await ((IOutboxClaimStore)store).MarkFailedAsync(message, "retired", "instance-A", CancellationToken.None);

        Assert.Equal((3L, "instance-A"), store.MarkedFailed);
    }

    // A host claim store written against 1.1, which has only the three-argument MarkFailedAsync.
    private sealed class StoreWithoutReason : IOutboxClaimStore
    {
        public (long Id, string InstanceId)? MarkedFailed { get; private set; }

        public Task<IReadOnlyList<IOutboxMessage>> ClaimBatchAsync(
            string instanceId, TimeSpan lease, int batchSize, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<IOutboxMessage>>([]);

        public Task MarkSentAsync(IOutboxMessage message, string instanceId, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public Task RescheduleAsync(
            IOutboxMessage message, DateTimeOffset nextAttemptAt, string instanceId, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public Task MarkFailedAsync(IOutboxMessage message, string instanceId, CancellationToken cancellationToken)
        {
            MarkedFailed = (message.Id, instanceId);
            return Task.CompletedTask;
        }
    }
}
