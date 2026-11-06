using Microsoft.Extensions.Time.Testing;

namespace Endatix.Outbox.Engine.IntegrationTests;

/// <summary>Real-SQL Server tests for <see cref="SqlOutboxClaimStore"/>. x64 only (see <see cref="SqlServerFixture"/>).</summary>
[Trait("Category", "Integration")]
[Trait("Provider", "SqlServer")]
[Collection(SqlServerCollection.Name)]
public sealed class SqlServerClaimStoreTests(SqlServerFixture fixture)
{
    [SqlServerFact]
    public async Task Message_given_back_at_now_is_reclaimed_once_the_clock_advances()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        await using var store = new SqlServerOutboxTestStore(fixture.ConnectionString, clock);
        await store.CreateTableAsync();
        await store.InsertAsync(1);

        await GiveBackAtNow.AssertReclaimedAfterAdvanceAsync(store.ClaimStore, clock, 1);
    }

    [SqlServerFact]
    public async Task Permanent_failure_fails_the_row_on_its_first_attempt()
    {
        await using var store = new SqlServerOutboxTestStore(fixture.ConnectionString, TimeProvider.System);
        await store.CreateTableAsync();
        await store.InsertAsync(1);

        await RelayTick.RunWithFailingPublisherAsync(
            store.ClaimStore, new OutboxPermanentFailureException("Event type 'form.retired' is retired."));

        var row = await store.GetOutcomeAsync(1);
        Assert.Equal(OutboxStatus.Failed, row.Status);
        Assert.Equal(1, row.Attempts);
        Assert.Null(row.LockedBy);
    }

    [SqlServerFact]
    public async Task Ordinary_failure_reschedules_the_row_behind_its_backoff()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        await using var store = new SqlServerOutboxTestStore(fixture.ConnectionString, clock);
        await store.CreateTableAsync();
        await store.InsertAsync(1);
        var firstBackoff = TimeSpan.FromSeconds(new OutboxOptions().BackoffBaseSeconds);

        await RelayTick.RunWithFailingPublisherAsync(
            store.ClaimStore, new InvalidOperationException("broker down"), clock);

        var row = await store.GetOutcomeAsync(1);
        Assert.Equal(OutboxStatus.Pending, row.Status);
        Assert.Equal(1, row.Attempts);
        Assert.NotNull(row.NextAttemptAt);
        Assert.Empty(await ClaimAsync(store));

        // Past the backoff by more than datetime's 1/300 s rounding (see GiveBackAtNow).
        clock.Advance(firstBackoff + GiveBackAtNow.Advance);
        Assert.Equal([1L], (await ClaimAsync(store)).Select(m => m.Id));
    }

    private static Task<IReadOnlyList<IOutboxMessage>> ClaimAsync(SqlServerOutboxTestStore store) =>
        store.ClaimStore.ClaimBatchAsync("instance-B", TimeSpan.FromMinutes(1), 1, CancellationToken.None);
}
