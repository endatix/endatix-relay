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
    public async Task Ordinary_failure_reschedules_the_row()
    {
        await using var store = new SqlServerOutboxTestStore(fixture.ConnectionString, TimeProvider.System);
        await store.CreateTableAsync();
        await store.InsertAsync(1);

        await RelayTick.RunWithFailingPublisherAsync(store.ClaimStore, new InvalidOperationException("broker down"));

        var row = await store.GetOutcomeAsync(1);
        Assert.Equal(OutboxStatus.Pending, row.Status);
        Assert.Equal(1, row.Attempts);
        Assert.NotNull(row.NextAttemptAt);
    }
}
