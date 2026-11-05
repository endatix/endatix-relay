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
    public async Task Permanent_failure_fails_the_row_on_its_first_attempt_with_the_reason()
    {
        await using var store = new SqlServerOutboxTestStore(fixture.ConnectionString, TimeProvider.System);
        await store.CreateTableAsync();
        await store.InsertAsync(1);

        await RelayTick.RunWithFailingPublisherAsync(
            store.ClaimStore, new OutboxPermanentFailureException("Event type 'form.retired' is retired."));

        var row = await store.GetFailureAsync(1);
        Assert.Equal(OutboxStatus.Failed, row.Status);
        Assert.Equal(1, row.Attempts);
        Assert.Equal("Event type 'form.retired' is retired.", row.FailureReason);
    }

    [SqlServerFact]
    public async Task Ordinary_failure_reschedules_the_row_and_stores_no_reason()
    {
        await using var store = new SqlServerOutboxTestStore(fixture.ConnectionString, TimeProvider.System);
        await store.CreateTableAsync();
        await store.InsertAsync(1);

        await RelayTick.RunWithFailingPublisherAsync(store.ClaimStore, new InvalidOperationException("broker down"));

        var row = await store.GetFailureAsync(1);
        Assert.Equal(OutboxStatus.Pending, row.Status);
        Assert.Equal(1, row.Attempts);
        Assert.Null(row.FailureReason);
    }

    [SqlServerFact]
    public async Task A_reason_at_the_column_length_is_stored_whole()
    {
        await using var store = new SqlServerOutboxTestStore(fixture.ConnectionString, TimeProvider.System);
        await store.CreateTableAsync();
        await store.InsertAsync(1);

        await RelayTick.RunWithFailingPublisherAsync(
            store.ClaimStore, new OutboxPermanentFailureException(new string('x', 5000)));

        var row = await store.GetFailureAsync(1);
        Assert.Equal(OutboxStatus.Failed, row.Status);
        Assert.Equal(OutboxSchema.FailureReasonMaxLength, row.FailureReason!.Length);
    }

    [SqlServerFact]
    public async Task Exhausted_retries_store_the_last_error()
    {
        await using var store = new SqlServerOutboxTestStore(fixture.ConnectionString, TimeProvider.System);
        await store.CreateTableAsync();
        await store.InsertAsync(1, attempts: new OutboxOptions().MaxAttempts - 1);

        await RelayTick.RunWithFailingPublisherAsync(store.ClaimStore, new InvalidOperationException("broker down"));

        var row = await store.GetFailureAsync(1);
        Assert.Equal(OutboxStatus.Failed, row.Status);
        Assert.Equal("System.InvalidOperationException: broker down", row.FailureReason);
    }
}
