namespace Endatix.Outbox.Engine.IntegrationTests;

/// <summary>Real-SQL Server tests for <see cref="SqlOutboxClaimStore"/>.</summary>
[Trait("Category", "Integration")]
[Collection(SqlServerCollection.Name)]
public sealed class SqlServerClaimStoreTests(SqlServerFixture fixture)
{
    [Fact]
    public async Task Message_given_back_due_now_is_reclaimed_at_once()
    {
        await using var store = new SqlServerOutboxTestStore(fixture.ConnectionString);
        await store.CreateTableAsync();
        await store.InsertAsync(1);

        await DueNowGiveBack.AssertEveryReclaimSucceedsAsync(store.ClaimStore, 1);
    }
}
