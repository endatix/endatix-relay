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
}
