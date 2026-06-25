using System.Text.Json.Nodes;
using Endatix.Outbox.Engine;

namespace Endatix.Outbox.Engine.IntegrationTests;

/// <summary>
/// Real-PostgreSQL tests for <see cref="SqlOutboxClaimStore"/> — the things string assertions can't prove:
/// skip-locked claim, lease semantics, lock-ownership guards, the Payload-as-text projection, and
/// DateTimeOffset persistence.
/// </summary>
[Trait("Category", "Integration")]
[Collection(PostgresCollection.Name)]
public sealed class PostgresClaimStoreTests(PostgresFixture fixture)
{
    private static readonly TimeSpan Lease = TimeSpan.FromMinutes(1);
    private const string InstanceA = "instance-A";
    private const string InstanceB = "instance-B";

    private async Task<OutboxTestStore> NewStoreAsync()
    {
        var store = new OutboxTestStore(fixture.ConnectionString);
        await store.CreateTableAsync();
        return store;
    }

    private static async Task<IReadOnlyList<long>> ClaimIdsAsync(
        OutboxTestStore store, string instanceId, int batchSize = 50)
    {
        var claimed = await store.ClaimStore.ClaimBatchAsync(instanceId, Lease, batchSize, CancellationToken.None);
        return claimed.Select(m => m.Id).ToList();
    }

    [Fact]
    public async Task Claim_returns_only_claimable_rows_in_id_order()
    {
        await using var store = await NewStoreAsync();
        await store.InsertAsync(1);                                                   // claimable
        await store.InsertAsync(2, status: OutboxStatus.Sent);                        // not pending
        await store.InsertAsync(3, lockedUntil: DateTime.UtcNow.AddMinutes(5));       // active lease
        await store.InsertAsync(4, nextAttemptAt: DateTime.UtcNow.AddMinutes(5));     // backoff gate
        await store.InsertAsync(5, lockedUntil: DateTime.UtcNow.AddMinutes(-5));      // expired lease → claimable

        var claimed = await ClaimIdsAsync(store, InstanceA);

        Assert.Equal([1L, 5L], claimed);
    }

    [Fact]
    public async Task Concurrent_claims_are_disjoint_and_cover_every_row()
    {
        await using var store = await NewStoreAsync();
        var ids = Enumerable.Range(1, 20).Select(i => (long)i).ToArray();
        foreach (var id in ids)
        {
            await store.InsertAsync(id);
        }

        var both = await Task.WhenAll(
            ClaimIdsAsync(store, InstanceA, batchSize: 20),
            ClaimIdsAsync(store, InstanceB, batchSize: 20));

        var a = both[0];
        var b = both[1];
        Assert.Empty(a.Intersect(b));                  // no row claimed twice
        Assert.Equal(ids.OrderBy(x => x), a.Concat(b).OrderBy(x => x)); // every row claimed exactly once
    }

    [Fact]
    public async Task Expired_lease_is_reclaimed_by_another_instance()
    {
        await using var store = await NewStoreAsync();
        await store.InsertAsync(1);

        // A claims with an already-expired lease (simulates a crashed instance).
        var firstClaim = await store.ClaimStore.ClaimBatchAsync(InstanceA, TimeSpan.FromMinutes(-1), 50, CancellationToken.None);
        Assert.Equal([1L], firstClaim.Select(m => m.Id));

        var reclaimed = await ClaimIdsAsync(store, InstanceB);

        Assert.Equal([1L], reclaimed);
    }

    [Fact]
    public async Task Mark_by_a_non_owner_is_a_noop()
    {
        await using var store = await NewStoreAsync();
        await store.InsertAsync(1);

        // A claims with an expired lease, then B re-claims the row (B is now the owner).
        var claimedByA = await store.ClaimStore.ClaimBatchAsync(InstanceA, TimeSpan.FromMinutes(-1), 50, CancellationToken.None);
        await ClaimIdsAsync(store, InstanceB);

        // A's mark must not clobber B's claim (lease-ownership guard → 0 rows).
        await store.ClaimStore.MarkSentAsync(claimedByA[0], InstanceA, CancellationToken.None);

        var row = await store.GetAsync(1);
        Assert.Equal(OutboxStatus.Pending, row.Status);
        Assert.Equal(InstanceB, row.LockedBy);
    }

    [Fact]
    public async Task MarkSent_sets_sent_and_releases_lease()
    {
        await using var store = await NewStoreAsync();
        await store.InsertAsync(1);
        var claimed = await store.ClaimStore.ClaimBatchAsync(InstanceA, Lease, 50, CancellationToken.None);

        await store.ClaimStore.MarkSentAsync(claimed[0], InstanceA, CancellationToken.None);

        var row = await store.GetAsync(1);
        Assert.Equal(OutboxStatus.Sent, row.Status);
        Assert.NotNull(row.ProcessedAt);
        Assert.Null(row.LockedBy);
        Assert.Null(row.LockedUntil);
    }

    [Fact]
    public async Task Reschedule_increments_attempts_gates_next_attempt_and_keeps_pending()
    {
        await using var store = await NewStoreAsync();
        await store.InsertAsync(1);
        var claimed = await store.ClaimStore.ClaimBatchAsync(InstanceA, Lease, 50, CancellationToken.None);
        var nextAttempt = DateTimeOffset.UtcNow.AddHours(1);

        await store.ClaimStore.RescheduleAsync(claimed[0], nextAttempt, InstanceA, CancellationToken.None);

        var row = await store.GetAsync(1);
        Assert.Equal(OutboxStatus.Pending, row.Status);
        Assert.Equal(1, row.Attempts);
        Assert.Null(row.LockedBy);
        Assert.NotNull(row.NextAttemptAt);
        // The future backoff gate keeps it out of the next claim.
        Assert.Empty(await ClaimIdsAsync(store, InstanceB));
    }

    [Fact]
    public async Task MarkFailed_sets_failed_and_stops_claiming()
    {
        await using var store = await NewStoreAsync();
        await store.InsertAsync(1);
        var claimed = await store.ClaimStore.ClaimBatchAsync(InstanceA, Lease, 50, CancellationToken.None);

        await store.ClaimStore.MarkFailedAsync(claimed[0], InstanceA, CancellationToken.None);

        var row = await store.GetAsync(1);
        Assert.Equal(OutboxStatus.Failed, row.Status);
        Assert.Equal(1, row.Attempts);
        Assert.Null(row.LockedBy);
        Assert.Empty(await ClaimIdsAsync(store, InstanceB));
    }

    [Fact]
    public async Task Payload_round_trips_through_the_jsonb_text_projection()
    {
        await using var store = await NewStoreAsync();
        const string payload = """{"formId":"123","name":"Test","nested":{"a":1,"b":[1,2,3]}}""";
        await store.InsertAsync(1, payload: payload);

        var claimed = await store.ClaimStore.ClaimBatchAsync(InstanceA, Lease, 50, CancellationToken.None);

        // jsonb normalizes whitespace/key order, so compare semantically, not byte-for-byte.
        Assert.True(JsonNode.DeepEquals(JsonNode.Parse(claimed[0].Payload), JsonNode.Parse(payload)));
    }

    [Fact]
    public async Task OccurredAt_round_trips_as_a_utc_offset()
    {
        await using var store = await NewStoreAsync();
        var occurredAt = new DateTime(2026, 3, 15, 12, 30, 45, DateTimeKind.Utc);
        await store.InsertAsync(1, occurredAt: occurredAt);

        var claimed = await store.ClaimStore.ClaimBatchAsync(InstanceA, Lease, 50, CancellationToken.None);

        Assert.Equal(new DateTimeOffset(occurredAt), claimed[0].OccurredAt);
        Assert.Equal(TimeSpan.Zero, claimed[0].OccurredAt.Offset);
    }
}
