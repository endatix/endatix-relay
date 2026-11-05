using Microsoft.Extensions.Time.Testing;

namespace Endatix.Outbox.Engine.IntegrationTests;

/// <summary>
/// Gives a claimed message back due at the engine clock's "now", advances the clock, and claims again: the
/// message must come back. Shared by the provider suites.
/// </summary>
/// <remarks>
/// The advance is needed on SQL Server. Its timestamps are bound as <c>datetime</c>, rounded to 1/300 s, so a
/// message's stored <c>NextAttemptAt</c> and the claim's <c>@now</c> for the same instant don't reliably compare
/// as equal: without the advance, about 2 claims in 5 missed it. That's by design. Production polls
/// seconds apart, so the engine doesn't bind <c>datetime2</c> (endatix-relay#16, D10).
/// </remarks>
internal static class GiveBackAtNow
{
    private const string Instance = "instance-A";
    private static readonly TimeSpan Lease = TimeSpan.FromMinutes(1);

    /// <summary>More than <c>datetime</c>'s rounding, and far less than any real poll interval.</summary>
    public static readonly TimeSpan Advance = TimeSpan.FromMilliseconds(10);

    public static async Task AssertReclaimedAfterAdvanceAsync(SqlOutboxClaimStore store, FakeTimeProvider clock, long id)
    {
        var claimed = await ClaimAsync(store);
        Assert.Equal([id], claimed.Select(m => m.Id));

        await store.RescheduleAsync(claimed[0], clock.GetUtcNow(), Instance, CancellationToken.None);
        clock.Advance(Advance);

        var reclaimed = await ClaimAsync(store);
        Assert.Equal([id], reclaimed.Select(m => m.Id));
    }

    private static Task<IReadOnlyList<IOutboxMessage>> ClaimAsync(SqlOutboxClaimStore store) =>
        store.ClaimBatchAsync(Instance, Lease, 1, CancellationToken.None);
}
