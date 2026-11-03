namespace Endatix.Outbox.Engine.IntegrationTests;

/// <summary>
/// Claims one message, gives it back due now, and claims again at once, over and over. Every reclaim must
/// return the message: a give-back due now is due to the very next claim. Shared by the provider suites.
/// </summary>
internal static class DueNowGiveBack
{
    // Cheap enough for CI; on SQL Server a datetime-bound timestamp misses roughly 1 reclaim in 10, so
    // 200 rounds catch it. Set OUTBOX_DUE_NOW_ITERATIONS for a longer proof run.
    private const int DefaultIterations = 200;

    private static int Iterations =>
        int.TryParse(Environment.GetEnvironmentVariable("OUTBOX_DUE_NOW_ITERATIONS"), out var n) && n > 0
            ? n
            : DefaultIterations;

    private const string Instance = "instance-A";
    private static readonly TimeSpan Lease = TimeSpan.FromMinutes(1);

    // Far above datetime's 1/300 s rounding: a message still unclaimable after this is lost, not late.
    private static readonly TimeSpan MaxWait = TimeSpan.FromSeconds(1);

    public static async Task AssertEveryReclaimSucceedsAsync(SqlOutboxClaimStore store, long id)
    {
        var iterations = Iterations;

        var missed = await CountMissedReclaimsAsync(store, id, iterations);

        Assert.True(missed == 0, $"{missed}/{iterations} reclaims came back empty.");
    }

    private static async Task<int> CountMissedReclaimsAsync(SqlOutboxClaimStore store, long id, int iterations)
    {
        var missed = 0;
        var claimed = await ClaimAsync(store);
        Assert.Equal([id], claimed.Select(m => m.Id));

        for (var i = 0; i < iterations; i++)
        {
            await store.RescheduleAsync(claimed[0], DateTimeOffset.UtcNow, Instance, CancellationToken.None);
            claimed = await ClaimAsync(store);
            if (claimed.Count == 0)
            {
                missed++;
                claimed = await WaitForClaimAsync(store, round: i);
            }
        }

        return missed;
    }

    private static Task<IReadOnlyList<IOutboxMessage>> ClaimAsync(SqlOutboxClaimStore store) =>
        store.ClaimBatchAsync(Instance, Lease, 1, CancellationToken.None);

    // Waits out the rounding so the next round starts from a claimed message.
    private static async Task<IReadOnlyList<IOutboxMessage>> WaitForClaimAsync(SqlOutboxClaimStore store, int round)
    {
        var deadline = DateTime.UtcNow + MaxWait;
        IReadOnlyList<IOutboxMessage> claimed;
        do
        {
            Assert.True(DateTime.UtcNow < deadline, $"Round {round}: the message given back was not claimable within {MaxWait}.");
            await Task.Delay(5);
            claimed = await ClaimAsync(store);
        }
        while (claimed.Count == 0);

        return claimed;
    }
}
