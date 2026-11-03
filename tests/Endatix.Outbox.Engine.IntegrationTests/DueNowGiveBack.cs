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

    public static int Iterations =>
        int.TryParse(Environment.GetEnvironmentVariable("OUTBOX_DUE_NOW_ITERATIONS"), out var n) && n > 0
            ? n
            : DefaultIterations;

    public static async Task<int> CountMissedReclaimsAsync(SqlOutboxClaimStore store, long id, int iterations)
    {
        const string instance = "instance-A";
        var lease = TimeSpan.FromMinutes(1);
        var missed = 0;

        var claimed = await store.ClaimBatchAsync(instance, lease, 1, CancellationToken.None);
        Assert.Equal([id], claimed.Select(m => m.Id));

        for (var i = 0; i < iterations; i++)
        {
            await store.RescheduleAsync(claimed[0], DateTimeOffset.UtcNow, instance, CancellationToken.None);
            claimed = await store.ClaimBatchAsync(instance, lease, 1, CancellationToken.None);
            if (claimed.Count == 0)
            {
                missed++;
                // Wait out the rounding so the next round starts from a claimed message.
                do
                {
                    await Task.Delay(5);
                    claimed = await store.ClaimBatchAsync(instance, lease, 1, CancellationToken.None);
                }
                while (claimed.Count == 0);
            }
        }

        return missed;
    }
}
