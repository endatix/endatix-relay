using Endatix.Outbox.Engine;

namespace Endatix.Outbox.Engine.Tests;

public class SqlOutboxClaimStoreTests
{
    [Fact]
    public void ToUtc_passes_utc_through_unchanged()
    {
        var utc = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);

        var result = SqlOutboxClaimStore.ToUtc(utc);

        Assert.Equal(DateTimeKind.Utc, result.Kind);
        Assert.Equal(utc, result);
    }

    [Fact]
    public void ToUtc_converts_local_to_utc()
    {
        var local = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Local);

        var result = SqlOutboxClaimStore.ToUtc(local);

        Assert.Equal(DateTimeKind.Utc, result.Kind);
        Assert.Equal(local.ToUniversalTime(), result);
    }

    [Fact]
    public void ToUtc_stamps_unspecified_as_utc_without_shifting()
    {
        var unspecified = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Unspecified);

        var result = SqlOutboxClaimStore.ToUtc(unspecified);

        Assert.Equal(DateTimeKind.Utc, result.Kind);
        Assert.Equal(unspecified.Ticks, result.Ticks); // same wall-clock, just stamped UTC
    }
}
