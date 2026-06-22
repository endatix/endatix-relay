namespace Endatix.Outbox.Engine;

/// <summary>
/// Tuning for the relay loop. A plain POCO (not tied to Endatix config conventions) so the engine stays
/// host-agnostic; the host binds it (e.g. from an <c>Endatix:Outbox</c> section).
/// </summary>
public sealed class OutboxOptions
{
    /// <summary>Delay between poll ticks.</summary>
    public int PollIntervalSeconds { get; set; } = 5;

    /// <summary>Max rows claimed per tick.</summary>
    public int BatchSize { get; set; } = 50;

    /// <summary>Lease duration for a claimed row; a crashed instance's rows become claimable after this.</summary>
    public int LeaseSeconds { get; set; } = 60;

    /// <summary>Delivery attempts before a row is marked failed.</summary>
    public int MaxAttempts { get; set; } = 8;

    /// <summary>Base of the exponential retry backoff.</summary>
    public int BackoffBaseSeconds { get; set; } = 5;

    /// <summary>Cap on the exponential retry backoff.</summary>
    public int BackoffCapSeconds { get; set; } = 300;

    /// <summary>Convenience accessor for the lease as a <see cref="TimeSpan"/>.</summary>
    public TimeSpan Lease => TimeSpan.FromSeconds(LeaseSeconds);

    /// <summary>Convenience accessor for the poll interval as a <see cref="TimeSpan"/>.</summary>
    public TimeSpan PollInterval => TimeSpan.FromSeconds(PollIntervalSeconds);
}
