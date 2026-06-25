using System.ComponentModel.DataAnnotations;

namespace Endatix.Outbox.Engine;

/// <summary>
/// Tuning for the relay loop. A plain POCO (not tied to Endatix config conventions) so the engine stays
/// host-agnostic; the host binds it (e.g. from an <c>Endatix:Outbox</c> section). Values are validated at
/// startup (see <c>AddOutboxRelay</c>) so misconfiguration fails fast instead of producing a tight-loop or
/// a relay that never delivers.
/// </summary>
public sealed class OutboxOptions : IValidatableObject
{
    /// <summary>Delay between poll ticks.</summary>
    [Range(1, int.MaxValue)]
    public int PollIntervalSeconds { get; set; } = 5;

    /// <summary>Max rows claimed per tick.</summary>
    [Range(1, int.MaxValue)]
    public int BatchSize { get; set; } = 50;

    /// <summary>Lease duration for a claimed row; a crashed instance's rows become claimable after this.</summary>
    [Range(1, int.MaxValue)]
    public int LeaseSeconds { get; set; } = 60;

    /// <summary>Delivery attempts before a row is marked failed.</summary>
    [Range(1, int.MaxValue)]
    public int MaxAttempts { get; set; } = 8;

    /// <summary>Base of the exponential retry backoff.</summary>
    [Range(1, int.MaxValue)]
    public int BackoffBaseSeconds { get; set; } = 5;

    /// <summary>Cap on the exponential retry backoff.</summary>
    [Range(1, int.MaxValue)]
    public int BackoffCapSeconds { get; set; } = 300;

    /// <summary>Convenience accessor for the lease as a <see cref="TimeSpan"/>.</summary>
    public TimeSpan Lease => TimeSpan.FromSeconds(LeaseSeconds);

    /// <summary>Convenience accessor for the poll interval as a <see cref="TimeSpan"/>.</summary>
    public TimeSpan PollInterval => TimeSpan.FromSeconds(PollIntervalSeconds);

    /// <inheritdoc />
    public IEnumerable<ValidationResult> Validate(ValidationContext validationContext)
    {
        if (BackoffCapSeconds < BackoffBaseSeconds)
        {
            yield return new ValidationResult(
                $"{nameof(BackoffCapSeconds)} ({BackoffCapSeconds}) must be >= {nameof(BackoffBaseSeconds)} ({BackoffBaseSeconds}).",
                [nameof(BackoffCapSeconds), nameof(BackoffBaseSeconds)]);
        }
    }
}
