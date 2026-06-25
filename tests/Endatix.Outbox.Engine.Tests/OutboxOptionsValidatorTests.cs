using Endatix.Outbox.Engine;

namespace Endatix.Outbox.Engine.Tests;

public class OutboxOptionsValidatorTests
{
    private static bool Validate(Action<OutboxOptions> mutate)
    {
        var options = new OutboxOptions();
        mutate(options);
        return new OutboxOptionsValidator().Validate(null, options).Succeeded;
    }

    [Fact]
    public void Defaults_are_valid()
    {
        Assert.True(Validate(_ => { }));
    }

    public static IEnumerable<object[]> NonPositiveKnobs() =>
    [
        ["PollIntervalSeconds", (Action<OutboxOptions>)(o => o.PollIntervalSeconds = 0)],
        ["BatchSize", (Action<OutboxOptions>)(o => o.BatchSize = 0)],
        ["LeaseSeconds", (Action<OutboxOptions>)(o => o.LeaseSeconds = -1)],
        ["MaxAttempts", (Action<OutboxOptions>)(o => o.MaxAttempts = 0)],
        ["BackoffBaseSeconds", (Action<OutboxOptions>)(o => o.BackoffBaseSeconds = -5)],
    ];

    [Theory]
    [MemberData(nameof(NonPositiveKnobs))]
    public void Nonpositive_tuning_value_fails(string knob, Action<OutboxOptions> mutate)
    {
        Assert.False(Validate(mutate), $"Expected validation to fail for non-positive {knob}.");
    }

    [Fact]
    public void Backoff_cap_below_base_fails_the_cross_field_check()
    {
        var options = new OutboxOptions { BackoffBaseSeconds = 100, BackoffCapSeconds = 10 }; // both in range
        var result = new OutboxOptionsValidator().Validate(null, options);

        Assert.False(result.Succeeded);
        Assert.Contains(result.Failures!, f => f.Contains(nameof(OutboxOptions.BackoffCapSeconds)));
    }

    [Fact]
    public void Backoff_cap_equal_to_base_is_valid()
    {
        Assert.True(Validate(o => { o.BackoffBaseSeconds = 30; o.BackoffCapSeconds = 30; }));
    }
}
