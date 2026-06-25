using System.ComponentModel.DataAnnotations;
using Microsoft.Extensions.Options;

namespace Endatix.Outbox.Engine;

/// <summary>
/// Validates <see cref="OutboxOptions"/> via its DataAnnotations (and <see cref="IValidatableObject"/>).
/// Registered by <c>AddOutboxRelay</c>; runs when the options are first materialized — which the relay does
/// at startup (its constructor reads <c>IOptions&lt;OutboxOptions&gt;.Value</c>), so bad tuning fails fast.
/// </summary>
internal sealed class OutboxOptionsValidator : IValidateOptions<OutboxOptions>
{
    public ValidateOptionsResult Validate(string? name, OutboxOptions options)
    {
        var results = new List<ValidationResult>();
        if (Validator.TryValidateObject(options, new ValidationContext(options), results, validateAllProperties: true))
        {
            return ValidateOptionsResult.Success;
        }

        var failures = results.Select(r => r.ErrorMessage ?? "Invalid OutboxOptions value.").ToList();
        return ValidateOptionsResult.Fail(failures);
    }
}
