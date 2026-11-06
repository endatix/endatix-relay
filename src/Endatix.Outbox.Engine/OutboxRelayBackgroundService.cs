using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Endatix.Outbox.Engine;

/// <summary>
/// The outbox relay loop: each tick it (optionally) claims a batch of pending rows, publishes each, and
/// marks the outcome. The same class runs in-process (Endatix API) and in a standalone worker — only the
/// registered <see cref="IIntegrationEventPublisher"/> differs.
/// </summary>
/// <remarks>
/// <para><b>Scope per tick.</b> A <see cref="BackgroundService"/> is a singleton, but the claim store,
/// publisher, and gate are scoped — so each tick opens an <see cref="IServiceScope"/>.</para>
/// <para><b>Gated, not stopped.</b> The relay is gated by <see cref="IOutboxRelayGate"/> evaluated every
/// tick. When disabled it idles (rows stay pending) and resumes on the next tick once re-enabled — no
/// restart, so the standalone cutover is a flag flip.</para>
/// <para><b>Concurrency-safe.</b> The DB-arbitrated claim + lease let any number of instances run; the
/// residual "published then crashed before mark" case re-publishes on reclaim (at-least-once).</para>
/// </remarks>
public class OutboxRelayBackgroundService : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly OutboxOptions _options;
    private readonly ILogger<OutboxRelayBackgroundService> _logger;
    private readonly OutboxRelaySignal _signal;
    private readonly string _instanceId = $"{Environment.MachineName}:{Environment.ProcessId}";

    private bool? _lastEnabled;

    /// <summary>Creates the relay on <see cref="TimeProvider.System"/>, polling every <see cref="OutboxOptions.PollIntervalSeconds"/>.</summary>
    /// <remarks>
    /// Kept for existing callers. A relay built with it doesn't take part in the engine's timing: it ignores
    /// <c>UseOutboxClock</c> and <c>UseOutboxPollInterval</c>, and it waits on a private signal, so
    /// <see cref="IOutboxRelaySignal.Wake"/> from DI never reaches it. <c>AddOutboxRelay</c> uses the constructor
    /// that takes <see cref="OutboxTiming"/>.
    /// </remarks>
    public OutboxRelayBackgroundService(
        IServiceScopeFactory scopeFactory,
        IOptions<OutboxOptions> options,
        ILogger<OutboxRelayBackgroundService> logger)
        : this(scopeFactory, options, logger, new OutboxTiming(new OutboxTimingOptions(), new OutboxRelaySignal()))
    {
    }

    /// <summary>
    /// Creates the relay on the engine's timing: its clock, its wake signal, and the code-only poll interval,
    /// which wins over <see cref="OutboxOptions.PollIntervalSeconds"/>. DI uses this one once
    /// <c>AddOutboxRelay</c> has registered <see cref="OutboxTiming"/>.
    /// </summary>
    public OutboxRelayBackgroundService(
        IServiceScopeFactory scopeFactory,
        IOptions<OutboxOptions> options,
        ILogger<OutboxRelayBackgroundService> logger,
        OutboxTiming timing)
    {
        _scopeFactory = scopeFactory;
        _options = options.Value;
        _logger = logger;
        _signal = timing.Signal;
        Clock = timing.Clock;
        PollInterval = timing.PollInterval ?? _options.PollInterval;
    }

    /// <summary>The clock the relay waits on and computes backoff from.</summary>
    internal TimeProvider Clock { get; }

    /// <summary>The wait between polls: the code-only interval when set, else <see cref="OutboxOptions.PollIntervalSeconds"/>.</summary>
    internal TimeSpan PollInterval { get; }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        LogLifecycle("started");
        WarnAboutNonDefaultTiming();

        // Fail fast on DI misconfiguration: resolve the required scoped services once at startup so a missing
        // registration crashes the host instead of being swallowed by the per-tick retry catch below (which
        // would leave the host "healthy" while the relay can never process a single message).
        await using (var startupScope = _scopeFactory.CreateAsyncScope())
        {
            EnsureRequiredServices(startupScope.ServiceProvider);
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            var processed = 0;
            try
            {
                await using var scope = _scopeFactory.CreateAsyncScope();
                processed = await ProcessOnceAsync(scope.ServiceProvider, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Outbox relay tick failed; will retry next poll.");
            }

            // Drain: a full batch means more rows are likely ready now — loop immediately instead of
            // waiting a full poll interval (otherwise throughput caps at BatchSize / PollInterval). Only
            // delay when the tick caught up (partial batch), was idle, or gated off (all return < BatchSize).
            if (processed >= _options.BatchSize)
            {
                continue;
            }

            await _signal.WaitAsync(PollInterval, Clock, stoppingToken);
        }

        LogLifecycle("stopping");
    }

    /// <summary>
    /// Runs a single relay tick against the given scope: evaluates the gate, and if enabled, claims and
    /// processes one batch. Returns the number of rows processed (0 when gated off or nothing pending).
    /// Exposed for unit testing without the <see cref="ExecuteAsync"/> loop.
    /// </summary>
    internal async Task<int> ProcessOnceAsync(IServiceProvider services, CancellationToken cancellationToken)
    {
        var gate = services.GetRequiredService<IOutboxRelayGate>();
        var enabled = await gate.IsRelayEnabledAsync(cancellationToken);

        if (_lastEnabled != enabled)
        {
            LogGateChange(enabled);
            _lastEnabled = enabled;
        }

        if (!enabled)
        {
            return 0;
        }

        return await ProcessBatchAsync(services, cancellationToken);
    }

    /// <summary>
    /// Resolves every scoped service the relay needs, so a missing/misconfigured registration throws at
    /// startup rather than per tick. Called once before the loop; exposed for unit testing.
    /// </summary>
    internal static void EnsureRequiredServices(IServiceProvider services)
    {
        services.GetRequiredService<IOutboxRelayGate>();
        services.GetRequiredService<IOutboxClaimStore>();
        services.GetRequiredService<IIntegrationEventPublisher>();
    }

    private async Task<int> ProcessBatchAsync(IServiceProvider services, CancellationToken cancellationToken)
    {
        var claimStore = services.GetRequiredService<IOutboxClaimStore>();
        var publisher = services.GetRequiredService<IIntegrationEventPublisher>();

        var batch = await claimStore.ClaimBatchAsync(
            _instanceId, _options.Lease, _options.BatchSize, cancellationToken);

        foreach (var message in batch)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var publishSucceeded = false;
            try
            {
                await publisher.PublishAsync(message, cancellationToken);
                publishSucceeded = true;
            }
            catch (Exception ex) when (!IsCancellation(ex, cancellationToken))
            {
                await HandlePublishFailureAsync(claimStore, message, ex, cancellationToken);
            }

            if (publishSucceeded)
            {
                await MarkSentOrAbortTickAsync(claimStore, message, cancellationToken);
            }
        }

        return batch.Count;
    }

    private async Task HandlePublishFailureAsync(
        IOutboxClaimStore claimStore, IOutboxMessage message, Exception ex, CancellationToken cancellationToken)
    {
        // Log "failed permanently" only once the row is marked: if the mark throws, the row stays pending.
        if (ex is OutboxPermanentFailureException permanent)
        {
            await claimStore.MarkFailedAsync(message, _instanceId, cancellationToken);
            _logger.LogError(
                ex, "Outbox message {MessageId} ({EventType}) failed permanently on attempt {Attempt}: {Reason}",
                message.Id, message.EventType, message.Attempts + 1, permanent.Message);
        }
        else if (message.Attempts + 1 >= _options.MaxAttempts)
        {
            await claimStore.MarkFailedAsync(message, _instanceId, cancellationToken);
            _logger.LogError(
                ex, "Outbox message {MessageId} ({EventType}) failed permanently after {Attempts} attempts.",
                message.Id, message.EventType, message.Attempts + 1);
        }
        else
        {
            var nextAttemptAt = ComputeNextAttempt(message.Attempts, Clock.GetUtcNow(), _options);
            _logger.LogWarning(
                ex, "Outbox message {MessageId} ({EventType}) publish failed (attempt {Attempt}); retry at {NextAttemptAt:o}.",
                message.Id, message.EventType, message.Attempts + 1, nextAttemptAt);
            await claimStore.RescheduleAsync(message, nextAttemptAt, _instanceId, cancellationToken);
        }
    }

    private void LogLifecycle(string phase)
    {
        if (_logger.IsEnabled(LogLevel.Information))
        {
            _logger.LogInformation("Outbox relay {Phase} (instance {InstanceId}).", phase, _instanceId);
        }
    }

    // Both settings are meant for tests, so a production host that carries one by mistake says so at startup.
    private void WarnAboutNonDefaultTiming()
    {
        if (Clock != TimeProvider.System)
        {
            _logger.LogWarning(
                "Outbox relay runs on the clock {ClockType}, not TimeProvider.System. This is meant for tests: if it runs ahead of real time, the relay claims early and treats other instances' leases as expired.",
                Clock.GetType().Name);
        }

        if (PollInterval < TimeSpan.FromSeconds(1))
        {
            _logger.LogWarning(
                "Outbox relay poll interval is {PollIntervalMs} ms, under a second. This is meant for tests: every poll runs a claim query.",
                PollInterval.TotalMilliseconds);
        }
    }

    private void LogGateChange(bool enabled)
    {
        if (_logger.IsEnabled(LogLevel.Information))
        {
            _logger.LogInformation(
                "Outbox relay {State} (flag '{Flag}').",
                enabled ? "enabled" : "disabled", OutboxFlags.RelayInProcess);
        }
    }

    // Marks a published row sent. A MarkSent failure must NOT be routed to HandlePublishFailureAsync — that
    // would re-publish a delivered message or, at MaxAttempts, wrongly dead-letter it. Instead abort the tick
    // by throwing (the ExecuteAsync loop logs it): a failing mark usually means the store is unhealthy, so
    // continuing would publish the rest of the batch and redeliver all of them on lease expiry (a duplicate storm). The already-published row
    // redelivers after its lease expires (at-least-once); the remaining claimed rows were never published.
    private async Task MarkSentOrAbortTickAsync(
        IOutboxClaimStore claimStore, IOutboxMessage message, CancellationToken cancellationToken)
    {
        try
        {
            await claimStore.MarkSentAsync(message, _instanceId, cancellationToken);
        }
        catch (Exception ex) when (!IsCancellation(ex, cancellationToken))
        {
            throw new InvalidOperationException(
                $"Outbox message {message.Id} ({message.EventType}) published but MarkSent failed; aborting the tick (it redelivers after lease expiry).",
                ex);
        }
    }

    // True when the exception is cooperative cancellation of this loop — it must propagate (abort the loop)
    // rather than be treated as a publish/mark failure. Used as an exception filter so cancellation is never
    // caught as a delivery error.
    private static bool IsCancellation(Exception ex, CancellationToken cancellationToken) =>
        ex is OperationCanceledException && cancellationToken.IsCancellationRequested;

    /// <summary>
    /// Exponential backoff with a cap: <c>now + min(cap, base * 2^attempts)</c>. <paramref name="attempts"/>
    /// is the count before the just-failed attempt. Pure/static so it can be unit-tested deterministically.
    /// </summary>
    internal static DateTimeOffset ComputeNextAttempt(int attempts, DateTimeOffset utcNow, OutboxOptions options)
    {
        var exponent = Math.Min(attempts, 30); // guard against overflow on pathological attempt counts
        var seconds = options.BackoffBaseSeconds * Math.Pow(2, exponent);
        var capped = Math.Min(seconds, options.BackoffCapSeconds);
        return utcNow.AddSeconds(capped);
    }
}
