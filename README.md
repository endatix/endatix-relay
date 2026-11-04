[![Quality gate status](https://sonarcloud.io/api/project_badges/measure?project=endatix_endatix-relay&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=endatix_endatix-relay)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=endatix_endatix-relay&metric=coverage)](https://sonarcloud.io/summary/new_code?id=endatix_endatix-relay)

# Endatix.Outbox.Engine

Endatix-agnostic **transactional-outbox relay engine**. This is the foundational package depended on by
*both* the Endatix platform (in-process relay) and a standalone relay worker — it references neither, so it
sits below both in the dependency graph.

## What it provides

- **Contracts:**
  - `IOutboxMessage` — read shape of an outbox row.
  - `IOutboxClaimStore` — DB-arbitrated claim (`SKIP LOCKED`/`READPAST` + lease) + mark sent/reschedule/failed.
  - `IIntegrationEventPublisher` — delivers a claimed message (webhook in-process, DAPR in the worker).
- **`OutboxRelayBackgroundService`** — the `claim → publish → mark` loop. Scope-per-tick, exponential
  backoff, lease-based crash recovery, at-least-once.
- **OpenFeature gate** — `IOutboxRelayGate` / `OpenFeatureOutboxRelayGate` evaluate the
  `outbox-relay-in-process` flag (`OutboxFlags.RelayInProcess`, default `true`) every tick. Flip it to
  hand the relay over to a standalone worker — no restart with a dynamic provider.
- **Database layer (the shared DB logic):** `SqlOutboxClaimStore` — provider-agnostic ADO.NET implementation
  of `IOutboxClaimStore` over a host-supplied connection. The dialect difference is just the skip-locked
  clause + identifier quoting (`OutboxSqlDialect`). `OutboxSchema` / `OutboxStatus` are the **canonical
  storage contract** (column names + status ints) — the host's EF mapping references the same constants so
  the SQL and the schema can't silently drift.
- **`AddOutboxRelay()` / `AddSqlOutboxClaimStore(dialect, connectionFactory, tableName?)`** — register the
  loop + default gate, and the SQL claim store.

## What the host must supply

`AddOutboxRelay()` does **not** register an `IIntegrationEventPublisher` or an OpenFeature provider (which
supplies `IFeatureClient`) — the host wires those (webhook vs DAPR; in-memory vs flagd). For the database,
the host calls `AddSqlOutboxClaimStore(...)` with its **dialect**, a **connection factory** (e.g.
`_ => new NpgsqlConnection(connString)`), and optionally the **table name** (both hosts must agree). The host
also owns the table's existence (migration) and EF mapping for the capture/write side.

## Testing a host: clock, poll interval, and wake

Delivery is eventually consistent: in production the relay polls every `PollIntervalSeconds` (default 5 s,
minimum 1 s), and nothing here changes that. Tests of the relay, or of an app that hosts it, can make delivery
immediate and deterministic with three seams. All three are meant for tests, and the relay logs a warning at
startup when it runs on a non-default clock or a sub-second interval.

```csharp
var clock = new FakeTimeProvider(DateTimeOffset.UtcNow); // Microsoft.Extensions.TimeProvider.Testing

services.AddSingleton<TimeProvider>(clock);              // the host's own code, which writes NextAttemptAt;
                                                       // the engine ignores this registration
services.AddOutboxRelay(...)
    .UseOutboxClock(clock)                                // the engine's clock: the same instance
    .UseOutboxPollInterval(TimeSpan.FromMilliseconds(50)); // code-only, 10 ms minimum

// after writing an outbox row:
provider.GetRequiredService<IOutboxRelaySignal>().Wake();
```

- **`UseOutboxClock(TimeProvider)`** replaces the engine's clock, which defaults to `TimeProvider.System`. The
  claim's "now", the lease, the backoff, and the wait between polls all read it, so advancing a fake clock
  runs the next poll with no real wait. Only this call changes it: a `TimeProvider` the host registers in DI
  for other reasons doesn't, because a clock that runs ahead claims early and treats other instances'
  leases as expired.
- **`UseOutboxPollInterval(TimeSpan)`** sets the interval in code, from 10 ms up to `Task.Delay`'s maximum
  (about 49.7 days; `PollIntervalSeconds` has the same ceiling). It wins over `PollIntervalSeconds`. No
  configuration key, environment variables included, can set an interval under a second. The interval must be
  finite: a test driven only by `Wake()` can pass a long one such as `TimeSpan.FromDays(1)`.
- **`IOutboxRelaySignal.Wake()`** ends the current wait, so the next poll runs at once. A wake that arrives
  during a poll is remembered: the next poll starts as soon as that one finishes, and several wakes during one
  poll cause one extra poll.

**Never let the engine's clock run behind the host's.** The host writes `NextAttemptAt` with its own clock; if
the engine's clock is behind it, due messages look like they're in the future. Use the same clock for both,
or only move the engine's forward.

**SQL Server rounds timestamps to 1/300 s.** The claim store binds timestamps as `datetime`, so a message's
stored `NextAttemptAt` and a claim's "now" for the same instant don't reliably compare as equal. Between a
give-back and the next claim, advance the clock by more than 1/300 s (the integration suite uses 10 ms) rather
than claiming at the same instant.

## Dependencies

`Microsoft.Extensions.*` abstractions (Hosting/DI/Options/Logging) + `OpenFeature`, plus `System.Data.Common`
(in-runtime — the claim store is raw ADO.NET). **No EF, no provider package, no Endatix, no DAPR.**

## Build & test

```bash
dotnet test --filter "Category!=Integration"   # unit
dotnet test --filter "Category=Integration"    # real databases via Testcontainers (needs Docker)
```

The integration suite runs on PostgreSQL and SQL Server. The SQL Server image is amd64-only, so its tests
(trait `Provider=SqlServer`) skip on Arm64; CI runs both on x64.

## CI / Release (trunk-based)

A single workflow, [`.github/workflows/ci.yml`](.github/workflows/ci.yml):

- **Every PR to `main` (and each update):** restore → build → test.
- **On merge to `main`:** the same build + test, then a **release** step (gated by
  `if: github.ref_name == 'main'`) driven by [semantic-release](https://github.com/semantic-release/semantic-release).

**Versioning is automatic** from [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/):
semantic-release analyzes the commits merged to `main`, computes the next SemVer, tags it (`vX.Y.Z`), creates
a GitHub Release with generated notes, then ([`.releaserc.yaml`](.releaserc.yaml) via `@semantic-release/exec`)
runs `dotnet pack -p:Version=<computed>` and `dotnet nuget push` to nuget.org.

- `fix:` → patch · `feat:` → minor · `feat!:` / `BREAKING CHANGE:` → major · `chore/docs/refactor/...` → no release.
- Use Conventional Commit messages (or squash-merge with a Conventional PR title).

**One-time setup:**
- The job's `GITHUB_TOKEN` stays **read-only**. Release writes (tags/releases/PR comments) come from a
  short-lived **GitHub App** installation token minted only on `main` — so PRs never carry write scopes.
- Store these in **Infisical** (`dev` env of `github-actions-for-endatix`; the `recursive` fetch finds them
  in any subfolder), loaded by the Infisical OIDC step before release:
  - **`NUGET_API_KEY`** — nuget.org key scoped to push `Endatix.Outbox.Engine`.
  - **`ENDATIX_PROJECT_AUTOMATIONS_APP_ID`** / **`ENDATIX_PROJECT_AUTOMATIONS_PRIVATE_KEY`** — the release-bot
    GitHub App (installed on this repo with **contents + issues + pull-requests: write**).
- Ensure the **release-bot GitHub App** (used by `.github/workflows/ci.yml`) is installed on this repo and
  allowed to create tags/releases — configure any branch-protection ruleset/bypass for the **App**, not the
  `github-actions[bot]` actor (releases are minted with the App token, not `GITHUB_TOKEN`).

> No file is committed back to `main` (no `@semantic-release/git`), so this works cleanly with protected
> `main`. To also commit a `CHANGELOG.md`, add `@semantic-release/changelog` + `@semantic-release/git` (needs
> a token allowed to push to protected `main`).

endatix consumes the published package via a normal `PackageReference` (Infrastructure only).
