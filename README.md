# Endatix.Outbox.Engine

Endatix-agnostic **transactional-outbox relay engine**. This is the foundational package depended on by
*both* the Endatix platform (in-process relay) and a standalone relay worker — it references neither, so it
sits below both in the dependency graph.

## What it provides

- **Contracts** the relay operates on (host implements them):
  - `IOutboxMessage` — read shape of an outbox row.
  - `IOutboxClaimStore` — DB-arbitrated claim (`SKIP LOCKED`/lease) + mark sent/reschedule/failed.
  - `IIntegrationEventPublisher` — delivers a claimed message (webhook in-process, DAPR in the worker).
- **`OutboxRelayBackgroundService`** — the `claim → publish → mark` loop. Scope-per-tick, exponential
  backoff, lease-based crash recovery, at-least-once.
- **OpenFeature gate** — `IOutboxRelayGate` / `OpenFeatureOutboxRelayGate` evaluate the
  `outbox-relay-in-process` flag (`OutboxFlags.RelayInProcess`, default `true`) every tick. Flip it to
  hand the relay over to a standalone worker — no restart with a dynamic provider.
- **`AddOutboxRelay()`** — registers the loop, options, and default gate.

## What the host must supply

`AddOutboxRelay()` deliberately does **not** register: an `IOutboxClaimStore`, an
`IIntegrationEventPublisher`, or an OpenFeature provider (which supplies `IFeatureClient`). The host wires
those — keeping the engine transport- and storage-agnostic.

## Dependencies

`Microsoft.Extensions.*` abstractions (Hosting/DI/Options/Logging) + `OpenFeature`. No EF, no Endatix.

## Build & test

```bash
dotnet test
```
