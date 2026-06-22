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

## Dependencies

`Microsoft.Extensions.*` abstractions (Hosting/DI/Options/Logging) + `OpenFeature`, plus `System.Data.Common`
(in-runtime — the claim store is raw ADO.NET). **No EF, no provider package, no Endatix, no DAPR.**

## Build & test

```bash
dotnet test
```
