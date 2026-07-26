# rodnik design

## Goals

1. **One system, one transaction.** Transport, processing, and indexed state
   behind one API; a processor's effects commit atomically with its
   progress. Exactly-once state updates are the defining guarantee.
2. **State as plain Clojure data.** Matviews are durable EDN values of any
   shape, navigated with Specter paths.
3. **Pluggable storage.** The model is defined against a small backend SPI;
   the in-memory backend is its executable specification.
4. **Boring operations.** No new stateful service to run.

Non-goals for now: a distributed compute runtime, a dataflow DSL,
cross-partition transactions.

## Model

```
append! ─> log (partitioned, append-only)
               │ polled in batches
               ▼
           processor ── handler(tx, event) ─> matview writes via tx
               │                                   │
               └── set-position ───────────────────┘
                       one backend transaction
                                              matview <── select / select-one
```

- Offsets are per-partition and monotonic; `append!` routes by partition
  key, and ordering holds only within a partition.
- One consumer loop per (processor, partition). A batch is one transaction;
  a handler exception rolls the batch back and retries — never skips.
- Handlers may *run* more than once for an event, but their transactional
  effects apply exactly once. Side effects outside the tx (HTTP, email) are
  at-least-once.

## Backend SPI

`rodnik.backend/Backend` + `Tx`: logs are ordered-per-partition durable
sequences, matviews are durable key→EDN maps, and `with-tx*` runs matview writes
and position advances atomically, with read-your-writes inside the
transaction. Implementations must also ensure:

- `read-batch*` never returns offset n+1 while an append destined for
  offset n is uncommitted — consumers would skip n forever;
- concurrent transactions on the same matview key serialize instead of
  lost-updating each other.

## PostgreSQL backend

| rodnik | PostgreSQL |
|---|---|
| log | `rodnik_log_<name>` — part, off (identity, PK), data (EDN text) |
| matview | `rodnik_matview_<name>` — k (EDN text, PK), v (EDN text) |
| positions | `rodnik_positions` — (processor, log, part) → off |
| `with-tx*` | one SQL transaction |
| tx matview read | `SELECT … FOR UPDATE` |
| consumption | polling, 20 ms default |

- **EDN text over jsonb:** full Clojure fidelity; paths apply client-side.
  Opt-in jsonb matviews with server-side path push-down are roadmap.
- **`pg_advisory_xact_lock` per (log, partition) on append** keeps commit
  order equal to offset order. Cost: single-writer throughput per partition.

Limitations: one JVM per system (no partition leases yet), no connection
pooling, no log retention, poison events block their partition.

## Roadmap

1. Hardening: connection pooling, metrics, dead-letter policy, retention.
2. Scale-out: partition leases so multiple JVMs share one system.
3. Composition: multi-log processors, chained pipelines, windows.
4. Reactive reads: subscriptions to matview paths.
5. More backends: FoundationDB next.
6. Server-side paths: schema'd jsonb matviews for hot queries.
