# rodnik design

## Goals

1. **One system, one transaction.** Event transport, processing, and indexed
   state live behind one API, and a processor's effects commit atomically
   with its progress. Exactly-once state updates are the defining guarantee,
   not an add-on.
2. **State as plain Clojure data.** Views are durable EDN values of arbitrary
   shape, navigated with Specter paths — the same way you'd work with an
   atom in a REPL.
3. **Pluggable storage.** The programming model is defined against a small
   backend SPI. PostgreSQL is the first durable backend; FoundationDB is the
   planned second. The in-memory backend is the executable specification.
4. **Boring operations.** No new stateful service to run. If you can operate
   PostgreSQL, you can operate rodnik.

## Non-goals (for now)

- A distributed compute runtime. Rodnik processes run inside your JVM;
  scale-out coordination (leases, partition assignment across nodes) is
  future work.
- A dataflow DSL. Handlers are plain functions. Composition primitives
  (chained processors, joins, windows) should grow out of real usage.
- Cross-key transactions spanning multiple partitions of a log.

## The model

```
append! ──> log (partitioned, append-only)
                │  polled in batches
                ▼
            processor ── handler(tx, event) ──> view writes via tx
                │                                    │
                └── set-position ────────────────────┘
                        one backend transaction
                                                view <── select / select-one
```

- **Logs** are append-only and partitioned. `append!` hashes a partition key
  to pick a partition; offsets are per-partition and monotonically
  increasing. Ordering is guaranteed only within a partition.
- **Processors** own their progress: one consumer loop per (processor,
  partition). Each iteration reads a batch of events past the stored
  position, runs the handler for each event, advances the position, and
  commits — all inside `with-tx*`. A handler exception rolls the whole batch
  back; it is retried after a backoff, never skipped. Poison events
  therefore block their partition (dead-letter policy is roadmap).
- **Views** are maps of key → EDN value. Handlers read/modify/write through
  the transaction (`transform!` = read at key, apply a Specter transform,
  write back). Readers see committed state only.

### Delivery semantics, precisely

Handlers may run more than once for the same event (a batch that fails after
partial handler execution is retried), but their *transactional effects*
apply exactly once: every write goes through the tx that also advances the
position, so a rolled-back attempt leaves nothing behind. Corollary: side
effects outside the tx (HTTP calls, emails) are at-least-once — keep
handlers pure writes-through-tx where possible.

## Backend SPI

`rodnik.backend/Backend` + `rodnik.backend/Tx` (see docstrings). The
contract in one sentence: logs are ordered-per-partition durable sequences,
views are durable key→EDN maps, and `with-tx*` runs view writes + position
advances atomically with read-your-writes visibility inside the
transaction.

What a new backend must think about:

- **Atomicity** of `with-tx*` — the whole guarantee rests on it.
- **Append visibility order.** `read-batch*` must never return offset n+1
  while an append that will occupy offset n is still uncommitted, or
  consumers would skip n forever. (See how the PostgreSQL backend handles
  this below.)
- **Concurrent access to a view key.** Two transactions updating the same
  key must serialize (lock or retry), not lost-update each other.

## PostgreSQL backend

| rodnik | PostgreSQL |
|---|---|
| log | `rodnik_log_<name>` — `part int`, `off bigint identity` (PK), `data text` (EDN), `appended_at`; index on `(part, off)` |
| view | `rodnik_view_<name>` — `k text` (PK, EDN), `v text` (EDN) |
| positions | `rodnik_positions` — `(processor, log, part) -> off` |
| registry | `rodnik_meta` — declared logs/views and their opts |
| `with-tx*` | one SQL transaction |
| tx view read | `SELECT ... FOR UPDATE` (serializes writers per key) |
| consumption | polling (`off > position ORDER BY off LIMIT batch`) |

Design choices and their reasons:

- **EDN text over jsonb.** Values round-trip with full Clojure fidelity
  (keywords, sets, ratios, non-string keys). The cost: paths are applied
  client-side after fetching the key's value. Planned optimization: optional
  schema'd views with jsonb + generated columns for server-side path
  push-down — as an opt-in, not the default.
- **Append serialization per partition.** `append!*` takes
  `pg_advisory_xact_lock(hashtext('<table>/<part>'))` before inserting, so
  commit order always matches offset order within a partition and the
  visibility rule above holds. Cost: appends to one partition serialize
  (~single-writer throughput per partition); spread hot logs across more
  partitions. Roadmap: batched appends amortizing the lock.
- **Offsets are global per log** (one identity column), but consumed and
  compared per partition — gaps within a partition's offset sequence are
  normal and harmless.
- **Polling, not LISTEN/NOTIFY**, for the first version: simpler, works
  through poolers, and 20 ms default polls are fine for most workloads.
  NOTIFY-based wakeup is a straightforward later addition.

### Current limitations

- One consumer loop per (processor, partition) *per process*; running the
  same system in two JVMs would double-process. Multi-node partition leases
  are the planned fix.
- No connection pooling yet (plain `DriverManager` datasource) — wire in
  HikariCP for anything beyond experiments.
- Log tables grow forever; retention/compaction is roadmap.
- A poison event blocks its partition (by design, until dead-letter policy
  lands).

## Roadmap

1. **Hardening:** connection pooling, metrics hooks, dead-letter policy,
   log retention.
2. **Scale-out:** partition leases so multiple JVMs share one system safely.
3. **Composition:** processors consuming multiple logs, chained
   processor→log pipelines, windows.
4. **Reactive reads:** subscriptions to view paths (poll → NOTIFY →
   incremental).
5. **FoundationDB backend:** the SPI's second durable implementation;
   its transaction model maps naturally onto `with-tx*`.
6. **Server-side paths:** opt-in schema'd views on jsonb with generated
   columns/indexes for hot query paths.
