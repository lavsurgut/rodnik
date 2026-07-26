# rodnik

Rodnik is an event-streaming and materialized-view system for Clojure, built
on storage you already run. You append events to durable, partitioned
**logs**; **processors** consume them transactionally; the results live in
**views** — durable Clojure data structures of any shape, queried with
[Specter](https://clojars.org/com.rpl/specter) paths. Storage is
pluggable: the same program runs unchanged on the in-memory backend or on
**PostgreSQL** (more backends is on the roadmap).

**Status: pre-alpha.** The programming model works end to end, but APIs will
change and nothing has seen production traffic yet.

## Why

The conventional event-driven stack is three systems glued together — a
message queue, a stream processor, and a database — with delivery guarantees
that evaporate at each seam. Rodnik collapses the seams: because a
processor's view writes and its log position advance in a *single backend
transaction*, every event is reflected in your views **exactly once**, even
across crashes and restarts. No idempotency bookkeeping, no dedup tables, no
at-least-once surprises.

And because views are plain Clojure data (EDN in, EDN out — keywords, sets,
ratios, vector keys and all), you shape state exactly like you would in a
REPL, then query any slice of it with a Specter path.

## Quickstart

```clojure
;; deps.edn
{:deps {io.github.lavsurgut/rodnik {:git/sha "..."}}}
```

```clojure
(require '[rodnik.core :as r]
         '[rodnik.backend.mem :as mem]
         '[com.rpl.specter :as sp])

(def sys
  (-> (r/system {:backend (mem/backend)})
      (r/declare-log! :words {:partitions 2})
      (r/declare-view! :word-counts)
      (r/declare-processor! :word-count
        {:source :words
         :handler (fn [tx word]
                    (r/transform! tx :word-counts word [(sp/nil->val 0)] inc))})
      (r/start!)))

(r/append! sys :words "hello")
(r/select-one sys :word-counts "hello") ;=> 1
```

Swap one line to run the same program on PostgreSQL:

```clojure
(require '[rodnik.backend.pg :as pg])

(r/system {:backend (pg/backend {:jdbc-url "jdbc:postgresql://localhost:5433/rodnik?user=rodnik&password=rodnik"})})
```

```bash
docker compose up -d   # local PostgreSQL on :5433
```

## Concepts

| Concept | What it is |
|---|---|
| **system** | The unit of deployment: a backend plus your declared logs, views, and processors. |
| **log** | A durable, append-only, partitioned event log. `append!` routes by partition key; per-key order is preserved. |
| **processor** | A transactional consumer of one log. Its handler receives each event with an open transaction and updates views through it. |
| **view** | A durable map of key → arbitrary EDN value. Written with `transform!`/`put!` inside processors, read anywhere with `select`/`select-one` and Specter paths. |

## Guarantees

- **Exactly-once state updates.** View writes and position advances commit
  atomically; a failed batch rolls back completely and is retried.
- **Per-partition-key ordering.** Events with the same partition key are
  processed in append order.
- **Data fidelity.** Views and events round-trip as EDN — what you put in is
  what you get out.

See [DESIGN.md](DESIGN.md) for the model, the backend SPI, the PostgreSQL
mapping, and current limitations.

## Development

```bash
clojure -X:test                                   # in-memory tests
docker compose up -d && RODNIK_PG_TEST=1 clojure -X:test   # + PostgreSQL tests
```

## License

Copyright © 2026 Valery Lavrentiev. Licensed under the
[Apache License 2.0](LICENSE).
