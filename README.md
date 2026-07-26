# rodnik

[![ci](https://github.com/lavsurgut/rodnik/actions/workflows/ci.yml/badge.svg)](https://github.com/lavsurgut/rodnik/actions/workflows/ci.yml)
![status](https://img.shields.io/badge/status-pre--alpha-orange)

Event streams and materialized views for Clojure, on storage you already
run. Append events to partitioned **logs**; **processors** consume them
transactionally into **views** — durable Clojure data of any shape, queried
with [Specter](https://clojars.org/com.rpl/specter) paths. View writes and
consumer positions commit in one backend transaction, so every event is
reflected in your views **exactly once**. Backends are pluggable: in-memory
and PostgreSQL today.

## Quickstart

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

Same program on PostgreSQL:

```clojure
(require '[rodnik.backend.pg :as pg])

(r/system {:backend (pg/backend {:jdbc-url "jdbc:postgresql://localhost:5433/rodnik?user=rodnik&password=rodnik"})})
```

See [DESIGN.md](DESIGN.md) for the model, guarantees, and roadmap.

## Development

```bash
clojure -M:test                                            # in-memory tests
docker compose up -d && RODNIK_PG_TEST=1 clojure -M:test   # + PostgreSQL
```

## License

Apache-2.0 © 2026 Valery Lavrentiev
