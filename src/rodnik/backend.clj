(ns rodnik.backend
  "Backend SPI — the storage contract rodnik runs on.

  A backend provides three primitives:

   - logs:      append-only, partitioned logs of EDN events
   - views:     durable maps of key -> arbitrary EDN value
   - positions: consumer progress per (processor, log, partition)

  and one guarantee: `with-tx*` runs view writes and position advances in a
  single atomic transaction. That transaction is what gives rodnik
  exactly-once state updates — an event's effects on views and the position
  move past that event commit together or not at all.

  Implementations: rodnik.backend.mem (reference), rodnik.backend.pg.")

(defprotocol Tx
  "Handle to an open backend transaction. Processor handlers receive one and
  issue all writes through it. Reads through the Tx must see the
  transaction's own uncommitted writes."
  (view-get* [tx view k]
    "Value at key k as seen by this transaction, or nil.")
  (view-put* [tx view k v]
    "Upsert value at key k.")
  (set-position* [tx processor log partition offset]
    "Record that processor consumed log's partition up to offset, inclusive."))

(defprotocol Backend
  (create-log! [b log opts]
    "Idempotently create a log. opts: {:partitions n}.")
  (create-view! [b view opts]
    "Idempotently create a view.")
  (log-partitions [b log]
    "Number of partitions of log.")
  (append!* [b log partition data]
    "Append EDN data to a log partition; returns the assigned offset.")
  (read-batch* [b log partition after-offset limit]
    "Committed events with offset > after-offset, in offset order:
     ({:offset n :data d} ...).")
  (get-position* [b processor log partition]
    "Last offset committed by processor for log's partition, or -1.")
  (view-read* [b view k]
    "Committed value at key k, or nil.")
  (with-tx* [b f]
    "Run (f tx). Commit on normal return, roll back on throw.
     Returns f's result.")
  (close!* [b]
    "Release resources held by the backend."))
