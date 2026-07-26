(ns rodnik.core
  "rodnik public API.

  rodnik combines event logs and materialized views into one system: you
  append events to durable partitioned logs, processors consume them
  transactionally, and the results live in views — durable Clojure data
  structures of any shape, queried with Specter paths. Storage is pluggable:
  the same program runs on the in-memory backend or on PostgreSQL.

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
                        (r/transform! tx :word-counts word
                                      [(sp/nil->val 0)] inc))})
          (r/start!)))

    (r/append! sys :words \"hello\")
    (r/select-one sys :word-counts \"hello\") ;=> 1"
  (:require [com.rpl.specter :as sp]
            [rodnik.backend :as b]
            [rodnik.processor :as processor]))

(defn system
  "Create a system on the given backend. Declare logs, views and processors
  on it, then `start!` it."
  [{:keys [backend]}]
  {:backend backend
   :processors (atom {})
   :stops (atom nil)})

(defn declare-log!
  "Declare an append-only partitioned event log. Idempotent.
  opts: {:partitions n} (default 1)."
  ([sys log] (declare-log! sys log {}))
  ([sys log opts]
   (b/create-log! (:backend sys) log (merge {:partitions 1} opts))
   sys))

(defn declare-view!
  "Declare a durable view: a map of key -> arbitrary EDN value. Idempotent."
  ([sys view] (declare-view! sys view {}))
  ([sys view opts]
   (b/create-view! (:backend sys) view opts)
   sys))

(defn declare-processor!
  "Declare a processor consuming one source log.

  spec:
    :source     log name to consume (required)
    :handler    (fn [tx event]) — issue writes via transform!/put! (required)
    :batch-size max events per transaction (default 100)
    :poll-ms    idle poll interval (default 20)
    :retry-ms   backoff after a failed batch (default 1000)"
  [sys processor-name spec]
  (swap! (:processors sys) assoc processor-name spec)
  sys)

(defn start!
  "Start consumer threads for all declared processors. Idempotent."
  [sys]
  (when-not @(:stops sys)
    (reset! (:stops sys)
            (mapv (fn [[processor-name spec]]
                    (processor/start! (:backend sys) processor-name spec))
                  @(:processors sys))))
  sys)

(defn stop!
  "Stop all processor threads. The system can be started again."
  [sys]
  (when-let [stops @(:stops sys)]
    (run! #(%) stops)
    (reset! (:stops sys) nil))
  sys)

(defn close!
  "Stop processors and release backend resources."
  [sys]
  (stop! sys)
  (b/close!* (:backend sys))
  sys)

(defn append!
  "Append an event to a log. Returns the assigned offset.

  partition-key routes the event to a partition (hash-based) and defaults to
  the event itself. Events with the same partition-key are processed in
  order; there is no ordering across partition keys."
  ([sys log event] (append! sys log event event))
  ([sys log partition-key event]
   (let [backend (:backend sys)
         partition (mod (hash partition-key) (b/log-partitions backend log))]
     (b/append!* backend log partition event))))

(defn select
  "All results of navigating the Specter path inside the view's value at
  key k. Empty path selects the whole value."
  ([sys view k] (select sys view k []))
  ([sys view k path]
   (sp/select (vec path) (b/view-read* (:backend sys) view k))))

(defn select-one
  "Like `select` but returns the single result, or nil."
  ([sys view k] (select-one sys view k []))
  ([sys view k path]
   (sp/select-one (vec path) (b/view-read* (:backend sys) view k))))

(defn transform!
  "Within a processor transaction: navigate the Specter path inside the
  view's value at key k, apply (f value & args) at its terminus, and write
  the result back. Use sp/nil->val in the path to seed missing state."
  [tx view k path f & args]
  (let [v (b/view-get* tx view k)
        v' (sp/transform (vec path) #(apply f % args) v)]
    (b/view-put* tx view k v')))

(defn put!
  "Within a processor transaction: replace the view's value at key k."
  [tx view k v]
  (b/view-put* tx view k v))
