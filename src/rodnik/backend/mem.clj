(ns rodnik.backend.mem
  "In-memory backend: the reference implementation of the SPI.

  Single-process and non-durable — for tests, REPL exploration, and as
  executable documentation of the semantics every backend must provide.
  Transactions are modeled with a single lock: a tx works on a snapshot of
  the state and publishes it atomically on commit."
  (:require [rodnik.backend :as b]))

(defn- tx-view [pending]
  (reify b/Tx
    (matview-get* [_ matview k]
      (get-in @pending [:matviews matview k]))
    (matview-put* [_ matview k v]
      (vswap! pending assoc-in [:matviews matview k] v))
    (set-position* [_ processor log partition offset]
      (vswap! pending assoc-in [:positions [processor log partition]] offset))))

(defrecord MemBackend [state lock]
  b/Backend
  (create-log! [_ log opts]
    (let [n (:partitions opts 1)]
      (swap! state update-in [:logs log]
             #(or % {:partitions n :events (vec (repeat n []))}))))
  (create-matview! [_ matview _opts]
    (swap! state update-in [:matviews matview] #(or % {})))
  (log-partitions [_ log]
    (get-in @state [:logs log :partitions]))
  (append!* [_ log partition data]
    (locking lock
      (let [state' (swap! state update-in [:logs log :events partition] conj data)]
        (dec (count (get-in state' [:logs log :events partition]))))))
  (read-batch* [_ log partition after-offset limit]
    (let [events (get-in @state [:logs log :events partition])]
      (into []
            (comp (drop (inc after-offset))
                  (take limit)
                  (map-indexed (fn [i d] {:offset (+ after-offset 1 i) :data d})))
            events)))
  (get-position* [_ processor log partition]
    (get-in @state [:positions [processor log partition]] -1))
  (matview-read* [_ matview k]
    (get-in @state [:matviews matview k]))
  (with-tx* [_ f]
    (locking lock
      (let [pending (volatile! @state)
            result (f (tx-view pending))]
        (reset! state @pending)
        result)))
  (close!* [_] nil))

(defn backend
  "Create a fresh in-memory backend."
  []
  (->MemBackend (atom {:logs {} :matviews {} :positions {}}) (Object.)))
