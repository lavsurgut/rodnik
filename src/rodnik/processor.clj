(ns rodnik.processor
  "The processor runtime: polling consumer loops.

  One daemon thread per (processor, source partition). Each iteration runs a
  single backend transaction: read a batch, run the handler for every event,
  advance the position, commit. A handler exception rolls the whole batch
  back; the batch is retried after :retry-ms and never skipped."
  (:require [clojure.tools.logging :as log]
            [rodnik.backend :as b]))

(defn- run-once!
  "Process at most one batch for one partition. Returns the number of events
  processed (0 when the log had nothing new)."
  [backend processor-name {:keys [source handler batch-size]} partition]
  (let [from (b/get-position* backend processor-name source partition)
        batch (b/read-batch* backend source partition from batch-size)]
    (if (empty? batch)
      0
      (do (b/with-tx* backend
            (fn [tx]
              (doseq [{:keys [data]} batch]
                (handler tx data))
              (b/set-position* tx processor-name source partition
                               (:offset (last batch)))))
          (count batch)))))

(defn start!
  "Start consumer threads for one processor. Returns a zero-arg stop fn."
  [backend processor-name {:keys [source poll-ms retry-ms batch-size]
                           :or {poll-ms 20 retry-ms 1000 batch-size 100}
                           :as spec}]
  (let [spec (assoc spec :batch-size batch-size)
        running? (atom true)
        n-parts (b/log-partitions backend source)
        threads
        (mapv (fn [partition]
                (doto (Thread.
                       (fn []
                         (while @running?
                           (let [n (try
                                     (run-once! backend processor-name spec partition)
                                     (catch Throwable t
                                       (log/warn t "processor" processor-name
                                                 "partition" partition
                                                 "failed; batch rolled back, retrying in"
                                                 retry-ms "ms")
                                       (Thread/sleep (long retry-ms))
                                       0))]
                             (when (zero? n)
                               (Thread/sleep (long poll-ms))))))
                       (str "rodnik-" (name processor-name) "-p" partition))
                  (.setDaemon true)
                  (.start)))
              (range n-parts))]
    (fn stop! []
      (reset! running? false)
      (doseq [^Thread t threads]
        (.join t 2000)))))
