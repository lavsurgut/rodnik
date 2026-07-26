(ns rodnik.pg-test
  "End-to-end tests against a real PostgreSQL.

  Opt-in: set RODNIK_PG_TEST=1 (and optionally RODNIK_PG_URL). Locally:

    docker compose up -d
    RODNIK_PG_TEST=1 clojure -X:test

  The tests drop and recreate every rodnik_* table they touch."
  (:require [clojure.test :refer [deftest is]]
            [com.rpl.specter :as sp]
            [next.jdbc :as jdbc]
            [rodnik.backend.pg :as pg]
            [rodnik.core :as r]
            [rodnik.core-test :as ct]))

(def ^:private enabled? (some? (System/getenv "RODNIK_PG_TEST")))

(def ^:private url
  (or (System/getenv "RODNIK_PG_URL")
      "jdbc:postgresql://localhost:5433/rodnik?user=rodnik&password=rodnik"))

(defn- reset-tables! []
  (let [ds (jdbc/get-datasource url)]
    (doseq [t ["rodnik_log_words" "rodnik_matview_word_counts"
               "rodnik_positions" "rodnik_meta"]]
      (jdbc/execute! ds [(str "DROP TABLE IF EXISTS " t " CASCADE")]))))

(deftest word-count-on-postgres
  (if-not enabled?
    (println "rodnik.pg-test: skipped (set RODNIK_PG_TEST=1 to run)")
    (do
      (reset-tables!)
      (let [sys (ct/word-count-system (pg/backend {:jdbc-url url}))]
        (try
          (run! #(r/append! sys :words %) ["to" "be" "or" "not" "to" "be"])
          (is (ct/await-until
               #(and (= 2 (r/select-one sys :word-counts "to"))
                     (= 2 (r/select-one sys :word-counts "be"))
                     (= 1 (r/select-one sys :word-counts "or"))
                     (= 1 (r/select-one sys :word-counts "not")))))
          (finally
            (r/close! sys))))
      ;; Durability: a brand-new system over the same database continues from
      ;; the stored matviews and positions — no reprocessing, no double counts.
      (let [sys (ct/word-count-system (pg/backend {:jdbc-url url}))]
        (try
          (run! #(r/append! sys :words %) ["to" "and" "fro"])
          (is (ct/await-until
               #(and (= 3 (r/select-one sys :word-counts "to"))
                     (= 1 (r/select-one sys :word-counts "and"))
                     (= 1 (r/select-one sys :word-counts "fro")))))
          (is (= 2 (r/select-one sys :word-counts "be")))
          (finally
            (r/close! sys)))))))

(deftest edn-fidelity-on-postgres
  (if-not enabled?
    (println "rodnik.pg-test: skipped (set RODNIK_PG_TEST=1 to run)")
    (let [ds (jdbc/get-datasource url)]
      (doseq [t ["rodnik_log_events" "rodnik_matview_snapshots"]]
        (jdbc/execute! ds [(str "DROP TABLE IF EXISTS " t " CASCADE")]))
      (let [event {:id #uuid "8d47f2a0-0000-4000-8000-000000000042"
                   :tags #{:a :b}
                   :ratio 1/3
                   :nested {[1 2] "vector-key"}}
            sys (-> (r/system {:backend (pg/backend {:jdbc-url url})})
                    (r/declare-log! :events)
                    (r/declare-matview! :snapshots)
                    (r/declare-processor! :snapshotter
                      {:source :events
                       :handler (fn [tx e] (r/put! tx :snapshots (:id e) e))})
                    (r/start!))]
        (try
          (r/append! sys :events (:id event) event)
          (is (ct/await-until #(= event (r/select-one sys :snapshots (:id event)))))
          (is (= #{:a :b} (r/select-one sys :snapshots (:id event)
                                        [(sp/keypath :tags)])))
          (finally
            (r/close! sys)))))))
