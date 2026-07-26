(ns user
  "REPL scratchpad. Start with: clojure -M:dev -r"
  (:require [com.rpl.specter :as sp]
            [rodnik.backend.mem :as mem]
            [rodnik.backend.pg :as pg]
            [rodnik.core :as r]))

(comment
  ;; In-memory word count
  (def sys
    (-> (r/system {:backend (mem/backend)})
        (r/declare-log! :words {:partitions 2})
        (r/declare-matview! :word-counts)
        (r/declare-processor! :word-count
          {:source :words
           :handler (fn [tx word]
                      (r/transform! tx :word-counts word [(sp/nil->val 0)] inc))})
        (r/start!)))

  (r/append! sys :words "hello")
  (r/select-one sys :word-counts "hello")

  ;; Same program on PostgreSQL (docker compose up -d)
  (def pg-sys
    (-> (r/system {:backend (pg/backend
                             {:jdbc-url "jdbc:postgresql://localhost:5433/rodnik?user=rodnik&password=rodnik"})})
        (r/declare-log! :words {:partitions 2})
        (r/declare-matview! :word-counts)
        (r/declare-processor! :word-count
          {:source :words
           :handler (fn [tx word]
                      (r/transform! tx :word-counts word [(sp/nil->val 0)] inc))})
        (r/start!)))

  (r/append! pg-sys :words "hello")
  (r/select-one pg-sys :word-counts "hello")

  (r/close! sys)
  (r/close! pg-sys))
