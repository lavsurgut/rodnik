(ns rodnik.core-test
  (:require [clojure.test :refer [deftest is]]
            [com.rpl.specter :as sp]
            [rodnik.backend.mem :as mem]
            [rodnik.core :as r]))

(defn await-until
  "Poll pred until it returns truthy or timeout-ms elapses."
  ([pred] (await-until pred 10000))
  ([pred timeout-ms]
   (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
     (loop []
       (cond
         (pred) true
         (> (System/currentTimeMillis) deadline) false
         :else (do (Thread/sleep 20) (recur)))))))

(defn word-count-system
  "The canonical example: count words from the :words log into the
  :word-counts view."
  [backend]
  (-> (r/system {:backend backend})
      (r/declare-log! :words {:partitions 2})
      (r/declare-view! :word-counts)
      (r/declare-processor! :word-count
        {:source :words
         :handler (fn [tx word]
                    (r/transform! tx :word-counts word [(sp/nil->val 0)] inc))})
      (r/start!)))

(deftest word-count
  (let [sys (word-count-system (mem/backend))]
    (try
      (run! #(r/append! sys :words %) ["to" "be" "or" "not" "to" "be"])
      (is (await-until #(and (= 2 (r/select-one sys :word-counts "to"))
                             (= 2 (r/select-one sys :word-counts "be"))
                             (= 1 (r/select-one sys :word-counts "or"))
                             (= 1 (r/select-one sys :word-counts "not")))))
      (is (nil? (r/select-one sys :word-counts "absent")))
      (finally
        (r/close! sys)))))

(deftest nested-views-and-paths
  (let [sys (-> (r/system {:backend (mem/backend)})
                (r/declare-log! :page-views)
                (r/declare-view! :profiles)
                (r/declare-processor! :profiler
                  {:source :page-views
                   :handler (fn [tx {:keys [user page]}]
                              (r/transform! tx :profiles user
                                            [(sp/keypath :views)
                                             (sp/keypath page)
                                             (sp/nil->val 0)]
                                            inc))})
                (r/start!))]
    (try
      (r/append! sys :page-views "alice" {:user "alice" :page "home"})
      (r/append! sys :page-views "alice" {:user "alice" :page "home"})
      (r/append! sys :page-views "alice" {:user "alice" :page "settings"})
      (is (await-until #(= 2 (r/select-one sys :profiles "alice"
                                           [(sp/keypath :views "home")]))))
      (is (= 1 (r/select-one sys :profiles "alice"
                             [(sp/keypath :views "settings")])))
      (is (= {:views {"home" 2 "settings" 1}}
             (r/select-one sys :profiles "alice")))
      (is (= [2] (r/select sys :profiles "alice" [(sp/keypath :views "home")])))
      (finally
        (r/close! sys)))))

(deftest positions-survive-restart
  (let [backend (mem/backend)
        sys (word-count-system backend)]
    (run! #(r/append! sys :words %) ["a" "b" "a"])
    (is (await-until #(= 2 (r/select-one sys :word-counts "a"))))
    (r/stop! sys)
    ;; A new system on the same backend continues from the stored positions:
    ;; nothing is reprocessed, so counts advance instead of doubling.
    (let [sys2 (word-count-system backend)]
      (try
        (run! #(r/append! sys2 :words %) ["a" "c"])
        (is (await-until #(and (= 3 (r/select-one sys2 :word-counts "a"))
                               (= 1 (r/select-one sys2 :word-counts "c")))))
        (is (= 1 (r/select-one sys2 :word-counts "b")))
        (finally
          (r/stop! sys2))))))

(deftest failed-batches-roll-back-and-retry
  (let [attempts (atom 0)
        sys (-> (r/system {:backend (mem/backend)})
                (r/declare-log! :numbers)
                (r/declare-view! :sums)
                (r/declare-processor! :summer
                  {:source :numbers
                   :retry-ms 20
                   :handler (fn [tx n]
                              (r/transform! tx :sums :total [(sp/nil->val 0)] + n)
                              ;; Fail the first two delivery attempts *after*
                              ;; writing, proving the write rolls back with
                              ;; the batch and is not applied twice.
                              (when (< (swap! attempts inc) 3)
                                (throw (ex-info "transient failure" {}))))})
                (r/start!))]
    (try
      (r/append! sys :numbers 41)
      (is (await-until #(= 41 (r/select-one sys :sums :total))))
      (is (= 3 @attempts))
      (finally
        (r/close! sys)))))
