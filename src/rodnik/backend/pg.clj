(ns rodnik.backend.pg
  "PostgreSQL backend.

  Mapping:

   - log       -> rodnik_log_<name>   (part int, off bigint identity, data text)
   - view      -> rodnik_view_<name>  (k text primary key, v text)
   - positions -> rodnik_positions    (processor, log, part -> off)
   - registry  -> rodnik_meta         (declared logs/views and their opts)

  All keys and values are stored as EDN text (pr-str / clojure.edn), so views
  hold arbitrary Clojure data with full fidelity. `with-tx*` maps to a single
  SQL transaction; view reads inside a tx take FOR UPDATE row locks, so
  concurrent processors updating the same key serialize instead of clobbering
  each other.

  Table names are derived from log/view names sanitized to [a-z0-9_]; they
  come from code, not user input."
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [rodnik.backend :as b]))

(def ^:private q-opts {:builder-fn rs/as-unqualified-lower-maps})

(defn- q1 [conn sql-params] (jdbc/execute-one! conn sql-params q-opts))
(defn- q  [conn sql-params] (jdbc/execute! conn sql-params q-opts))

(defn- sql-name [prefix n]
  (str prefix
       (-> (name n)
           (str/replace #"[^a-zA-Z0-9]+" "_")
           (str/replace #"(^_+|_+$)" "")
           str/lower-case)))

(defn- log-table [l] (sql-name "rodnik_log_" l))
(defn- view-table [v] (sql-name "rodnik_view_" v))

(def ^:private base-ddl
  ["CREATE TABLE IF NOT EXISTS rodnik_meta (
      kind text NOT NULL,
      name text NOT NULL,
      opts text NOT NULL,
      PRIMARY KEY (kind, name))"
   "CREATE TABLE IF NOT EXISTS rodnik_positions (
      processor text   NOT NULL,
      log       text   NOT NULL,
      part      int    NOT NULL,
      off       bigint NOT NULL,
      PRIMARY KEY (processor, log, part))"])

(defrecord PgTx [conn]
  b/Tx
  (view-get* [_ view k]
    (some-> (q1 conn [(str "SELECT v FROM " (view-table view)
                           " WHERE k = ? FOR UPDATE")
                      (pr-str k)])
            :v
            edn/read-string))
  (view-put* [_ view k v]
    (q1 conn [(str "INSERT INTO " (view-table view) " (k, v) VALUES (?, ?)"
                   " ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v")
              (pr-str k) (pr-str v)]))
  (set-position* [_ processor log partition offset]
    (q1 conn [(str "INSERT INTO rodnik_positions (processor, log, part, off)"
                   " VALUES (?, ?, ?, ?)"
                   " ON CONFLICT (processor, log, part)"
                   " DO UPDATE SET off = EXCLUDED.off")
              (str processor) (str log) partition offset])))

(defrecord PgBackend [ds meta-cache]
  b/Backend
  (create-log! [_ log opts]
    (let [t (log-table log)]
      (jdbc/execute! ds [(str "CREATE TABLE IF NOT EXISTS " t " (
                                part        int    NOT NULL,
                                off         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                                data        text   NOT NULL,
                                appended_at timestamptz NOT NULL DEFAULT now())")])
      (jdbc/execute! ds [(str "CREATE INDEX IF NOT EXISTS " t "_part_off"
                              " ON " t " (part, off)")])
      (jdbc/execute! ds ["INSERT INTO rodnik_meta (kind, name, opts) VALUES ('log', ?, ?)
                          ON CONFLICT (kind, name) DO UPDATE SET opts = EXCLUDED.opts"
                         (str log) (pr-str opts)])
      (swap! meta-cache assoc [:log log] opts)))
  (create-view! [_ view opts]
    (jdbc/execute! ds [(str "CREATE TABLE IF NOT EXISTS " (view-table view) " (
                              k text PRIMARY KEY,
                              v text NOT NULL)")])
    (jdbc/execute! ds ["INSERT INTO rodnik_meta (kind, name, opts) VALUES ('view', ?, ?)
                        ON CONFLICT (kind, name) DO UPDATE SET opts = EXCLUDED.opts"
                       (str view) (pr-str opts)])
    (swap! meta-cache assoc [:view view] opts))
  (log-partitions [_ log]
    (or (get-in @meta-cache [[:log log] :partitions])
        (some-> (q1 ds ["SELECT opts FROM rodnik_meta WHERE kind = 'log' AND name = ?"
                        (str log)])
                :opts edn/read-string :partitions)
        1))
  (append!* [_ log partition data]
    ;; The advisory lock serializes appends per partition so that commit
    ;; order always matches offset order. Without it, a reader could observe
    ;; offset n+1 while the transaction that took offset n is still open,
    ;; advance past n, and never see it.
    (jdbc/with-transaction [tx ds]
      (q1 tx ["SELECT pg_advisory_xact_lock(hashtext(?))"
              (str (log-table log) "/" partition)])
      (:off (q1 tx [(str "INSERT INTO " (log-table log)
                         " (part, data) VALUES (?, ?) RETURNING off")
                    partition (pr-str data)]))))
  (read-batch* [_ log partition after-offset limit]
    (mapv (fn [{:keys [off data]}]
            {:offset off :data (edn/read-string data)})
          (q ds [(str "SELECT off, data FROM " (log-table log)
                      " WHERE part = ? AND off > ? ORDER BY off LIMIT ?")
                 partition after-offset limit])))
  (get-position* [_ processor log partition]
    (or (:off (q1 ds ["SELECT off FROM rodnik_positions
                       WHERE processor = ? AND log = ? AND part = ?"
                      (str processor) (str log) partition]))
        -1))
  (view-read* [_ view k]
    (some-> (q1 ds [(str "SELECT v FROM " (view-table view) " WHERE k = ?")
                    (pr-str k)])
            :v
            edn/read-string))
  (with-tx* [_ f]
    (jdbc/with-transaction [tx-conn ds]
      (f (->PgTx tx-conn))))
  (close!* [_] nil))

(defn backend
  "Create a PostgreSQL backend.

  opts: {:jdbc-url \"jdbc:postgresql://host:port/db?user=u&password=p\"}
  or any next.jdbc db-spec map."
  [opts]
  (let [ds (jdbc/get-datasource (or (:jdbc-url opts) opts))]
    (doseq [stmt base-ddl]
      (jdbc/execute! ds [stmt]))
    (->PgBackend ds (atom {}))))
