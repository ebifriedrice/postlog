(ns ebifriedrice.postlog.admin
  "Manage the server-side objects a logical-replication consumer
  needs: replication slots and publications, plus the introspection
  (WAL level, current LSN, slot state) that operations around a
  consumer want.

  Everything here runs as ordinary SQL on a regular connection -- no
  replication protocol -- so it works with any JDBC connection the
  caller already has.  Connect with a map or a JDBC URL:

  ```clojure
  (with-open [conn (admin/connect {:host \"localhost\" :database \"app\"
                                   :user \"repl\" :password \"...\"})]
    (admin/ensure-slot! conn \"my_slot\")
    (admin/ensure-publication! conn \"my_pub\" {:tables [\"public.users\"]}))
  ```

  A user creating slots or publications needs enough privilege
  (superuser, or ``pg_create_subscription``/ownership as the server
  version allows) and the server must run ``wal_level = logical``."
  (:require [clojure.string :as str]
            [ebifriedrice.postlog.lsn :as lsn])
  (:import (java.sql Connection DriverManager)))

(def ^:private defaults
  {:host "localhost" :port 5432 :user "postgres"})

(defn jdbc-url
  "JDBC URL for `spec`: a map (:host :port :database :user plus any
  extra params, keywordized as query params) or an existing URL
  string, passed through."
  [spec]
  (if (string? spec)
    spec
    (let [{:keys [host port database]} (merge defaults spec)
          params (dissoc spec :host :port :database :user :password)]
      (str "jdbc:postgresql://" host ":" port "/" database
           (when (seq params)
             (str "?" (str/join "&" (map (fn [[k v]] (str (name k) "=" v)) params))))))))

(defn replication-jdbc-url
  "Like [[jdbc-url]] but with ``replication=database``, the parameter
  that turns a pgjdbc connection into a replication connection (the
  stream APIs in ``ebifriedrice.postlog.replication`` need this)."
  [spec]
  (if (string? spec)
    (let [url (jdbc-url spec)]
      (if (re-find #"[?&]replication=" url)
        url
        (str url (if (str/includes? url "?") "&" "?") "replication=database")))
    (jdbc-url (assoc spec :replication "database"))))

(defn connect
  "Open a ``java.sql.Connection`` from a spec map or JDBC URL string.
  The caller owns closing it (``with-open`` works)."
  [spec]
  (let [{:keys [user password]} (when (map? spec) spec)]
    (DriverManager/getConnection (jdbc-url spec) user password)))

;; ---------------------------------------------------------------------------
;; helpers

(defn- quote-ident
  "Double-quote an SQL identifier, doubling embedded quotes.  DDL
  cannot take parameters, so names are interpolated and must be
  quoted; a name with no characters is rejected."
  [s]
  (let [n (str/replace (str s) "\"" "\"\"")]
    (when (str/blank? n)
      (throw (ex-info "Blank SQL identifier" {:name s})))
    (str "\"" n "\"")))

(defn- table-ident
  "Quote ``schema.table`` as two quoted identifiers, or one when
  unqualified."
  [s]
  (->> (str/split (str s) #"\.")
       (map quote-ident)
       (str/join ".")))

(defn- query
  "Rows of a SELECT as maps with lowercase keyword keys."
  [^Connection conn sql params]
  (with-open [stmt (doto (.prepareStatement conn sql)
                     (as-> ps (dorun (map-indexed
                                      (fn [i v] (.setObject ps (inc i) v))
                                      params))))]
    (with-open [rs (.executeQuery stmt)]
      (let [meta (.getMetaData rs)
            cols (mapv (fn [i] (keyword (str/lower-case (.getColumnName meta i))))
                       (range 1 (inc (.getColumnCount meta))))]
        (loop [rows []]
          (if (.next rs)
            (recur (conj rows (zipmap cols (map #(.getObject rs ^int %) cols))))
            rows))))))

(defn- exec!
  "Run a statement, ignoring the (meaningless for DDL) update count."
  [^Connection conn ^String sql]
  (with-open [stmt (.createStatement conn)]
    (.execute stmt sql))
  nil)

;; ---------------------------------------------------------------------------
;; server introspection

(defn wal-level
  "The server's ``wal_level`` setting; logical replication requires
  ``logical``."
  [conn]
  (str/lower-case (:wal_level (first (query conn "SHOW wal_level" nil)))))

(defn current-wal-lsn
  "The server's current write-ahead log position, as a long LSN."
  [conn]
  (lsn/parse (:pg_current_wal_lsn (first (query conn "SELECT pg_current_wal_lsn()::text" nil)))))

;; ---------------------------------------------------------------------------
;; slots

(defn- row->slot
  [row]
  (-> row
      (update :active boolean)
      (update :restart_lsn #(some-> % lsn/parse))
      (update :confirmed_flush_lsn #(some-> % lsn/parse))))

(defn- slot-query
  [where]
  (str "SELECT slot_name, plugin, slot_type, active, active_pid,"
       " restart_lsn::text, confirmed_flush_lsn::text, wal_status"
       " FROM pg_replication_slots" where))

(defn slots
  "All replication slots the server holds, as maps with parsed LSNs."
  [conn]
  (mapv row->slot (query conn (slot-query "") nil)))

(defn slot-info
  "The slot named `name`, or nil.  Shape as in [[slots]]."
  [conn name]
  (row->slot (first (query conn (str (slot-query " WHERE slot_name = ?") "?") [name]))))

(defn slot-exists? [conn name]
  (boolean (seq (query conn "SELECT 1 FROM pg_replication_slots WHERE slot_name = ?" [name]))))

(defn create-slot!
  "Create a logical replication slot.  Returns ``{:slot-name ...
  :lsn ...}`` where :lsn (the slot's consistent point) is where a
  stream from this slot begins.  `opts`:

  :plugin    output plugin, default ``pgoutput``.

  Creating a slot pins WAL from that point: drop it when done, or
  the server retains WAL forever."
  ([conn name] (create-slot! conn name nil))
  ([conn name {:keys [plugin] :or {plugin "pgoutput"} :as _opts}]
   (let [row (first (query conn "SELECT slot_name, lsn::text FROM pg_create_logical_replication_slot(?, ?)"
                           [name plugin]))]
     {:slot-name (:slot_name row)
      :lsn (lsn/parse (:lsn row))})))

(defn drop-slot!
  "Drop a replication slot, freeing its WAL retention.  Fails (as
  Postgres does) when the slot is active."
  [conn name]
  (query conn "SELECT pg_drop_replication_slot(?)" [name])
  nil)

(defn ensure-slot!
  "The slot named `name`, created with `opts` if missing.  Idempotent
  bootstrap for consumers.  Returns slot-info (plus :created?)."
  ([conn name] (ensure-slot! conn name nil))
  ([conn name opts]
   (or (some-> (slot-info conn name) (assoc :created? false))
       (assoc (create-slot! conn name opts) :created? true))))

;; ---------------------------------------------------------------------------
;; publications

(defn publication-exists? [conn name]
  (boolean (seq (query conn "SELECT 1 FROM pg_publication WHERE pubname = ?" [name]))))

(defn publication-ddl
  "The CREATE PUBLICATION statement for `pub-name` and `opts`:

  :tables              vector of table names (``\"public.users\"``);
                       mutually exclusive with :all-tables?
  :all-tables?         publish every table (FOR ALL TABLES)
  :publish             operations to publish, subset of
                       #{:insert :update :delete :truncate}, default all
  :via-partition-root? publish partition leaf rows through the root
                       table (Postgres 13+)

  Exposed because the DDL string is what there is to unit-test."
  [pub-name {:keys [tables all-tables? publish via-partition-root?]
             :or {publish [:insert :update :delete :truncate]}}]
  (when (not= 1 (count (keep identity [(seq tables) all-tables?])))
    (throw (ex-info "Specify exactly one of :tables or :all-tables?"
                    {:tables tables :all-tables? all-tables?})))
  (let [ops (str/join ", " (map (fn [op] (str/upper-case (clojure.core/name op))) publish))
        with (cond-> []
               via-partition-root? (conj "publish_via_partition_root = true"))]
    (str "CREATE PUBLICATION " (quote-ident pub-name)
         (if all-tables?
           " FOR ALL TABLES"
           (str " FOR TABLE " (str/join ", " (map table-ident tables))))
         " WITH (publish = '" ops "'"
         (when (seq with) (str ", " (str/join ", " with)))
         ")")))

(defn create-publication!
  "Create a publication (see [[publication-ddl]] for opts)."
  [conn pub-name opts]
  (exec! conn (publication-ddl pub-name opts)))

(defn drop-publication!
  "Drop a publication.  Pass :if-exists? true to tolerate a missing
  one."
  [conn pub-name {:keys [if-exists?]}]
  (exec! conn (str "DROP PUBLICATION " (when if-exists? "IF EXISTS ")
                   (quote-ident pub-name))))

(defn publication-tables
  "The ``schema.table`` names a publication carries."
  [conn name]
  (mapv #(str (:schemaname %) "." (:tablename %))
        (query conn "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = ?"
               [name])))

(defn ensure-publication!
  "The publication named `name`, created with `opts` if missing.
  Returns {:created? bool :tables [...]}."
  [conn name opts]
  (if (publication-exists? conn name)
    {:created? false :tables (publication-tables conn name)}
    (do (create-publication! conn name opts)
        {:created? true :tables (publication-tables conn name)})))
