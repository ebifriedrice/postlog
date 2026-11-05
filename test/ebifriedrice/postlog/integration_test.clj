(ns ebifriedrice.postlog.integration-test
  "End-to-end tests against a live Postgres server.

  By default the suite starts its own throwaway container (compose.yaml:
  postgres 17 with wal_level=logical on localhost:55432) via docker
  compose, falling back to rootless podman with the same settings.  Set
  POSTLOG_TEST_HOST/PORT/USER/PASSWORD/DATABASE to run against an
  existing server instead (it must run wal_level=logical), or
  POSTLOG_INTEGRATION=0 to skip."
  (:require [clojure.java.process :as process]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [ebifriedrice.postlog.admin :as admin]
            [ebifriedrice.postlog.consumer :as consumer]
            [ebifriedrice.postlog.decode :as decode]
            [ebifriedrice.postlog.lsn :as lsn]
            [ebifriedrice.postlog.replication :as replication])
  (:import (java.sql Connection)
           (java.time OffsetDateTime ZoneOffset)))

(defn- env [^String k] (System/getenv k))

(def managed-port 55432)

(def db-spec
  {:host (or (env "POSTLOG_TEST_HOST") "localhost")
   :port (or (some-> (env "POSTLOG_TEST_PORT") Long/parseLong) managed-port)
   :user (or (env "POSTLOG_TEST_USER") "postgres")
   :password (or (env "POSTLOG_TEST_PASSWORD") "postgres")
   :database (or (env "POSTLOG_TEST_DATABASE") "postgres")})

(defn- external-server? []
  (some env ["POSTLOG_TEST_HOST" "POSTLOG_TEST_PORT" "POSTLOG_TEST_USER"
             "POSTLOG_TEST_PASSWORD" "POSTLOG_TEST_DATABASE"]))

(defn- sh!
  "Run a command; true when it exits zero."
  [& args]
  (try (apply process/exec (map str args)) true
       (catch Exception _ false)))

(defn- connectable?
  "True when db-spec accepts a connection and a trivial query."
  []
  (try
    (with-open [^Connection conn (admin/connect db-spec)]
      (with-open [stmt (.createStatement conn)]
        (.execute stmt "SELECT 1")))
    true
    (catch Exception _ false)))

(defn- wait-for
  "Truth of (pred) within timeout-ms, else nil."
  [pred timeout-ms]
  (let [deadline (+ (System/nanoTime) (* timeout-ms 1000000))]
    (loop []
      (or (pred)
          (when (< (System/nanoTime) deadline)
            (Thread/sleep 100)
            (recur))))))

(def ^:private container "postlog-it-pg")

(defn- start-managed!
  "Bring the throwaway server up.  Returns the teardown fn, or nil
  (after saying why) when no engine could start one."
  []
  (cond
    (and (sh! "docker" "compose" "-f" "compose.yaml" "up" "-d" "--wait")
         (connectable?))
    #(sh! "docker" "compose" "-f" "compose.yaml" "down" "-v")

    (and (sh! "podman" "rm" "-f" container)
         (sh! "podman" "run" "-d" "--name" container
              "-p" (str managed-port ":5432")
              "-e" "POSTGRES_USER=postgres"
              "-e" "POSTGRES_PASSWORD=postgres"
              "-e" "POSTGRES_DB=postgres"
              "postgres:17"
              "postgres" "-c" "wal_level=logical"
              "-c" "max_replication_slots=10"
              "-c" "max_wal_senders=10")
         (wait-for connectable? 60000))
    #(sh! "podman" "rm" "-f" container)

    :else
    (do (println "integration tests: no container engine could start a"
                 "server (tried docker compose and podman); set"
                 "POSTLOG_TEST_* for an existing server or"
                 "POSTLOG_INTEGRATION=0 to silence this")
        nil)))

(def table "postlog_it.things")

(def created-at-instant
  (.toInstant (OffsetDateTime/of 2026 11 5 12 34 56 0 ZoneOffset/UTC)))

(defn- exec!
  "Run SQL with optional params on a fresh admin connection.  Uses
  execute (not executeUpdate) so statements that return results --
  pg_terminate_backend -- work too."
  [sql & params]
  (with-open [^Connection conn (admin/connect db-spec)]
    (with-open [ps (.prepareStatement conn sql)]
      (dorun (map-indexed (fn [i v] (.setObject ps (inc i) v)) params))
      (.execute ps))))

(defn- with-admin-info
  "slot-info through a fresh admin connection."
  [slot-name]
  (with-open [^Connection conn (admin/connect db-spec)]
    (admin/slot-info conn slot-name)))

(defn- cleanup!
  "Drop the test's slot and publication.  pg_drop_replication_slot
  blocks indefinitely while the slot is active -- and a walsender can
  outlive its client by a moment -- so terminate a lingering backend
  first and drop only once the slot is quiet."
  [slot-name pub-name]
  (with-open [^Connection conn (admin/connect db-spec)]
    (admin/drop-publication! conn pub-name {:if-exists? true})
    (loop [attempts 50]
      (when-let [info (admin/slot-info conn slot-name)]
        (cond
          (:active info)
          (do (some->> (:active_pid info)
                       (exec! "SELECT pg_terminate_backend(?)"))
              (Thread/sleep 100)
              (recur (dec attempts)))

          (pos? attempts)
          (do (try (admin/drop-slot! conn slot-name)
                   (catch Exception _ (Thread/sleep 100)))
              (when (admin/slot-exists? conn slot-name)
                (recur (dec attempts))))))))
  nil)

(defn- unique-name [label]
  (str "postlog_it_" label "_" (Long/toString (System/currentTimeMillis) 36)))

(defn- bootstrap-objects!
  "Create the slot and publication up front so test writes race
  nothing: a slot only streams WAL from its creation point on."
  [slot-name pub-name]
  (with-open [^Connection conn (admin/connect db-spec)]
    (let [level (admin/wal-level conn)]
      (when-not (= "logical" level)
        (throw (ex-info "Server must run wal_level=logical"
                        {:wal-level level
                         :hint "point POSTLOG_TEST_* at a logical server"}))))
    (admin/ensure-slot! conn slot-name)
    (admin/ensure-publication! conn pub-name {:tables [table]})))

(defn- start-consumer!
  "Consumer over `slot-name`/`pub-name`, collecting :on-change
  changes into a fresh atom.  Returns [consumer changes-atom]."
  ([slot-name pub-name]
   (let [changes (atom [])]
     [(start-consumer! slot-name pub-name changes)
      changes]))
  ([slot-name pub-name changes]
   (consumer/start!
    {:db db-spec
     :slot-name slot-name
     :publication-name pub-name
     :tables [table]
     :value-decoder decode/decode-value
     :poll-interval-ms 10
     :retry {:base-ms 50 :retries 10}
     :on-change (fn [change] (swap! changes conj change))})))

(defn- insert-raw!
  [id label]
  (exec! (str "INSERT INTO " table " VALUES (?, ?, true, "
              "'2026-11-05 12:34:56+00', '{a,b}')")
         (long id) label))

(defn- stop-and-await! [c]
  (consumer/stop! c)
  ((:await c) 5000))

(defn- changes-for
  "Changes whose row id matches, in delivery order."
  [changes-atom id]
  (filter (fn [c] (or (= id (get-in c [:after "id"]))
                      (= id (get-in c [:before "id"]))))
          @changes-atom))

(use-fixtures
  :once
  (fn [f]
    (if (= "0" (env "POSTLOG_INTEGRATION"))
      (println "POSTLOG_INTEGRATION=0: skipping live-server integration tests")
      (let [external? (external-server?)
            teardown (when-not external? (start-managed!))]
        (when (or external? teardown)
          (exec! "CREATE SCHEMA IF NOT EXISTS postlog_it")
          (exec! (str "CREATE TABLE IF NOT EXISTS " table " ("
                      " id integer PRIMARY KEY,"
                      " label text,"
                      " active boolean,"
                      " created_at timestamptz,"
                      " tags text[])"))
          (try
            (f)
            (finally
              (exec! "DROP SCHEMA IF EXISTS postlog_it CASCADE")
              (when teardown (teardown)))))))))

(deftest end-to-end-cycle-test
  (testing "insert, update, delete, truncate arrive decoded and in order"
    (let [slot (unique-name "e2e") pub (unique-name "e2e")
          _ (bootstrap-objects! slot pub)
          [c changes] (start-consumer! slot pub)]
      (try
        (exec! (str "INSERT INTO " table
                    " VALUES (1, 'first', true, '2026-11-05 12:34:56+00', '{a,b}')"))
        (exec! (str "UPDATE " table " SET label = 'renamed' WHERE id = 1"))
        (exec! (str "DELETE FROM " table " WHERE id = 1"))
        (exec! (str "TRUNCATE " table))
        (is (wait-for #(= :truncate (:op (last @changes))) 10000)
            "truncate arrives last")
        (finally
          (stop-and-await! c)
          (cleanup! slot pub)))
      (let [ops (mapv :op @changes)
            insert (first (changes-for changes 1))
            update (second (changes-for changes 1))]
        (is (= [:insert :update :delete :truncate] ops))
        (testing "insert values decode through the value-decoder"
          (is (= 1 (get-in insert [:after "id"])))
          (is (= "first" (get-in insert [:after "label"])))
          (is (true? (get-in insert [:after "active"])))
          (is (= ["a" "b"] (get-in insert [:after "tags"])))
          (is (= created-at-instant
                 (.. ^OffsetDateTime (get-in insert [:after "created_at"])
                     toInstant))))
        (is (nil? (:before update))
            "default replica identity: non-key update carries no before image")
        (is (= "renamed" (get-in update [:after "label"])))))))

(deftest restart-resume-test
  (testing "a stopped and restarted consumer loses nothing"
    (let [slot (unique-name "resume") pub (unique-name "resume")
          _ (bootstrap-objects! slot pub)]
      (insert-raw! 10 "while-running")
      (let [[c changes] (start-consumer! slot pub)]
        (is (wait-for #(seq (changes-for changes 10)) 10000))
        (stop-and-await! c))
      (insert-raw! 11 "while-stopped-1")
      (insert-raw! 12 "while-stopped-2")
      (let [[c changes] (start-consumer! slot pub)]
        (is (wait-for #(and (seq (changes-for changes 11))
                            (seq (changes-for changes 12)))
                      10000)
            "rows written while stopped are delivered on restart")
        (stop-and-await! c)
        (cleanup! slot pub)
        (is (= [11 12] (mapv #(get-in % [:after "id"])
                             (concat (changes-for changes 11)
                                     (changes-for changes 12))))
            "no duplicates either: the acked position survived the restart")))))

(deftest acking-advances-slot-test
  (testing "delivered transactions advance the slot's confirmed position"
    (let [slot (unique-name "ack") pub (unique-name "ack")
          _ (bootstrap-objects! slot pub)
          [c changes] (start-consumer! slot pub)]
      (insert-raw! 20 "acked")
      (is (wait-for #(seq (changes-for changes 20)) 10000))
      (is (wait-for #(pos? (or (:confirmed_flush_lsn (with-admin-info slot)) 0))
                    5000)
          "confirmed_flush_lsn is set")
      (let [confirmed (:confirmed_flush_lsn (with-admin-info slot))
            status-lsn (lsn/parse (:confirmed-lsn ((:status c))))]
        (is (>= confirmed status-lsn)
            "the slot's position is at least what the consumer reported"))
      (stop-and-await! c)
      (cleanup! slot pub))))

(deftest reconnect-after-terminate-test
  (testing "a terminated walsender connection recovers and keeps delivering"
    (let [slot (unique-name "recon") pub (unique-name "recon")
          _ (bootstrap-objects! slot pub)
          [c changes] (start-consumer! slot pub)]
      (insert-raw! 30 "before-terminate")
      (is (wait-for #(seq (changes-for changes 30)) 10000))
      (let [pid (wait-for #(some-> (with-admin-info slot) :active_pid) 5000)]
        (is pid "walsender backend is visible")
        (exec! "SELECT pg_terminate_backend(?)" pid))
      (insert-raw! 31 "after-terminate")
      (is (wait-for #(seq (changes-for changes 31)) 15000)
          "consumer reconnects and delivers")
      (stop-and-await! c)
      (cleanup! slot pub)
      (is (= [30 31] (mapv #(get-in % [:after "id"]) @changes))))))

(deftest conn-fn-ownership-test
  (testing "a caller-supplied connection factory is used and never closed"
    (let [slot (unique-name "connfn") pub (unique-name "connfn")
          _ (bootstrap-objects! slot pub)
          conns (atom [])
          conn-fn (fn []
                    (let [conn (replication/connect db-spec)]
                      (swap! conns conj conn)
                      conn))
          changes (atom [])
          c (consumer/start!
             {:conn-fn conn-fn
              :slot-name slot
              :publication-name pub
              :value-decoder decode/decode-value
              :poll-interval-ms 10
              :on-change (fn [change] (swap! changes conj change))})]
      (try
        (insert-raw! 40 "via-conn-fn")
        (is (wait-for #(seq (changes-for changes 40)) 10000)
            "delivers through the factory's connection")
        (stop-and-await! c)
        (is (= 1 (count @conns)) "one connection, no reconnect churn")
        (is (not-any? (fn [^Connection conn] (.isClosed conn)) @conns)
            "postlog leaves caller connections open")
        (finally
          (dorun (map (fn [^Connection conn] (.close conn)) @conns))
          (cleanup! slot pub))))))
