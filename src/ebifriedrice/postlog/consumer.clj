(ns ebifriedrice.postlog.consumer
  "The replication consumer loop: open a stream, assemble decoded
  messages into transactions, deliver them to handlers, and ack the
  flushed LSN only after delivery succeeds.

  Delivery is ordered and at-least-once.  The slot's confirmed
  position advances only at transaction boundaries after the handler
  returns, so a handler failure or a crash redelivers from the last
  acked transaction; handlers must tolerate (or dedupe) duplicates.

  ```clojure
  (def c (consumer/start!
          {:db           {:host \"localhost\" :database \"app\"
                          :user \"repl\" :password \"...\"}
           :slot-name    \"my_slot\"
           :publication-name \"my_pub\"
           :tables       [\"public.users\"]
           :on-change    (fn [change] ...)       ; per change
           ;; and/or
           :on-transaction (fn [tx] ...)}))      ; per commit
  (consumer/stop! c)
  ```

  Connections (exactly one source is required):

  :db        spec map; postlog opens and owns both the replication
             and admin connections, closing them on stop.
  :conn-fn   (fn [] Connection); called for every (re)connect and
             must return a fresh, dedicated, un-pooled replication
             connection (``replication=database``) which postlog
             never closes.  A bare connection instance is not
             accepted on purpose: a replication connection that dies
             mid-stream cannot be revived -- resuming the slot means
             a *fresh* connection, which only a factory can produce.
  :admin-conn  optional Connection for slot/publication bootstrap;
             never closed by postlog.  Without :db or :admin-conn,
             the slot and publication must already exist.

  :stream-fn  test escape hatch (fn [] LogicalStream); bypasses
              connection handling entirely.

  :binary?    stream values in the binary wire format (default
              true); false selects text.  decode/decode-value
              decodes both, so the choice matters mainly for custom
              value-decoders and for unknown types, which pass
              through as raw bytes (binary) or raw text (text).

  Handlers run on the consumer's single thread, one at a time --
  that is what keeps ordering and provides backpressure.  Handler
  exceptions retry with exponential backoff (see :retry) and fail
  the consumer when exhausted; a failed consumer acks nothing past
  the failure.  Connection loss reconnects with the same backoff,
  and the slot resumes from its confirmed position.

  Transactions with no delivered changes (nothing published, or only
  filtered operations) still ack -- the position must advance -- but
  do not invoke :on-transaction."
  (:require [ebifriedrice.postlog.admin :as admin]
            [ebifriedrice.postlog.lsn :as lsn]
            [ebifriedrice.postlog.pgoutput :as pg]
            [ebifriedrice.postlog.relations :as relations]
            [ebifriedrice.postlog.replication :as replication])
  (:import (java.sql Connection SQLException)))

(def ^:private defaults
  {:poll-interval-ms 100
   :status-interval-ms 5000
   :retry {:retries 8 :base-ms 100}
   :ensure-slot? true
   :ensure-publication? true
   :binary? true})

(def ^:private max-backoff-ms 60000)

;; ---------------------------------------------------------------------------
;; handler invocation with retry

(defn- call-handler!
  "Invoke (f arg), retrying on exception with exponential backoff.
  Throws the last exception once :retries attempts are used up
  (:retries :inf never gives up).  Gives up immediately when the
  consumer is stopping, so shutdown is not held hostage by retries."
  [ctrl {:keys [retry]} f arg]
  (let [{:keys [retries base-ms]} retry]
    (loop [attempt 0, delay base-ms]
      (if-let [e (try (f arg) nil (catch Exception t t))]
        (if (or (not (:run? @ctrl))
                (and (not= :inf retries) (>= attempt retries)))
          (throw e)
          (do (Thread/sleep delay)
              (recur (inc attempt) (min (* 2 delay) max-backoff-ms))))))))

;; ---------------------------------------------------------------------------
;; message handling

(defn- shape-truncate
  "Truncate degrades to raw relation ids when the stream never
  announced those relations (pgoutput may truncate tables it has not
  described in this session)."
  [cache msg]
  (try
    (relations/shape-change cache msg)
    (catch clojure.lang.ExceptionInfo _
      {:op :truncate
       :relation-ids (:relation-ids msg)
       :cascade? (:cascade? msg)
       :restart-identity? (:restart-identity? msg)})))

(defn- deliver-txn!
  "Deliver a completed transaction and checkpoint its end LSN."
  [ctrl opts stream {:keys [begin commit changes]}]
  (when-let [f (:on-transaction opts)]
    (when (seq changes)
      (call-handler! ctrl opts f {:type :transaction
                                  :xid (:xid begin)
                                  :commit-lsn (lsn/format (:commit-lsn commit))
                                  :end-lsn (lsn/format (:end-lsn commit))
                                  :commit-ts (:commit-ts commit)
                                  :changes changes})))
  (replication/ack! stream (:end-lsn commit))
  (swap! ctrl (fn [c] (-> c
                          (update :txns inc)
                          (assoc :confirmed-lsn (lsn/format (:end-lsn commit)))))))

(defn- handle!
  "Fold one decoded message into the session state, delivering
  handlers and acks as side effects.  Returns [relations current-tx]."
  [ctrl opts stream {:keys [relations tx]} msg]
  (case (:op msg)
    :relation [(relations/apply-message relations msg) tx]
    (:type :origin) [relations tx]
    :begin [relations {:begin msg :changes []}]
    :message (if (:transactional? msg)
               [relations (update tx :changes conj msg)]
               (do (when-let [f (:on-change opts)]
                     (call-handler! ctrl opts f msg))
                   (replication/ack! stream (:lsn msg))
                   [relations tx]))
    (:insert :update :delete)
    (let [change (relations/shape-change relations msg
                                         {:value-decoder (:value-decoder opts)})]
      (when-let [f (:on-change opts)]
        (call-handler! ctrl opts f change))
      [relations (if tx (update tx :changes conj change) tx)])
    :truncate (let [change (shape-truncate relations msg)]
                (when-let [f (:on-change opts)]
                  (call-handler! ctrl opts f change))
                [relations (if tx (update tx :changes conj change) tx)])
    :commit (do (deliver-txn! ctrl opts stream {:begin (:begin tx)
                                                :commit msg
                                                :changes (:changes tx)})
                [relations nil])
    ;; unreachable: pgoutput/decode rejects unknown types
    [relations tx]))

;; ---------------------------------------------------------------------------
;; stream session + reconnect loop

(defn- session!
  "Run one stream connection to its end: a stop request or handler
  failure (returns normally), or a connection error (throws
  SQLException upward for the reconnect loop)."
  [ctrl {:keys [poll-interval-ms] :as opts} open!]
  (let [{:keys [stream close]} (open!)]
    (try
      (loop [relations {}
             tx nil]
        (when (:run? @ctrl)
          (if-let [payload (replication/read-pending stream)]
            (let [[relations' tx'] (handle! ctrl
                                            opts
                                            stream
                                            {:relations relations :tx tx}
                                            (pg/decode payload))]
              (recur relations' tx'))
            (do (Thread/sleep (long poll-interval-ms))
                (recur relations tx)))))
      (catch SQLException e
        (throw e))
      (catch Exception e
        (swap! ctrl (fn [c] (cond-> (assoc c :error e)
                              (:run? c) (assoc :state :failed))))
        (throw e))
      (finally
        (try (replication/close! stream) (catch Exception _))
        (close)))))

(defn- reconnect-loop!
  "Session loop with reconnect-and-backoff on connection errors.
  session! returns normally when it stopped or failed for good;
  only a SQLException means the connection died and a fresh one
  should be opened (Clojure disallows recur from inside catch, so
  the error travels out through a binding instead)."
  [ctrl {:keys [retry] :as opts} open!]
  (loop [delay (:base-ms retry)]
    (when (:run? @ctrl)
      (let [connect-error (try
                            (session! ctrl opts open!)
                            nil
                            (catch SQLException e e))]
        (when (and connect-error (:run? @ctrl))
          (swap! ctrl assoc :last-connect-error connect-error)
          (Thread/sleep delay)
          (recur (min (* 2 delay) max-backoff-ms)))))))

;; ---------------------------------------------------------------------------
;; bootstrap + start

(defn- bootstrap!
  "Create the slot and publication when asked and possible; no-op
  without admin access (then they must already exist)."
  [ctrl opts]
  (let [with-admin (fn [f]
                     (cond
                       (:admin-conn opts) (f (:admin-conn opts))
                       (:db opts) (with-open [conn (admin/connect (:db opts))]
                                    (f conn))
                       :else nil))]
    (when (:ensure-slot? opts)
      (when-let [info (with-admin #(admin/ensure-slot! % (:slot-name opts)))]
        (swap! ctrl assoc :slot-created? (:created? info))))
    (when (:ensure-publication? opts)
      (when-let [info (with-admin #(admin/ensure-publication!
                                    % (:publication-name opts)
                                    (if (seq (:tables opts))
                                      {:tables (:tables opts)}
                                      {:all-tables? true})))]
        (swap! ctrl assoc :publication-created? (:created? info))))))

(defn- resolve-open!
  "The stream opener: returns {:stream LogicalStream :close (fn [])}
  where :close releases everything postlog owns."
  [opts]
  (let [stream-opts {:slot-name (:slot-name opts)
                     :publications (:publication-name opts)
                     :status-interval-ms (:status-interval-ms opts)
                     :binary? (:binary? opts)
                     :proto-version (:proto-version opts)}]
    (cond
      (:stream-fn opts)
      (fn [] {:stream ((:stream-fn opts)) :close #()})

      (:conn-fn opts)
      (fn [] {:stream (replication/start! ((:conn-fn opts)) stream-opts)
              :close #()})

      (:db opts)
      (fn [] (let [conn (replication/connect (:db opts))]
               {:stream (replication/start! conn stream-opts)
                :close #(.close ^Connection conn)}))

      :else (throw (ex-info "consumer/start! needs :db, :conn-fn, or :stream-fn"
                            {:opts (keys opts)})))))

(defn start!
  "Start a consumer for `opts` (see the namespace docs for the full
  option reference).  Returns a handle:

  ``{:stop! (fn []) :await (fn [timeout-ms?]) :status (fn [])}``

  stop! is idempotent and asynchronous; await blocks until the
  consumer stops (or the timeout passes, returning nil) and yields
  the final status map {:state :stopped/:failed, :error?, :txns,
  :confirmed-lsn, ...}.  stop! then await is the graceful-shutdown
  pair."
  [opts]
  (let [opts (merge defaults opts)]
    (when-not (or (:on-change opts) (:on-transaction opts))
      (throw (ex-info "consumer/start! needs :on-change and/or :on-transaction"
                      {:opts (keys opts)})))
    (when-not (or (:db opts) (:conn-fn opts) (:stream-fn opts))
      (throw (ex-info "consumer/start! needs :db, :conn-fn, or :stream-fn"
                      {:opts (keys opts)})))
    (when (and (not (:stream-fn opts))
               (or (not (:slot-name opts)) (not (:publication-name opts))))
      (throw (ex-info "consumer/start! needs :slot-name and :publication-name"
                      {:slot-name (:slot-name opts)
                       :publication-name (:publication-name opts)})))
    (let [ctrl (atom {:run? true
                      :state :running
                      :txns 0
                      :confirmed-lsn nil})
          open! (resolve-open! opts)
          done (promise)
          run (fn []
                (try
                  (bootstrap! ctrl opts)
                  (reconnect-loop! ctrl opts open!)
                  (catch Throwable t
                    (swap! ctrl assoc :state :failed :error t))
                  (finally
                    (swap! ctrl (fn [c]
                                  (cond-> (assoc c :run? false)
                                    (contains? #{:running :stopping} (:state c))
                                    (assoc :state :stopped))))
                    (deliver done @ctrl))))
          thread (doto (Thread. run) (.setDaemon true))
          _ (.start thread)]
      {:stop! (fn []
                (swap! ctrl (fn [c] (if (:run? c)
                                      (-> c (assoc :run? false) (assoc :state :stopping))
                                      c))))
       :await (fn await
                ([] @done)
                ([timeout-ms] (deref done timeout-ms nil)))
       :status (fn [] @ctrl)
       :thread thread})))

(defn stop!
  "Request a graceful stop: the loop finishes its current poll cycle,
  closes its stream and any owned connections, and acks nothing
  past what it already delivered.  Idempotent."
  [consumer]
  ((:stop! consumer)))
