(ns ebifriedrice.postlog.replication
  "Open replication connections and stream pgoutput messages.

  pgjdbc exposes the walsender protocol on a connection opened with
  ``replication=database``: unwrap to ``org.postgresql.PGConnection``,
  build a logical stream against a slot, and receive the raw pgoutput
  payload of each WAL record.  The driver owns the protocol mechanics
  this library is happy not to reimplement: XLogData framing,
  keepalive replies, and periodic standby status updates reporting
  our flushed LSN.

  [[start!]] returns anything implementing [[LogicalStream]] -- by
  default a thin adapter over pgjdbc's stream, but the consumer loop
  only talks to the protocol, so tests drive it with fakes."
  (:require [clojure.string :as str]
            [ebifriedrice.postlog.admin :as admin])
  (:import (java.sql Connection DriverManager)
           (java.util.concurrent TimeUnit)
           [org.postgresql PGConnection]
           (org.postgresql.replication LogSequenceNumber PGReplicationStream)))

(defprotocol LogicalStream
  "One logical replication stream."
  (read-pending [stream]
    "The next pending pgoutput message's bytes, or nil when nothing
    is available right now.  Never blocks.")
  (ack! [stream lsn]
    "Report `lsn` as the flushed-and-applied position and push a
    status update to the server immediately.  Call only after the
    work belonging to everything up to `lsn` has succeeded.")
  (close! [stream]
    "End the stream.  The slot keeps its last confirmed position."))

(defn connect
  "Open a dedicated replication connection (``replication=database``)
  from a spec map or JDBC URL string.  The caller owns closing it."
  [spec]
  (let [{:keys [user password]} (when (map? spec) spec)]
    (DriverManager/getConnection (admin/replication-jdbc-url spec) user password)))

(defn- wrap-pg-stream
  [^PGReplicationStream s]
  (reify LogicalStream
    (read-pending [_] (.readPending s))
    (ack! [_ lsn]
      (let [lsn' (LogSequenceNumber/valueOf (long lsn))]
        (.setFlushedLSN s lsn')
        (.setAppliedLSN s lsn')
        (.forceUpdateStatus s)))
    (close! [_] (.close s))))

(defn start!
  "Start a logical stream on an open replication `conn`.

  Opts:
  :slot-name          (required) the slot to consume
  :publications       (required) publication name(s), string or coll
  :proto-version      pgoutput protocol version, default 1 (text
                      values; the only version postlog decodes)
  :status-interval-ms standby status cadence, default 5000
  :binary?            request binary tuple values (proto v2+; off)
  :messages?          forward pg_logical_emit_message payloads
                      (proto v2+; off)

  Starts from the slot's own confirmed position; the server decides
  where that is, which is what makes resume-on-restart work."
  [^Connection conn {:keys [slot-name publications proto-version
                            status-interval-ms binary? messages?]}]
  (let [pubs (if (string? publications) [publications] publications)
        builder (-> (.getReplicationAPI (.unwrap conn PGConnection))
                    (.replicationStream)
                    (.logical)
                    (.withSlotName (str slot-name))
                    (.withStartPosition LogSequenceNumber/INVALID_LSN)
                    (.withSlotOption "proto_version" (int (or proto-version 1)))
                    (.withSlotOption "publication_names" (str/join "," (map str pubs)))
                    (.withStatusInterval (int (or status-interval-ms 5000))
                                         TimeUnit/MILLISECONDS)
                    ;; Keep pgjdbc's safe auto-advance: it only moves the
                    ;; flushed LSN past WAL we already explicitly confirmed,
                    ;; bridging idle stretches so the server can recycle WAL.
                    (.withAutomaticFlush true))
        builder (if binary? (.withSlotOption builder "binary" true) builder)
        builder (if messages? (.withSlotOption builder "messages" true) builder)]
    (wrap-pg-stream (.start builder))))
