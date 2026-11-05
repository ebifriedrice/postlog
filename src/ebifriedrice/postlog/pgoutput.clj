(ns ebifriedrice.postlog.pgoutput
  "Decode the binary stream produced by Postgres' ``pgoutput`` logical
  replication output plugin.

  Each message starts with a one-byte type tag followed by
  big-endian fixed-width integers and NUL-terminated (cstring) text.
  [[decode]] turns one message's bytes into a map carrying an ``:op``
  keyword plus the fields of that message type; LSNs are longs (see
  [[ebifriedrice.postlog.lsn]]) and commit timestamps are
  ``java.time.Instant``s.

  Tuple values arrive in one of four encodings, exposed as:
  ``nil`` (SQL NULL), a UTF-8 string (text format), a byte array
  (binary format, proto v2+ only), or [[unchanged-toast]] when a TOASTed
  value was not modified by the change and the server skipped sending
  it.

  This decoder covers protocol version 1 message layouts, which is
  what postlog streams with.  The in-progress-transaction stream
  messages (proto v2+) are rejected with a clear error rather than
  misparsed."
  (:import (java.nio ByteBuffer)
           (java.nio.charset StandardCharsets)
           (java.time Instant)))

(def unchanged-toast
  "Marker for a TOASTed column the server did not send because the
  change did not touch it.  Only Update of a table with large values
  produces these; re-read the row if you need the real value."
  ::unchanged-toast)

(def ^:private pg-epoch-micros
  "Postgres timestamps count microseconds since 2000-01-01 UTC."
  946684800000000)

(defn- micros->instant [^long us]
  (let [total (+ us pg-epoch-micros)]
    (Instant/ofEpochSecond (quot total 1000000) (* (rem total 1000000) 1000))))

(defn- ->buffer
  [payload]
  (cond
    (instance? ByteBuffer payload) payload
    (bytes? payload) (ByteBuffer/wrap payload)
    :else (throw (ex-info "pgoutput payload must be bytes or a ByteBuffer"
                          {:payload (type payload)}))))

(defn- uint32 [^ByteBuffer b]
  (bit-and (.getInt b) 0xFFFFFFFF))

(defn- read-cstring
  [^ByteBuffer b]
  (loop [i 0]
    (if (zero? (.get b (+ (.position b) i)))
      (let [bs (byte-array i)]
        (.get b bs)
        (.get b)
        (String. bs StandardCharsets/UTF_8))
      (recur (inc i)))))

(defn- read-tuple-value
  "Read one TupleData column value: ``n`` NULL, ``u`` unchanged TOAST,
  ``t`` text, ``b`` binary."
  [^ByteBuffer b]
  (let [subtype (char (.get b))]
    (case subtype
      \n nil
      \u unchanged-toast
      (\t \b) (let [len (.getInt b)
                    bs (byte-array len)]
                (.get b bs)
                (if (= \t subtype)
                  (String. bs StandardCharsets/UTF_8)
                  bs))
      (throw (ex-info "Unknown tuple value subtype" {:subtype subtype})))))

(defn- read-tuple
  "Read a TupleData: Int16 column count, then that many typed values."
  [^ByteBuffer b]
  (let [n (.getShort b)]
    (vec (repeatedly n #(read-tuple-value b)))))

(def ^:private replica-identities
  "pg_class.relreplident values sent in Relation messages."
  {\d :default \n :nothing \f :full \i :index})

(defn- read-relation
  [^ByteBuffer b]
  (let [relation-id (uint32 b)
        schema (read-cstring b)
        table (read-cstring b)
        replica-identity (char (bit-and (.get b) 0xFF))
        n (.getShort b)
        columns (vec (repeatedly n
                                 (fn []
                                   (let [flags (bit-and (.get b) 0xFF)]
                                     {:name (read-cstring b)
                                      :key? (bit-test flags 0)
                                      :type-oid (uint32 b)
                                      :type-mod (.getInt b)}))))]
    {:op :relation
     :relation-id relation-id
     :schema schema
     :table table
     :replica-identity (get replica-identities
                            replica-identity replica-identity)
     :columns columns}))

(defn decode
  "Decode one pgoutput message from `payload` (bytes or ByteBuffer,
  positioned at the type byte) into a map with an ``:op`` keyword.
  Throws ex-info for message types postlog does not handle."
  [payload]
  (let [b (->buffer payload)]
    (try
      (case (char (.get b))
        \B {:op :begin
            :final-lsn (.getLong b)
            :commit-ts (micros->instant (.getLong b))
            :xid (uint32 b)}
        \C {:op :commit
            :flags (bit-and (.get b) 0xFF)
            :commit-lsn (.getLong b)
            :end-lsn (.getLong b)
            :commit-ts (micros->instant (.getLong b))}
        \O {:op :origin
            :commit-lsn (.getLong b)
            :name (read-cstring b)}
        \R (read-relation b)
        \Y {:op :type
            :type-oid (uint32 b)
            :schema (read-cstring b)
            :name (read-cstring b)}
        \T (let [n (.getInt b)
                 flags (bit-and (.get b) 0xFF)]
             {:op :truncate
              :cascade? (bit-test flags 0)
              :restart-identity? (bit-test flags 1)
              :relation-ids (vec (repeatedly n #(uint32 b)))})
        \M (let [flags (bit-and (.get b) 0xFF)
                 lsn (.getLong b)
                 prefix (read-cstring b)
                 len (.getInt b)
                 content (byte-array len)]
             (.get b content)
             {:op :message
              :transactional? (bit-test flags 0)
              :lsn lsn
              :prefix prefix
              :content (String. content StandardCharsets/UTF_8)})
        (throw (ex-info "Unsupported pgoutput message type"
                        {:type-byte (char (bit-and (.get b 0) 0xFF))})))
      (catch java.nio.BufferUnderflowException _
        (throw (ex-info "Truncated pgoutput message" {})))
      (catch IndexOutOfBoundsException _
        (throw (ex-info "Truncated pgoutput message" {}))))))
