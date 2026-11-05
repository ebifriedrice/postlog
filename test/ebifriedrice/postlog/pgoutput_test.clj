(ns ebifriedrice.postlog.pgoutput-test
  "Tests build pgoutput messages as flat tagged segments so the byte
  fixtures stay readable and self-sizing."
  (:require [clojure.test :refer [deftest is testing]]
            [ebifriedrice.postlog.pgoutput :as pg])
  (:import (java.io ByteArrayOutputStream DataOutputStream)
           (java.nio.charset StandardCharsets)
           (java.time Instant)))

(defn- msg
  "Encode segments to bytes: [:b byte] [:w int16] [:i int32] [:l int64]
  [:s cstring] [:raw byte-array]."
  [& segments]
  (let [baos (ByteArrayOutputStream.)]
    (with-open [out (DataOutputStream. baos)]
      (doseq [[tag value] segments]
        (case tag
          :b (.writeByte out (long value))
          :w (.writeShort out (long value))
          :i (.writeInt out (unchecked-int value))
          :l (.writeLong out (long value))
          :s (do (.write out (.getBytes (str value) StandardCharsets/UTF_8))
                 (.write out 0))
          :raw (.write out ^bytes value))))
    (.toByteArray baos)))

(def pg-epoch-sec 946684800) ; 2000-01-01T00:00Z

(defn- instant-at
  "Instant for `micros` since the Postgres epoch."
  [micros]
  (Instant/ofEpochSecond (+ pg-epoch-sec (quot micros 1000000))
                         (* (rem micros 1000000) 1000)))

(deftest begin-test
  (testing "Begin carries the commit record LSN, commit time, and xid"
    (is (= {:op :begin
            :final-lsn 0x16B374D
            :commit-ts (instant-at 1500000)
            :xid 4815}
           (pg/decode (msg [:b \B] [:l 0x16B374D] [:l 1500000] [:i 4815])))))
  (testing "xids past 2^31 stay positive (unsigned read)"
    (is (= 3221225472
           (:xid (pg/decode (msg [:b \B] [:l 0] [:l 0] [:i 0xC0000000])))))))

(deftest commit-test
  (testing "Commit carries flags, both LSNs, and commit time"
    (is (= {:op :commit
            :flags 0
            :commit-lsn 0
            :end-lsn 0x100
            :commit-ts (instant-at 2500000)}
           (pg/decode (msg [:b \C] [:b 0] [:l 0] [:l 0x100] [:l 2500000]))))))

(deftest origin-test
  (testing "Origin names the upstream server a replayed txn came from"
    (is (= {:op :origin :commit-lsn 1234 :name "origin-a"}
           (pg/decode (msg [:b \O] [:l 1234] [:s "origin-a"]))))))

(deftest message-test
  (testing "logical-decoding messages carry prefix and content"
    (is (= {:op :message
            :transactional? true
            :lsn 55
            :prefix "myapp"
            :content "hi"}
           (pg/decode (msg [:b \M] [:b 1] [:l 55] [:s "myapp"]
                           [:i 2] [:raw (.getBytes "hi" StandardCharsets/UTF_8)]))))))

(deftest relation-test
  (testing "Relation describes a table: oid, name, replica identity, columns"
    (is (= {:op :relation
            :relation-id 16385
            :schema "public"
            :table "users"
            :replica-identity :default
            :columns [{:name "id" :key? true :type-oid 23 :type-mod -1}
                      {:name "email" :key? false :type-oid 25 :type-mod -1}
                      {:name "meta" :key? false :type-oid 114 :type-mod -1}]}
           (pg/decode (msg [:b \R]
                           [:i 16385]
                           [:s "public"]
                           [:s "users"]
                           [:b \d]
                           [:w 3]
                           [:b 1] [:s "id"] [:i 23] [:i -1]
                           [:b 0] [:s "email"] [:i 25] [:i -1]
                           [:b 0] [:s "meta"] [:i 114] [:i -1])))))
  (testing "replica identity chars map to keywords, unknown ones pass through"
    (is (= :full
           (:replica-identity (pg/decode (msg [:b \R] [:i 1] [:s "s"] [:s "t"] [:b \f] [:w 0])))))
    (is (= "x"
           (str (:replica-identity (pg/decode (msg [:b \R] [:i 1] [:s "s"] [:s "t"] [:b \x] [:w 0]))))))))

(deftest type-test
  (testing "Type announces a custom type's identity for relation columns"
    (is (= {:op :type
            :type-oid 2950
            :schema "public"
            :name "mood"}
           (pg/decode (msg [:b \Y] [:i 2950] [:s "public"] [:s "mood"]))))))

(deftest truncate-test
  (testing "one Truncate covers many relations with cascade/restart bits"
    (is (= {:op :truncate
            :cascade? true
            :restart-identity? true
            :relation-ids [16385 16390]}
           (pg/decode (msg [:b \T] [:i 2] [:b 3] [:i 16385] [:i 16390]))))
    (is (= {:op :truncate
            :cascade? false
            :restart-identity? false
            :relation-ids [16385]}
           (pg/decode (msg [:b \T] [:i 1] [:b 0] [:i 16385]))))))

(deftest unsupported-and-truncated-test
  (testing "unknown type bytes are rejected with the byte in the error"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"Unsupported pgoutput"
         (pg/decode (msg [:b \S])))
        "stream-start is proto v2 and unsupported")
    (is (= \S
           (-> (try (pg/decode (msg [:b \S]))
                    (catch clojure.lang.ExceptionInfo e (ex-data e)))
               :type-byte))))
  (testing "runs off the end of the buffer"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"Truncated"
         (pg/decode (msg [:b \B] [:l 1])))))
  (testing "non-bytes payload"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"bytes or a ByteBuffer"
         (pg/decode :nope)))))
