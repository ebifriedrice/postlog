(ns ebifriedrice.postlog.pgoutput-test
  (:require [clojure.test :refer [deftest is testing]]
            [ebifriedrice.postlog.pgoutput :as pg])
  (:import (java.nio ByteBuffer)
           (java.time Instant)))

(defn- buf
  "Allocate a ByteBuffer and apply size/put fns to build a message."
  [size & body]
  (let [b (ByteBuffer/allocate size)]
    ((apply juxt body) b)
    (.flip b)
    (.array b)))

(defn- put-byte [x] (fn [^ByteBuffer b] (.put b (byte x))))
(defn- put-short [x] (fn [^ByteBuffer b] (.putShort b (short x))))
(defn- put-int [x] (fn [^ByteBuffer b] (.putInt b (unchecked-int x))))
(defn- put-long [x] (fn [^ByteBuffer b] (.putLong b (long x))))
(defn- put-cstring [^String s]
  (fn [^ByteBuffer b]
    (.put b (.getBytes s java.nio.charset.StandardCharsets/UTF_8))
    (.put b (byte 0))))
(defn- put-bytes [bs] (fn [^ByteBuffer b] (.put b bs)))

(def pg-epoch-sec 946684800) ; 2000-01-01T00:00Z

(defn- instant-at
  "Instant for `micros` since the Postgres epoch."
  [micros]
  (Instant/ofEpochSecond (+ pg-epoch-sec (quot micros 1000000))
                         (* (rem micros 1000000) 1000)))

(deftest begin-test
  (testing "Begin carries the commit record LSN, commit time, and xid"
    (let [msg (pg/decode (buf 21
                              (put-byte \B)
                              (put-long 0x16B374D)
                              (put-long 1500000)
                              (put-int 4815)))]
      (is (= {:op :begin
              :final-lsn 0x16B374D
              :commit-ts (instant-at 1500000)
              :xid 4815}
             msg))))
  (testing "xids past 2^31 stay positive (unsigned read)"
    (is (= 3221225472
           (:xid (pg/decode (buf 21
                                 (put-byte \B)
                                 (put-long 0)
                                 (put-long 0)
                                 (put-int 0xC0000000))))))))

(deftest commit-test
  (testing "Commit carries flags, both LSNs, and commit time"
    (let [msg (pg/decode (buf 26
                              (put-byte \C)
                              (put-byte 0)
                              (put-long 0)
                              (put-long 0x100)
                              (put-long 2500000)))]
      (is (= {:op :commit
              :flags 0
              :commit-lsn 0
              :end-lsn 0x100
              :commit-ts (instant-at 2500000)}
             msg)))))

(deftest origin-test
  (testing "Origin names the upstream server a replayed txn came from"
    (is (= {:op :origin :commit-lsn 1234 :name "origin-a"}
           (pg/decode (buf 18
                           (put-byte \O)
                           (put-long 1234)
                           (put-cstring "origin-a")))))))

(deftest message-test
  (testing "logical-decoding messages carry prefix and content"
    (let [content (.getBytes "hi" java.nio.charset.StandardCharsets/UTF_8)
          msg (pg/decode (buf 64
                              (put-byte \M)
                              (put-byte 1)
                              (put-long 55)
                              (put-cstring "myapp")
                              (put-int (alength content))
                              (put-bytes content)))]
      (is (= {:op :message
              :transactional? true
              :lsn 55
              :prefix "myapp"
              :content "hi"}
             msg)))))

(deftest unsupported-and-truncated-test
  (testing "unknown type bytes are rejected with the byte in the error"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"Unsupported pgoutput"
         (pg/decode (buf 1 (put-byte \S)))))
    (is (= \S
           (-> (try (pg/decode (buf 1 (put-byte \S)))
                    (catch clojure.lang.ExceptionInfo e (ex-data e)))
               :type-byte))))
  (testing "runs off the end of the buffer"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"Truncated"
         (pg/decode (buf 9 (put-byte \B) (put-long 1))))))
  (testing "non-bytes payload"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"bytes or a ByteBuffer"
         (pg/decode :nope)))))
