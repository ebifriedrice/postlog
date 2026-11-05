(ns ebifriedrice.postlog.pgoutput-test
  (:require [clojure.test :refer [deftest is testing]]
            [ebifriedrice.postlog.bytes :refer [msg text-col utf8]]
            [ebifriedrice.postlog.pgoutput :as pg])
  (:import (java.time Instant)))

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
                           [:i 2] [:raw (utf8 "hi")]))))))

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

(deftest insert-test
  (testing "Insert carries the new tuple: text, null, toast, binary values"
    (is (= {:op :insert
            :relation-id 16385
            :tuple ["42" nil ::pg/unchanged-toast]}
           (pg/decode (msg [:b \I]
                           [:i 16385]
                           [:b \N] [:w 3]
                           (text-col "42")
                           [:b \n]
                           [:b \u])))))
  (testing "binary tuple values come back as raw bytes"
    (let [tuple (:tuple (pg/decode (msg [:b \I]
                                           [:i 1]
                                           [:b \N] [:w 1]
                                           [:b \b] [:i 2] [:raw (utf8 "hi")])))]
      (is (= 1 (count tuple)))
      (is (java.util.Arrays/equals (utf8 "hi") (first tuple))))))

(deftest update-test
  (testing "no before tuple when no key column changed"
    (is (= {:op :update
            :relation-id 16385
            :before-kind nil
            :before-tuple nil
            :tuple ["42" "new@x.io"]}
           (pg/decode (msg [:b \U]
                           [:i 16385]
                           [:b \N] [:w 2]
                           (text-col "42")
                           (text-col "new@x.io"))))))
  (testing "key-only before tuple when a replica-identity column changed"
    (is (= {:op :update
            :relation-id 16385
            :before-kind :key
            :before-tuple ["42"]
            :tuple ["43" "new@x.io"]}
           (pg/decode (msg [:b \U]
                           [:i 16385]
                           [:b \K] [:w 1] (text-col "42")
                           [:b \N] [:w 2] (text-col "43") (text-col "new@x.io"))))))
  (testing "full before tuple under replica identity full"
    (is (= {:op :update
            :relation-id 16385
            :before-kind :old
            :before-tuple ["42" "old@x.io"]
            :tuple ["42" "new@x.io"]}
           (pg/decode (msg [:b \U]
                           [:i 16385]
                           [:b \O] [:w 2] (text-col "42") (text-col "old@x.io")
                           [:b \N] [:w 2] (text-col "42") (text-col "new@x.io")))))))

(deftest delete-test
  (testing "Delete carries a key-only or full before tuple, never a new one"
    (is (= {:op :delete
            :relation-id 16385
            :before-kind :key
            :before-tuple ["42"]}
           (pg/decode (msg [:b \D]
                           [:i 16385]
                           [:b \K] [:w 1] (text-col "42")))))
    (is (= {:op :delete
            :relation-id 16385
            :before-kind :old
            :before-tuple ["42" "old@x.io"]}
           (pg/decode (msg [:b \D]
                           [:i 16385]
                           [:b \O] [:w 2] (text-col "42") (text-col "old@x.io")))))))

(deftest bad-tuple-tag-test
  (testing "a new-tuple tag where a key is required fails loudly"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"Unexpected tuple tag"
         (pg/decode (msg [:b \D] [:i 1] [:b \N] [:w 0]))))))

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
