(ns ebifriedrice.postlog.consumer-test
  (:require [clojure.test :refer [deftest is testing]]
            [ebifriedrice.postlog.bytes :refer [msg text-col]]
            [ebifriedrice.postlog.consumer :as consumer]
            [ebifriedrice.postlog.replication :as replication])
  (:import (java.sql SQLException)))

(defn fake-stream
  "A LogicalStream over a fixed queue of byte payloads.  Returns
  [stream state-atom]; state carries :queue :acks :closed."
  [payloads]
  (let [state (atom {:queue (vec payloads) :acks [] :closed 0})]
    [(reify replication/LogicalStream
       (read-pending [_]
         (let [[h & t] (:queue @state)]
           (when h
             (swap! state assoc :queue (vec t))
             h)))
       (ack! [_ lsn] (swap! state update :acks conj lsn))
       (close! [_] (swap! state update :closed inc)))
     state]))

(defn wait-for
  "True once (pred) holds within timeout-ms, else nil."
  [pred timeout-ms]
  (let [deadline (+ (System/nanoTime) (* timeout-ms 1000000))]
    (loop []
      (or (pred)
          (when (< (System/nanoTime) deadline)
            (Thread/sleep 5)
            (recur))))))

(def users-relation
  (msg [:b \R] [:i 16385] [:s "public"] [:s "users"] [:b \d] [:w 2]
       [:b 1] [:s "id"] [:i 23] [:i -1]
       [:b 0] [:s "name"] [:i 25] [:i -1]))

(def begin-42 (msg [:b \B] [:l 1000] [:l 1500000] [:i 42]))
(def commit-1100 (msg [:b \C] [:b 0] [:l 1000] [:l 1100] [:l 1500000]))
(def insert-7 (msg [:b \I] [:i 16385] [:b \N] [:w 2] (text-col "7") (text-col "x")))
(def delete-7 (msg [:b \D] [:i 16385] [:b \K] [:w 1] (text-col "7")))

(defn start-consumer!
  "Consumer over a fresh fake stream; returns [consumer state-atom]."
  [payloads opts]
  (let [[stream state] (fake-stream payloads)]
    [(consumer/start! (merge {:stream-fn (constantly stream)
                              :poll-interval-ms 5}
                             opts))
     state]))

(deftest transaction-delivery-test
  (testing "a commit assembles and delivers the transaction, then acks its end lsn"
    (let [txs (atom [])
          [c state] (start-consumer!
                     [users-relation begin-42 insert-7 commit-1100]
                     {:on-transaction (fn [tx] (swap! txs conj tx))})]
      (is (wait-for #(= 1 (:txns ((:status c)))) 2000))
      (consumer/stop! c)
      (is (= :stopped (:state ((:await c) 2000))))
      (is (= [{:type :transaction
               :xid 42
               :commit-lsn "0/3E8"
               :end-lsn "0/44C"
               :commit-ts (java.time.Instant/parse "2000-01-01T00:00:01.500Z")
               :changes [{:op :insert
                          :schema "public"
                          :table "users"
                          :after {"id" "7" "name" "x"}}]}]
             @txs)
          "shape and envelope of the delivered transaction")
      (is (= [0x44C] (:acks @state)) "acks exactly the end lsn")
      (is (= 1 (:closed @state)) "stream closed on stop"))))

(deftest on-change-delivery-test
  (testing "on-change fires per change even without on-transaction; ack at commit"
    (let [changes (atom [])
          [c state] (start-consumer!
                     [users-relation begin-42 insert-7 delete-7 commit-1100]
                     {:on-change (fn [ch] (swap! changes conj ch))})]
      (is (wait-for #(= 1 (:txns ((:status c)))) 2000))
      (consumer/stop! c)
      (is (= :stopped (:state ((:await c) 2000))))
      (is (= [{:op :insert :schema "public" :table "users"
               :after {"id" "7" "name" "x"}}
              {:op :delete :schema "public" :table "users"
               :before {"id" "7"}}]
             @changes))
      (is (= [0x44C] (:acks @state))))))

(deftest empty-transaction-test
  (testing "transactions with no delivered changes ack silently"
    (let [txs (atom [])
          [c state] (start-consumer!
                     [begin-42 commit-1100]
                     {:on-transaction (fn [tx] (swap! txs conj tx))})]
      (is (wait-for #(= 1 (:txns ((:status c)))) 2000))
      (consumer/stop! c)
      (is (empty? @txs))
      (is (= [0x44C] (:acks @state))))))

(deftest handler-failure-test
  (testing "exhausted retries fail the consumer and ack nothing"
    (let [attempts (atom 0)
          [c state] (start-consumer!
                     [users-relation begin-42 insert-7 commit-1100]
                     {:on-change (fn [_] (swap! attempts inc)
                                   (throw (RuntimeException. "boom")))
                      :retry {:retries 1 :base-ms 1}})]
      (let [final ((:await c) 2000)]
        (is (= :failed (:state final)))
        (is (= "boom" (some-> final :error ex-message))))
      (is (= 2 @attempts) "one retry then exhausted")
      (is (empty? (:acks @state)) "no ack for undelivered work"))))

(deftest handler-retry-succeeds-test
  (testing "a transient handler error is retried, not delivered twice"
    (let [calls (atom [])
          [c state] (start-consumer!
                     [users-relation begin-42 insert-7 commit-1100]
                     {:on-change (fn [ch] (swap! calls conj ch)
                                   (when (= 1 (count @calls))
                                     (throw (RuntimeException. "transient"))))
                      :retry {:retries 2 :base-ms 1}})]
      (is (wait-for #(= 1 (:txns ((:status c)))) 2000))
      (consumer/stop! c)
      (is (= :stopped (:state ((:await c) 2000))))
      (is (= 2 (count @calls)))
      (is (= [0x44C] (:acks @state))))))

(deftest standalone-message-test
  (testing "non-transactional messages deliver immediately and ack their own lsn"
    (let [events (atom [])
          [c state] (start-consumer!
                     [(msg [:b \M] [:b 0] [:l 2000] [:s "app"] [:i 2] [:raw (.getBytes "hi" "UTF-8")])]
                     {:on-change (fn [ch] (swap! events conj ch))})]
      (is (wait-for #(= [2000] (:acks @state)) 2000))
      (consumer/stop! c)
      (is (= [{:op :message :transactional? false :lsn 2000
               :prefix "app" :content "hi"}]
             @events)))))

(deftest truncate-without-relation-test
  (testing "truncate of an unannounced table degrades to raw relation ids"
    (let [events (atom [])
          [c state] (start-consumer!
                     [users-relation begin-42
                      (msg [:b \T] [:i 1] [:b 0] [:i 16385])
                      (msg [:b \T] [:i 1] [:b 1] [:i 99999])
                      commit-1100]
                     {:on-change (fn [ch] (swap! events conj ch))})]
      (is (wait-for #(= 1 (:txns ((:status c)))) 2000))
      (consumer/stop! c)
      (is (= [{:op :truncate :tables ["public.users"] :cascade? false :restart-identity? false}
              {:op :truncate :relation-ids [99999] :cascade? true :restart-identity? false}]
             @events)))))

(deftest value-decoder-plumbing-test
  (testing "the value-decoder sees column type oids"
    (let [events (atom [])
          [c _] (start-consumer!
                 [users-relation begin-42 insert-7 commit-1100]
                 {:on-change (fn [ch] (swap! events conj ch))
                  :value-decoder (fn [type-oid v]
                                   (if (= 23 type-oid) (Long/parseLong v) v))})]
      (is (wait-for #(= 1 (:txns ((:status c)))) 2000))
      (consumer/stop! c)
      (is (= 7 (get-in @events [0 :after "id"]))))))

(deftest validation-test
  (testing "start! refuses missing handlers and connection sources"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #":on-change"
                          (consumer/start! {:stream-fn #(nth (fake-stream []) 0)})))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #":db, :conn-fn"
                          (consumer/start! {:on-change (fn [_])})))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #":slot-name"
                          (consumer/start! {:conn-fn (fn [] nil)
                                            :on-change (fn [_])})))))

(deftest reconnect-test
  (testing "a dead stream is replaced from the factory and consumption continues"
    (let [open-count (atom 0)
          broken (reify replication/LogicalStream
                   (read-pending [_] (throw (SQLException. "connection reset")))
                   (ack! [_ _])
                   (close! [_]))
          [good-stream good-state] (fake-stream [users-relation begin-42 insert-7 commit-1100])
          c (consumer/start! {:stream-fn (fn [] (swap! open-count inc)
                                           (if (= 1 @open-count) broken good-stream))
                              :poll-interval-ms 5
                              :retry {:base-ms 1}
                              :on-change (fn [_])})]
      (is (wait-for #(= 1 (:txns ((:status c)))) 5000) "recovers and consumes")
      (consumer/stop! c)
      (is (= :stopped (:state ((:await c) 2000))))
      (is (= 2 @open-count))
      (is (= [0x44C] (:acks @good-state)) "delivered from the replacement stream"))))
