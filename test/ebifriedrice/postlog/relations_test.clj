(ns ebifriedrice.postlog.relations-test
  (:require [clojure.test :refer [deftest is testing]]
            [ebifriedrice.postlog.bytes :refer [msg text-col]]
            [ebifriedrice.postlog.pgoutput :as pg]
            [ebifriedrice.postlog.relations :as relations]))

(def users-relation
  {:op :relation
   :relation-id 16385
   :schema "public"
   :table "users"
   :replica-identity :default
   :columns [{:name "id" :key? true :type-oid 23 :type-mod -1}
             {:name "email" :key? false :type-oid 25 :type-mod -1}
             {:name "plan" :key? false :type-oid 25 :type-mod -1}]})

(def cache {16385 users-relation})

(deftest apply-and-resolve-test
  (testing "relation messages upsert into the cache by oid"
    (is (= {16385 users-relation}
           (relations/apply-message {} users-relation)))
    (is (= {}
           (relations/apply-message {} {:op :insert :relation-id 16385})))
    (let [renamed (assoc users-relation :table "people")
          cache' (relations/apply-message cache renamed)]
      (is (= cache' (relations/apply-message cache' renamed))
          "idempotent")
      (is (= "people" (get-in cache' [16385 :table])))))
  (testing "resolving an unknown oid says so, with the known oids"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"never announced"
         (relations/resolve {} 999)))))

(deftest shape-insert-test
  (is (= {:op :insert :schema "public" :table "users"
          :after {"id" "1" "email" nil "plan" ::pg/unchanged-toast}}
         (relations/shape-change
          cache
          {:op :insert :relation-id 16385 :tuple ["1" nil ::pg/unchanged-toast]}))))

(deftest shape-update-test
  (testing "key-only before image zips against key columns only"
    (is (= {:op :update :schema "public" :table "users"
            :before {"id" "1"}
            :after {"id" "1" "email" "new@x.io" "plan" "pro"}}
           (relations/shape-change
            cache
            {:op :update :relation-id 16385
             :before-kind :key :before-tuple ["1"]
             :tuple ["1" "new@x.io" "pro"]}))))
  (testing "no before image when no key column changed"
    (is (nil? (:before
               (relations/shape-change
                cache
                {:op :update :relation-id 16385
                 :before-kind nil :before-tuple nil
                 :tuple ["1" "new@x.io" "pro"]})))))
  (testing "full before image under replica identity full"
    (is (= {"id" "1" "email" "old@x.io" "plan" "free"}
           (:before
            (relations/shape-change
             cache
             {:op :update :relation-id 16385
              :before-kind :old
              :before-tuple ["1" "old@x.io" "free"]
              :tuple ["1" "new@x.io" "pro"]}))))))

(deftest shape-delete-test
  (is (= {:op :delete :schema "public" :table "users"
          :before {"id" "1"}}
         (relations/shape-change
          cache
          {:op :delete :relation-id 16385
           :before-kind :key :before-tuple ["1"]}))))

(deftest shape-truncate-test
  (is (= {:op :truncate :tables ["public.users"] :cascade? false :restart-identity? false}
         (relations/shape-change
          cache
          {:op :truncate :relation-ids [16385] :cascade? false :restart-identity? false}))))

(deftest width-mismatch-test
  (testing "a tuple whose width disagrees with the relation fails loudly"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"width does not match"
         (relations/shape-change
          cache
          {:op :insert :relation-id 16385 :tuple ["1" "2"]})))))

(deftest value-decoder-test
  (testing "a value-decoder sees each column's type-oid and text value"
    (is (= {:op :insert :schema "public" :table "users"
            :after {"id" 7 "email" nil "plan" ::pg/unchanged-toast}}
           (relations/shape-change
            cache
            {:op :insert :relation-id 16385
             :tuple ["7" nil ::pg/unchanged-toast]}
            {:value-decoder (fn [type-oid value]
                              (if (and (= 23 type-oid) (string? value))
                                (Long/parseLong value)
                                value))}))))
  (testing "without a decoder, text values pass through"
    (is (= "7"
           (get-in (relations/shape-change
                    cache
                    {:op :insert :relation-id 16385 :tuple ["7" nil nil]})
                   [:after "id"])))))

(deftest roundtrip-through-bytes
  (testing "shaping a change decoded from real bytes end to end"
    (let [rel (pg/decode (msg [:b \R]
                              [:i 16385]
                              [:s "public"]
                              [:s "users"]
                              [:b \d]
                              [:w 2]
                              [:b 1] [:s "id"] [:i 23] [:i -1]
                              [:b 0] [:s "email"] [:i 25] [:i -1]))
          cache' (relations/apply-message {} rel)]
      (is (= {:op :insert :schema "public" :table "users"
              :after {"id" "7" "email" nil}}
             (relations/shape-change
              cache'
              (pg/decode (msg [:b \I]
                              [:i 16385]
                              [:b \N] [:w 2]
                              (text-col "7")
                              [:b \n]))))))))
