(ns ebifriedrice.postlog.admin-test
  (:require [clojure.test :refer [deftest is testing]]
            [ebifriedrice.postlog.admin :as admin]))

(deftest jdbc-url-test
  (testing "maps become URLs, defaults fill host and port"
    (is (= "jdbc:postgresql://localhost:5432/app"
           (admin/jdbc-url {:database "app"})))
    (is (= "jdbc:postgresql://db.example:5433/app?socketTimeout=10&replication=database"
           (admin/replication-jdbc-url
            {:host "db.example" :port 5433 :database "app" :socketTimeout 10}))))
  (testing "strings pass through; replication url appends without clobbering"
    (is (= "jdbc:postgresql://h/db" (admin/jdbc-url "jdbc:postgresql://h/db")))
    (is (= "jdbc:postgresql://h/db?replication=database"
           (admin/replication-jdbc-url "jdbc:postgresql://h/db")))
    (is (= "jdbc:postgresql://h/db?x=1&replication=database"
           (admin/replication-jdbc-url "jdbc:postgresql://h/db?x=1")))
    (is (= "jdbc:postgresql://h/db?replication=database"
           (admin/replication-jdbc-url "jdbc:postgresql://h/db?replication=database")))))

(deftest publication-ddl-test
  (testing "table list, default ops"
    (is (= "CREATE PUBLICATION \"my_pub\" FOR TABLE \"public\".\"users\", \"public\".\"orders\" WITH (publish = 'INSERT, UPDATE, DELETE, TRUNCATE')"
           (admin/publication-ddl "my_pub" {:tables ["public.users" "public.orders"]}))))
  (testing "for all tables"
    (is (= "CREATE PUBLICATION \"my_pub\" FOR ALL TABLES WITH (publish = 'INSERT, DELETE')"
           (admin/publication-ddl "my_pub" {:all-tables? true :publish [:insert :delete]}))))
  (testing "partition root option rides in WITH"
    (is (= "CREATE PUBLICATION \"p\" FOR ALL TABLES WITH (publish = 'INSERT, UPDATE, DELETE, TRUNCATE', publish_via_partition_root = true)"
           (admin/publication-ddl "p" {:all-tables? true :via-partition-root? true}))))
  (testing "identifier quoting neutralizes embedded quotes"
    (is (= "CREATE PUBLICATION \"x\"\"y\" FOR ALL TABLES WITH (publish = 'INSERT, UPDATE, DELETE, TRUNCATE')"
           (admin/publication-ddl "x\"y" {:all-tables? true}))))
  (testing "exactly one of tables / all-tables?"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"exactly one"
         (admin/publication-ddl "p" {:tables ["a"] :all-tables? true})))
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"exactly one"
         (admin/publication-ddl "p" {}))))
  (testing "blank identifiers rejected"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"Blank SQL identifier"
         (admin/publication-ddl "" {:all-tables? true})))))
