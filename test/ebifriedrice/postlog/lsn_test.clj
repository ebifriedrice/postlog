(ns ebifriedrice.postlog.lsn-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [ebifriedrice.postlog.lsn :as lsn]))

(deftest parse-test
  (testing "known values"
    (is (= 0 (lsn/parse "0/0")))
    (is (= 0x16B374D (lsn/parse "0/16B374D")))
    (is (= 0x16B374D (lsn/parse "0/16b374d")))
    (is (= 0xAB00000001 (lsn/parse "AB/1")))
    (is (= 1 (lsn/parse " 0/1 "))))
  (testing "rejects junk"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Not a valid LSN"
                          (lsn/parse "0-1")))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Not a valid LSN"
                          (lsn/parse "16B374D")))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Not a valid LSN"
                          (lsn/parse "/1")))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Not a valid LSN"
                          (lsn/parse "0/G")))))

(deftest format-test
  (testing "known values"
    (is (= "0/0" (lsn/format 0)))
    (is (= "0/16B374D" (lsn/format 0x16B374D)))
    (is (= "AB/1" (lsn/format 0xAB00000001)))
    (testing "extreme halves round-trip even though hi >= 0x8 makes the long negative"
      (is (= "FFFFFFFF/FFFFFFFF" (lsn/format (lsn/parse "FFFFFFFF/FFFFFFFF"))))
      (is (= "80000000/1" (lsn/format (lsn/parse "80000000/1")))))))

(defspec parse-format-roundtrip 200
  (prop/for-all [lsn (gen/choose 0 Long/MAX_VALUE)]
    (= lsn (lsn/parse (lsn/format lsn)))))

(deftest compare-and-diff-test
  (testing "later LSNs are bigger longs"
    (is (< (lsn/parse "0/AAA") (lsn/parse "0/AAB")))
    (is (< (lsn/parse "0/FFFFFFFF") (lsn/parse "1/0")))
    (is (= (lsn/parse "0/5") (lsn/parse "0/5"))))
  (testing "diff counts WAL bytes between positions"
    (is (= 0x10 (lsn/diff (lsn/parse "0/1000") (lsn/parse "0/1010"))))
    (is (= -1 (lsn/diff (lsn/parse "0/2") (lsn/parse "0/1"))))
    (is (= 1 (lsn/diff (lsn/parse "0/FFFFFFFF") (lsn/parse "1/0"))))))
