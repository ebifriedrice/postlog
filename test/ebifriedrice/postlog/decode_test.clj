(ns ebifriedrice.postlog.decode-test
  (:require [clojure.test :refer [deftest is testing]]
            [ebifriedrice.postlog.decode :as decode]
            [ebifriedrice.postlog.pgoutput :as pg]))

(deftest numbers-test
  (testing "integers and oid become Longs, floats Doubles"
    (is (= 42 (decode/decode-value 23 "42")))
    (is (= -7 (decode/decode-value 21 "-7")))
    (is (= 9007199254740993 (decode/decode-value 20 "9007199254740993")))
    (is (= 12.5 (decode/decode-value 700 "12.5")))
    (is (= ##Inf (decode/decode-value 701 "Infinity")))
    (is (= ##-Inf (decode/decode-value 701 "-Infinity")))
    (is (Double/isNaN (decode/decode-value 701 "NaN"))))
  (testing "numeric becomes BigDecimal, NaN excepted"
    (is (= 2500.00M (decode/decode-value 1700 "2500.00")))
    (is (Double/isNaN (decode/decode-value 1700 "NaN")))))

(deftest bool-test
  (is (true? (decode/decode-value 16 "t")))
  (is (false? (decode/decode-value 16 "f")))
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo #"Unknown boolean"
       (decode/decode-value 16 "x"))))

(deftest passthrough-test
  (testing "unknown OIDs keep their raw text"
    (is (= "hello" (decode/decode-value 999999 "hello"))))
  (testing "non-string values pass through untouched"
    (is (nil? (decode/decode-value 23 nil)))
    (is (= ::pg/unchanged-toast
           (decode/decode-value 23 ::pg/unchanged-toast)))))
