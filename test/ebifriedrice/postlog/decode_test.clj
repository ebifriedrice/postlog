(ns ebifriedrice.postlog.decode-test
  (:require [clojure.test :refer [deftest is testing]]
            [ebifriedrice.postlog.decode :as decode]
            [ebifriedrice.postlog.pgoutput :as pg])
  (:import (java.time LocalDate LocalDateTime LocalTime OffsetDateTime
                      OffsetTime ZoneOffset)))

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

(deftest bytea-test
  (testing "hex-escaped bytea becomes a byte array"
    (is (java.util.Arrays/equals (byte-array [0x48 0x65 0x6c 0x6c 0x6f])
                                 (decode/decode-value 17 "\\x48656c6c6f")))
    (is (java.util.Arrays/equals (byte-array 0)
                                 (decode/decode-value 17 "\\x"))))
  (testing "the legacy escape format is rejected loudly"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"not hex-escaped"
         (decode/decode-value 17 "abc\\000def")))))

(deftest date-test
  (is (= (LocalDate/of 2026 11 5)
         (decode/decode-value 1082 "2026-11-05")))
  (is (= decode/infinity (decode/decode-value 1082 "infinity")))
  (is (= decode/neg-infinity (decode/decode-value 1082 "-infinity")))
  (testing "values java.time cannot represent keep their text"
    (is (= "0044-03-15 BC" (decode/decode-value 1082 "0044-03-15 BC")))))

(deftest time-test
  (is (= (LocalTime/of 12 34 56)
         (decode/decode-value 1083 "12:34:56")))
  (is (= (LocalTime/of 12 34 56 (* 789012 1000))
         (decode/decode-value 1083 "12:34:56.789012"))))

(deftest timetz-test
  (is (= (OffsetTime/of (LocalTime/of 12 34 56) (ZoneOffset/ofHoursMinutes 1 30))
         (decode/decode-value 1266 "12:34:56+01:30")))
  (testing "hour-only offsets gain minutes"
    (is (= (OffsetTime/of (LocalTime/of 12 34 56) (ZoneOffset/ofHours 1))
           (decode/decode-value 1266 "12:34:56+01")))))

(deftest timestamp-test
  (is (= (LocalDateTime/of 2026 11 5 12 34 56)
         (decode/decode-value 1114 "2026-11-05 12:34:56")))
  (is (= (LocalDateTime/of 2026 11 5 12 34 56 (* 789012 1000))
         (decode/decode-value 1114 "2026-11-05 12:34:56.789012")))
  (is (= decode/infinity (decode/decode-value 1114 "infinity"))))

(deftest timestamptz-test
  (is (= (OffsetDateTime/of 2026 11 5 12 34 56 0 (ZoneOffset/ofHoursMinutes 1 30))
         (decode/decode-value 1184 "2026-11-05 12:34:56+01:30")))
  (testing "hour-only offsets gain minutes"
    (is (= (OffsetDateTime/of 2026 11 5 12 34 56 0 ZoneOffset/UTC)
           (decode/decode-value 1184 "2026-11-05 12:34:56+00"))))
  (is (= decode/neg-infinity (decode/decode-value 1184 "-infinity"))))

(deftest array-literal-test
  (testing "flat arrays of bare tokens"
    (is (= ["1" "2" "3"] (decode/parse-array-literal "{1,2,3}")))
    (is (= [] (decode/parse-array-literal "{}"))))
  (testing "quoting, commas, escapes, and NULL"
    (is (= ["a b" "c,d"] (decode/parse-array-literal "{\"a b\",\"c,d\"}")))
    (is (= ["with \"quote\"" "back\\slash"]
           (decode/parse-array-literal "{\"with \\\"quote\\\"\",\"back\\\\slash\"}")))
    (is (= [nil "NULL" ""] (decode/parse-array-literal "{NULL,\"NULL\",\"\"}"))))
  (testing "nesting"
    (is (= [["1" "2"] ["3" "4"]]
           (decode/parse-array-literal "{{1,2},{3,4}}"))
        "two dimensions")
    (is (= [["a" nil] ["b" "c"]]
           (decode/parse-array-literal "{{a,NULL},{b,c}}"))))
  (testing "unquoted tokens trim whitespace"
    (is (= ["1" "2"] (decode/parse-array-literal "{ 1 , 2 }"))))
  (testing "malformed literals fail loudly"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Not an array"
                          (decode/parse-array-literal "1,2")))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Malformed"
                          (decode/parse-array-literal "{\"unterminated")))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Trailing"
                          (decode/parse-array-literal "{1,2}x")))))

(deftest typed-array-test
  (testing "built-in arrays decode element-wise"
    (is (= [1 2 3] (decode/decode-value 1007 "{1,2,3}")))
    (is (= [[1 2] [3 4]] (decode/decode-value 1007 "{{1,2},{3,4}}")))
    (is (= [nil 2] (decode/decode-value 1007 "{NULL,2}")))
    (is (= [true false] (decode/decode-value 1000 "{t,f}")))
    (is (= [2500.00M] (decode/decode-value 1231 "{2500.00}")))
    (is (= [(LocalDate/of 2026 11 5)] (decode/decode-value 1182 "{2026-11-05}")))
    (is (= [(OffsetDateTime/of 2026 11 5 12 34 56 0 ZoneOffset/UTC)]
           (decode/decode-value 1185 "{2026-11-05 12:34:56+00}"))))
  (testing "text-family arrays parse structure, elements stay text"
    (is (= ["a b" "c,d"] (decode/decode-value 1009 "{\"a b\",\"c,d\"}")))
    (is (= ["550e8400-e29b-41d4-a716-446655440000" nil]
           (decode/decode-value 2951 "{550e8400-e29b-41d4-a716-446655440000,NULL}"))))
  (testing "bytea arrays hold byte arrays"
    (let [arr (decode/decode-value 1001 "{\\x4869,\\x21}")]
      (is (= 2 (count arr)))
      (is (java.util.Arrays/equals (byte-array [0x48 0x69]) (first arr)))
      (is (java.util.Arrays/equals (byte-array [0x21]) (second arr)))))
  (testing "unknown array types keep the raw literal"
    (is (= "{x,y}" (decode/decode-value 999999 "{x,y}")))))

(deftest passthrough-test
  (testing "unknown OIDs keep their raw text"
    (is (= "hello" (decode/decode-value 999999 "hello"))))
  (testing "non-string values pass through untouched"
    (is (nil? (decode/decode-value 23 nil)))
    (is (= ::pg/unchanged-toast
           (decode/decode-value 23 ::pg/unchanged-toast)))))
