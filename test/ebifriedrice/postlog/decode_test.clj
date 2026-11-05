(ns ebifriedrice.postlog.decode-test
  (:require [clojure.test :refer [deftest is testing]]
            [ebifriedrice.postlog.decode :as decode]
            [ebifriedrice.postlog.pgoutput :as pg])
  (:import (java.nio ByteBuffer)
           (java.time LocalDate LocalDateTime LocalTime OffsetDateTime
                      OffsetTime ZoneOffset)))

(defn- ba
  "Byte array from possibly out-of-range byte literals."
  [& xs]
  (byte-array (map unchecked-byte xs)))

(defn- i16 [n] (let [b (ByteBuffer/allocate 2)] (.putShort b (unchecked-short n)) (.array b)))
(defn- i32 [n] (let [b (ByteBuffer/allocate 4)] (.putInt b (unchecked-int n)) (.array b)))
(defn- i64 [n] (let [b (ByteBuffer/allocate 8)] (.putLong b (long n)) (.array b)))

(deftest binary-scalars-test
  (testing "numbers decode from big-endian wire layouts"
    (is (= 42 (decode/decode-value 23 (i32 42))))
    (is (= -7 (decode/decode-value 21 (i16 -7))))
    (is (= 9007199254740993 (decode/decode-value 20 (i64 9007199254740993))))
    (is (= 12345 (decode/decode-value 26 (i32 12345))))
    (is (= 1.5 (decode/decode-value 700 (i32 (Float/floatToIntBits 1.5)))))
    (is (= 2.25 (decode/decode-value 701 (i64 (Double/doubleToLongBits 2.25)))))
    (is (Double/isNaN (decode/decode-value 701 (i64 0x7FF8000000000000))))
    (is (= ##Inf (decode/decode-value 701 (i64 0x7FF0000000000000)))))
  (testing "bool, strings, json, bytea, uuid"
    (is (true? (decode/decode-value 16 (ba 1))))
    (is (false? (decode/decode-value 16 (ba 0))))
    (is (= "hi" (decode/decode-value 25 (.getBytes "hi"))))
    (is (= "{\"a\": 1}" (decode/decode-value 114 (.getBytes "{\"a\": 1}"))))
    (is (= "{\"a\": 1}" (decode/decode-value 3802 (ba 0x01 0x7b 0x22 0x61 0x22
                                                            0x3a 0x20 0x31 0x7d))))
    (is (java.util.Arrays/equals (ba 0x00 0xff)
                                 (decode/decode-value 17 (ba 0x00 0xff))))
    (is (= "550e8400-e29b-41d4-a716-446655440000"
           (decode/decode-value 2950 (ba 0x55 0x0e 0x84 0x00 0xe2 0x9b 0x41
                                         0xd4 0xa7 0x16 0x44 0x66 0x55 0x44
                                         0x00 0x00)))))
  (testing "dates and times from epoch offsets"
    (is (= (LocalDate/of 2026 11 5) (decode/decode-value 1082 (i32 9805))))
    (is (= decode/infinity (decode/decode-value 1082 (i32 0x7FFFFFFF))))
    (is (= decode/neg-infinity (decode/decode-value 1082 (i32 -0x80000000))))
    (is (= (LocalTime/of 12 34 56 500000000)
           (decode/decode-value 1083 (i64 45296500000))))
    (is (= (OffsetTime/of (LocalTime/of 12 34 56) (ZoneOffset/ofHoursMinutes 1 30))
           ;; zone is west-positive: +01:30 arrives as -5400
           (decode/decode-value
            1266 (byte-array (concat (i64 45296000000) (i32 -5400)))))))
  (testing "timestamps decode to UTC from micros since 2000-01-01"
    (is (= (LocalDateTime/of 2026 11 5 12 34 56)
           (decode/decode-value 1114 (i64 0x0302857DE1FC00))))
    (is (= (OffsetDateTime/of 2026 11 5 12 34 56 0 ZoneOffset/UTC)
           (decode/decode-value 1184 (i64 0x0302857DE1FC00))))
    (is (= decode/infinity (decode/decode-value 1184 (i64 Long/MAX_VALUE))))
    (is (= decode/neg-infinity (decode/decode-value 1114 (i64 Long/MIN_VALUE)))))
  (testing "numeric from ndigits/weight/sign/dscale and base-10000 digits"
    (is (= 3.14M (decode/decode-value 1700 (ba 0x00 0x02 0x00 0x00 0x00 0x00
                                                   0x00 0x02 0x00 0x03 0x05 0x78))))
    (is (= -12.50M (decode/decode-value 1700 (ba 0x00 0x02 0x00 0x00 0x40 0x00
                                                    0x00 0x02 0x00 0x0c 0x13 0x88))))
    (is (Double/isNaN (decode/decode-value
                       1700 (byte-array (concat (i16 0) (i16 0)
                                                (i16 0xC000) (i16 0))))))
    (is (= ##Inf (decode/decode-value
                  1700 (byte-array (concat (i16 0) (i16 0)
                                           (i16 0xD000) (i16 0))))))
    (is (= ##-Inf (decode/decode-value
                   1700 (byte-array (concat (i16 0) (i16 0)
                                            (i16 0xF000) (i16 0)))))))
  (testing "unknown binary types keep their bytes"
    (let [raw (ba 0x01 0x02)]
      (is (java.util.Arrays/equals raw (decode/decode-value 999999 raw))))))

(defn- bin-arr
  "Binary array wire bytes for `elem-oid`, dims, and raw elements
  (nil = NULL).  Mirrors array_send: dims + lbound, then each element
  as its length and payload interleaved, no null bitmap."
  [elem-oid dims & elements]
  (let [out (java.io.ByteArrayOutputStream.)]
    (doseq [n (concat [(count dims)
                       (if (some nil? elements) 1 0)
                       elem-oid]
                      (mapcat (fn [d] [d 1]) dims))]
      (.write out ^bytes (i32 n)))
    (doseq [e elements]
      (if e
        (do (.write out ^bytes (i32 (alength ^bytes e)))
            (.write out ^bytes e))
        (.write out ^bytes (i32 -1))))
    (.toByteArray out)))

(deftest binary-arrays-test
  (testing "flat arrays with NULL holes decode element-wise"
    (is (= [1 nil 3] (decode/decode-value 1007 (bin-arr 23 [3] (i32 1) nil (i32 3))))))
  (testing "nesting follows the dimension lengths"
    (is (= [[1 2] [3 4]]
           (decode/decode-value 1007 (bin-arr 23 [2 2]
                                      (i32 1) (i32 2) (i32 3) (i32 4))))))
  (testing "the header's element oid drives decoding"
    (is (= ["a" "b"] (decode/decode-value 1009
                                          (bin-arr 25 [2]
                                                   (.getBytes "a") (.getBytes "b")))))
    (is (= [true false]
           (decode/decode-value 1000 (bin-arr 16 [2] (ba 1) (ba 0))))))
  (testing "unknown array types keep their bytes"
    (let [raw (bin-arr 999999 [1] (ba 1 2))]
      (is (java.util.Arrays/equals raw (decode/decode-value 999998 raw))))))

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
