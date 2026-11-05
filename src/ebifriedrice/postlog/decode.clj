(ns ebifriedrice.postlog.decode
  "Decode the column values pgoutput sends into Clojure data, keyed by
  type OID.

  pgoutput renders columns in one of two formats per stream: every
  column as its text representation (``42``, ``t``, ``2500.00``), or
  with the protocol-v2 ``binary`` option, each type's binary wire form
  (int4 as four big-endian bytes, timestamps as microseconds since
  2000-01-01, and so on — the layouts Postgres' typsend functions
  emit, verified identical on servers 14-17).  Relation messages name
  each column's type only by OID, so both formats decode through an
  OID-keyed lookup: [[decode-value]] dispatches on the value being a
  string (text) or bytes (binary).

  Values of unknown or custom types keep their raw text or bytes, and
  other values (NULL, unchanged-TOAST markers) pass through
  untouched."
  (:require [clojure.string :as str]
            [clojure.walk :as walk])
  (:import (java.math BigDecimal BigInteger)
           (java.nio ByteBuffer)
           (java.nio.charset StandardCharsets)
           (java.time Instant LocalDate LocalDateTime LocalTime
                      OffsetDateTime OffsetTime ZoneOffset)
           (java.time.format DateTimeParseException)
           (java.util UUID)))

(def infinity
  "Marker for date and timestamp values Postgres renders as
  ``infinity``."
  ::infinity)

(def neg-infinity
  "Marker for ``-infinity``."
  ::neg-infinity)

(defn- parse-or-raw
  "Parse with `parse`, falling back to the raw text for values
  java.time cannot represent (e.g. BC years)."
  [parse s]
  (try
    (parse s)
    (catch DateTimeParseException _ s)))

(defn- parse-temporal
  "Like parse-or-raw, but mapping Postgres' infinity spellings to
  [[infinity]]/[[neg-infinity]]."
  [parse s]
  (case s
    "infinity" infinity
    "-infinity" neg-infinity
    (parse-or-raw parse s)))

(defn- pad-offset
  "java.time rejects Postgres' hour-only offsets (``+00``); add :00."
  [s]
  (str/replace s #"[+-]\d{2}$" "$0:00"))

(defn- char-at
  "Char at `i`, or nil past the end."
  [^String s i]
  (when (< i (.length s))
    (.charAt s i)))

(defn- malformed [^String s]
  (throw (ex-info "Malformed array literal" {:text s})))

(defn- read-quoted
  "Read a quoted element body starting after the opening quote.
  Backslash escapes and doubled quotes become literal characters.
  Returns [text index-after-closing-quote]."
  [^String s start]
  (loop [sb (StringBuilder.), i start]
    (if-let [c (char-at s i)]
      (cond
        (= c \\) (if-let [esc (char-at s (inc i))]
                   (recur (doto sb (.append esc)) (+ i 2))
                   (malformed s))
        (= c \") (if (= (char-at s (inc i)) \")
                  (recur (doto sb (.append \")) (+ i 2))
                  [(str sb) (inc i)])
        :else (recur (doto sb (.append c)) (inc i)))
      (malformed s))))

(defn- read-bare
  "Read an unquoted element: up to the delimiter, trimmed, with an
  unquoted NULL read as nil.  Returns [text next-index]."
  [^String s start]
  (let [end (some (fn [i] (when (contains? #{\, \}} (char-at s i)) i))
                  (range start (inc (.length s))))]
    (if end
      (let [token (str/trim (subs s start end))]
        [(if (= "NULL" token) nil token) end])
      (malformed s))))

(defn- array-element
  [^String s i]
  (if (= (char-at s i) \")
    (read-quoted s (inc i))
    (read-bare s i)))

(defn- read-array-body
  "Read elements after an opening brace.  Returns [elements
  index-after-closing-brace]."
  [^String s start]
  (loop [elems [], i start]
    (if-let [c (char-at s i)]
      (condp = c
        \} [elems (inc i)]
        \, (recur elems (inc i))
        \{ (let [[nested i'] (read-array-body s (inc i))]
             (recur (conj elems nested) i'))
        (let [[element i'] (array-element s i)]
          (recur (conj elems element) i')))
      (malformed s))))

(defn parse-array-literal
  "Parse an array's text form into nested vectors of strings and
  nils (unquoted NULL); typed decoding maps over the result."
  [^String s]
  (when-not (and (pos? (.length s)) (= \{ (.charAt s 0)))
    (throw (ex-info "Not an array literal" {:text s})))
  (try
    (let [[elements end] (read-array-body s 1)]
      (if (= end (.length s))
        elements
        (throw (ex-info "Trailing characters after array" {:text s}))))
    (catch IndexOutOfBoundsException _
      (throw (ex-info "Malformed array literal" {:text s})))))

(def ^:private array-element-oids
  "Array type OID -> element type OID, for the built-in arrays.
  Arrays of the text-family types are here too: their elements keep
  their text, but the literal's quoting and NULLs still get parsed."
  {1000 16     ; _bool
   1001 17     ; _bytea
   1005 21     ; _int2
   1007 23     ; _int4
   1009 25     ; _text
   1014 1042   ; _bpchar
   1015 1043   ; _varchar
   1016 20     ; _int8
   1021 700    ; _float4
   1022 701    ; _float8
   1028 26     ; _oid
   1115 1114   ; _timestamp
   1182 1082   ; _date
   1183 1083   ; _time
   1185 1184   ; _timestamptz
   1231 1700   ; _numeric
   1270 1266   ; _timetz
   199 114     ; _json
   2951 2950   ; _uuid
   3807 3802   ; _jsonb
   })

(declare decode-value)

(defn- decode-array
  "Decode each element of a parsed array literal with its element
  type's decoder."
  [element-oid s]
  (let [element #(if (string? %) (decode-value element-oid %) %)]
    (walk/postwalk element (parse-array-literal s))))

(defn- parse-timetz [s]
  (parse-or-raw OffsetTime/parse (pad-offset s)))

(defn- iso-space->t
  "java.time wants a T where Postgres prints a space."
  [s]
  (str/replace-first s " " "T"))

(defn parse-timestamp
  "Timestamp without time zone -> LocalDateTime."
  [s]
  (parse-temporal (comp LocalDateTime/parse iso-space->t) s))

(defn parse-timestamptz
  "Timestamp with time zone -> OffsetDateTime."
  [s]
  (parse-temporal (comp OffsetDateTime/parse pad-offset iso-space->t) s))

(defn- parse-long* [^String s]
  (Long/parseLong s))

(defn- parse-double* [^String s]
  (Double/parseDouble s))

(defn- parse-numeric [^String s]
  (if (= "NaN" s)
    ##NaN
    (BigDecimal. s)))

(defn- parse-bool [^String s]
  (case s
    "t" true
    "f" false
    (throw (ex-info "Unknown boolean text" {:text s}))))

(defn- hex->bytes
  "Decode two hex digits per byte, as in bytea's ``\\x`` text format."
  [^String hex]
  (let [n (quot (count hex) 2)
        out (byte-array n)]
    (dotimes [i n]
      (aset-byte out i
                 (unchecked-byte
                  (Integer/parseInt (.substring hex (* 2 i) (+ 2 (* 2 i))) 16))))
    out))

(defn- parse-bytea [^String s]
  (if (.startsWith s "\\x")
    (hex->bytes (.substring s 2))
    (throw (ex-info "bytea text is not hex-escaped" {:text s}))))

(def ^:private decoders
  "OID -> parser for built-in types with an obvious Clojure
  equivalent; array types parse their literal, then decode each
  element with the element type's parser."
  (merge {16 parse-bool
          17 parse-bytea
          20 parse-long*                              ; int8
          21 parse-long*                              ; int2
          23 parse-long*                              ; int4
          26 parse-long*                              ; oid
          700 parse-double*                           ; float4
          701 parse-double*                           ; float8
          1082 (partial parse-temporal LocalDate/parse)  ; date
          1083 (partial parse-or-raw LocalTime/parse)    ; time
          1114 parse-timestamp                           ; timestamp
          1184 parse-timestamptz                         ; timestamptz
          1266 parse-timetz                              ; time with time zone
          1700 parse-numeric}
         (into {} (map (fn [[array-oid element-oid]]
                         [array-oid (partial decode-array element-oid)]))
               array-element-oids)))

;; ---------------------------------------------------------------------------
;; binary-format values (protocol v2's `binary` option)

(def ^:private pg-epoch-day
  "Days from 1970-01-01 to Postgres' 2000-01-01 epoch."
  10957)

(def ^:private pg-epoch-micros
  "Microseconds from the Unix epoch to Postgres' 2000-01-01 epoch."
  946684800000000)

(def ^:private int32-max 0x7FFFFFFF)
(def ^:private int32-min -0x80000000)
(def ^:private int64-max Long/MAX_VALUE)
(def ^:private int64-min Long/MIN_VALUE)

(defn- wrap [^bytes bs]
  (ByteBuffer/wrap bs))

(defn- micros->instant [^long us]
  (Instant/ofEpochSecond (quot us 1000000) (* (rem us 1000000) 1000)))

(defn- parse-binary-int2 [^bytes bs] (.getShort (wrap bs)))
(defn- parse-binary-int4 [^bytes bs] (.getInt (wrap bs)))
(defn- parse-binary-int8 [^bytes bs] (.getLong (wrap bs)))
(defn- parse-binary-oid  [^bytes bs] (bit-and (.getInt (wrap bs)) 0xFFFFFFFF))
(defn- parse-binary-bool [^bytes bs] (= (aget bs 0) 1))
(defn- parse-binary-float4 [^bytes bs] (Float/intBitsToFloat (.getInt (wrap bs))))
(defn- parse-binary-float8 [^bytes bs] (Double/longBitsToDouble (.getLong (wrap bs))))
(defn- parse-binary-string [^bytes bs] (String. bs StandardCharsets/UTF_8))

(defn- parse-binary-uuid [^bytes bs]
  (let [b (wrap bs)]
    (str (UUID. (.getLong b) (.getLong b)))))

(defn- parse-binary-jsonb [^bytes bs]
  (if (and (pos? (alength bs)) (= (aget bs 0) 1))
    (String. bs 1 (dec (alength bs)) StandardCharsets/UTF_8)
    bs))

(defn- parse-binary-date [^bytes bs]
  (let [days (.getInt (wrap bs))]
    (cond
      (= days int32-max) infinity
      (= days int32-min) neg-infinity
      :else (LocalDate/ofEpochDay (+ pg-epoch-day days)))))

(defn- parse-binary-time [^bytes bs]
  (LocalTime/ofNanoOfDay (* (.getLong (wrap bs)) 1000)))

(defn- parse-binary-timetz [^bytes bs]
  (let [b (wrap bs)
        micros (.getLong b)
        ;; the zone is seconds *west* of UTC: +01:30 arrives as -5400
        west-seconds (.getInt b)]
    (OffsetTime/of (LocalTime/ofNanoOfDay (* micros 1000))
                   (ZoneOffset/ofTotalSeconds (- west-seconds)))))

(defn- parse-binary-timestamp [^bytes bs]
  (let [us (.getLong (wrap bs))]
    (cond
      (= us int64-max) infinity
      (= us int64-min) neg-infinity
      :else (LocalDateTime/ofInstant (micros->instant (+ pg-epoch-micros us))
                                     ZoneOffset/UTC))))

(defn- parse-binary-timestamptz [^bytes bs]
  (let [us (.getLong (wrap bs))]
    (cond
      (= us int64-max) infinity
      (= us int64-min) neg-infinity
      :else (OffsetDateTime/ofInstant (micros->instant (+ pg-epoch-micros us))
                                      ZoneOffset/UTC))))

;; numeric sign field: 0x0000 positive, 0x4000 negative, 0xC000 NaN,
;; 0xD000/0xF000 the infinities
(defn- parse-binary-numeric [^bytes bs]
  (let [b (wrap bs)
        ndigits (bit-and (.getShort b) 0xFFFF)
        weight (.getShort b)
        sign (bit-and (.getShort b) 0xFFFF)
        dscale (bit-and (.getShort b) 0xFFFF)
        digits (repeatedly ndigits #(bit-and (.getShort b) 0xFFFF))
        ;; all digits as one base-10000 integer, then rescale:
        ;; value = N * 10000 ^ (weight - ndigits + 1)
        unscaled (reduce (fn [^BigInteger n d]
                           (.add (.multiply n (BigInteger/valueOf 10000))
                                 (BigInteger/valueOf (long d))))
                         BigInteger/ZERO
                         digits)]
    (case sign
      0xC000 ##NaN
      0xD000 ##Inf
      0xF000 ##-Inf
      (cond-> (-> (BigDecimal. unscaled)
                  (.scaleByPowerOfTen (* 4 (inc (- weight ndigits))))
                  (.setScale dscale))
        (= sign 0x4000) .negate))))

(def ^:private binary-decoders
  "OID -> parser for the binary wire layouts postlog understands.
  These are the typsend layouts, byte-verified identical on Postgres
  14 through 17.  timestamptz decodes to UTC: binary carries no zone
  offset, unlike text output which renders in the server's."
  {16 parse-binary-bool
   17 identity                                 ; bytea: binary is the bytes
   18 parse-binary-string                      ; "char"
   19 parse-binary-string                      ; name
   20 parse-binary-int8
   21 parse-binary-int2
   23 parse-binary-int4
   25 parse-binary-string
   26 parse-binary-oid
   114 parse-binary-string                     ; json's binary is raw text
   142 parse-binary-string                     ; xml
   700 parse-binary-float4
   701 parse-binary-float8
   1042 parse-binary-string                    ; bpchar
   1043 parse-binary-string                    ; varchar
   1082 parse-binary-date
   1083 parse-binary-time
   1114 parse-binary-timestamp
   1184 parse-binary-timestamptz
   1266 parse-binary-timetz
   2950 parse-binary-uuid
   1700 parse-binary-numeric
   3802 parse-binary-jsonb})

(defn decode-value
  "Decode one column `value` — text (string) or binary (bytes) — using
  its column's `type-oid`.  Non-string/non-bytes values pass through
  untouched; unknown OIDs keep the raw text or bytes.  This is the
  function to hand a consumer as ``:value-decoder``."
  [type-oid value]
  (cond
    (string? value) (if-let [decode-fn (get decoders type-oid)]
                      (decode-fn value)
                      value)
    (bytes? value) (if-let [decode-fn (get binary-decoders type-oid)]
                     (decode-fn value)
                     value)
    :else value))
