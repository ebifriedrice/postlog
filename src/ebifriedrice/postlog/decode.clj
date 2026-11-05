(ns ebifriedrice.postlog.decode
  "Decode the text-format column values pgoutput sends into Clojure
  data, keyed by type OID.

  pgoutput renders every column as its text representation (``42``,
  ``t``, ``2500.00``) and Relation messages name each column's type
  only by OID, so decoding is an OID-keyed lookup.  Values of unknown
  or custom types keep their raw text, and non-text values (NULL,
  unchanged-TOAST markers) pass through untouched."
  (:require [clojure.string :as str]
            [clojure.walk :as walk])
  (:import (java.math BigDecimal)
           (java.time LocalDate LocalDateTime LocalTime OffsetDateTime
                      OffsetTime)
           (java.time.format DateTimeParseException)))

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
  "OID -> parser for built-in scalar types with an obvious Clojure
  equivalent."
  {16 parse-bool
   20 parse-long*     ; int8
   21 parse-long*     ; int2
   23 parse-long*     ; int4
   26 parse-long*     ; oid
   700 parse-double*  ; float4
   701 parse-double*  ; float8
   17 parse-bytea
   1082 (partial parse-temporal LocalDate/parse)  ; date
   1083 (partial parse-or-raw LocalTime/parse)    ; time
   1114 parse-timestamp                           ; timestamp
   1184 parse-timestamptz                         ; timestamptz
   1266 parse-timetz                              ; time with time zone
   1700 parse-numeric}
  ;; built-in arrays decode by parsing the literal then decoding
  ;; each element with its element type
  (into {} (map (fn [[array-oid element-oid]]
                  [array-oid (partial decode-array element-oid)]))
        array-element-oids))

(defn decode-value
  "Decode one text-format `value` using its column's `type-oid`.
  Non-string values pass through untouched; unknown OIDs keep the
  raw text."
  [type-oid value]
  (if (and (string? value) (contains? decoders type-oid))
    ((get decoders type-oid) value)
    value))
