(ns ebifriedrice.postlog.decode
  "Decode the text-format column values pgoutput sends into Clojure
  data, keyed by type OID.

  pgoutput renders every column as its text representation (``42``,
  ``t``, ``2500.00``) and Relation messages name each column's type
  only by OID, so decoding is an OID-keyed lookup.  Values of unknown
  or custom types keep their raw text, and non-text values (NULL,
  unchanged-TOAST markers) pass through untouched."
  (:import (java.math BigDecimal)))

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
   1700 parse-numeric})

(defn decode-value
  "Decode one text-format `value` using its column's `type-oid`.
  Non-string values pass through untouched; unknown OIDs keep the
  raw text."
  [type-oid value]
  (if (and (string? value) (contains? decoders type-oid))
    ((get decoders type-oid) value)
    value))
