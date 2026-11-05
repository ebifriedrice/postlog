(ns ebifriedrice.postlog.bytes
  "Byte fixtures for tests: build pgoutput messages from flat tagged
  segments without hand-sizing buffers."
  (:import (java.io ByteArrayOutputStream DataOutputStream)
           (java.nio.charset StandardCharsets)))

(defn utf8 [s] (.getBytes s StandardCharsets/UTF_8))

(defn- expand
  "Splice nested segment groups into a flat seq of [tag value] pairs."
  [segments]
  (mapcat #(if (vector? (second %)) % [%]) segments))

(defn msg
  "Encode segments to bytes: [:b byte] [:w int16] [:i int32] [:l int64]
  [:s cstring] [:raw byte-array].  A segment may itself be a vector of
  segments (one level of nesting), which keeps helpers like text-col
  readable at call sites."
  [& segments]
  (let [baos (ByteArrayOutputStream.)]
    (with-open [out (DataOutputStream. baos)]
      (doseq [[tag value] (expand segments)]
        (case tag
          :b (.writeByte out (long value))
          :w (.writeShort out (long value))
          :i (.writeInt out (unchecked-int value))
          :l (.writeLong out (long value))
          :s (do (.write out (.getBytes (str value) StandardCharsets/UTF_8))
                 (.write out 0))
          :raw (.write out ^bytes value))))
    (.toByteArray baos)))

(defn text-col
  "Segments encoding one TupleData text column."
  [s]
  [[:b \t] [:i (alength (utf8 s))] [:raw (utf8 s)]])
