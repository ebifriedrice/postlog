(ns ebifriedrice.postlog.lsn
  "Postgres log sequence numbers.

  An LSN identifies a byte offset into the write-ahead log.  Postgres
  prints it as two hex halves joined by a slash, e.g. ``0/16B374D``;
  the first half is the high 32 bits, the second the low 32.

  This library represents an LSN as a plain 64-bit long, so ordinary
  comparisons and arithmetic work on it: a later LSN is a bigger
  number, and subtracting two LSNs yields the bytes of WAL between
  them.  Real-world LSNs stay far below ``Long/MAX_VALUE``, so the
  unsigned interpretation Postgres uses never matters in practice."
  (:refer-clojure :exclude [format]))

(def invalid
  "The LSN Postgres reports when no position exists (``0/0``)."
  0)

(defn parse
  "Parse the textual LSN form ``HI/LO`` (hex, case-insensitive) into a
  long.  Throws ex-info when the string is not an LSN."
  [s]
  (if-let [[_ hi lo] (re-matches #"\s*([0-9A-Fa-f]{1,8})/([0-9A-Fa-f]{1,8})\s*" (str s))]
    (bit-or (bit-shift-left (Long/parseLong hi 16) 32)
            (Long/parseLong lo 16))
    (throw (ex-info "Not a valid LSN" {:lsn (str s)}))))

(defn format
  "Render a long LSN in the canonical uppercase ``HI/LO`` text form."
  [lsn]
  (let [lsn (long lsn)
        ;; Unsigned split: hi halves >= 0x8 set the sign bit, so the
        ;; plain shift would smear ones into the high half.
        hi (unsigned-bit-shift-right lsn 32)
        ;; 0xFFFFFFFF reads as the long 4294967295 (it exceeds int), so the
        ;; mask keeps only the low 32 bits as a non-negative long.
        lo (bit-and lsn 0xFFFFFFFF)]
    (clojure.core/format "%X/%X" hi lo)))

(defn diff
  "Bytes of WAL between two LSNs: how far `lsn2` is ahead of `lsn1`.
  Negative when `lsn2` is behind.  Backs replication-lag reporting."
  [lsn1 lsn2]
  (- (long lsn2) (long lsn1)))
