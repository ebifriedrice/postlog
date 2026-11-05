# postlog

A Clojure library for consuming PostgreSQL logical replication: stream
commits from the write-ahead log — inserts, updates, deletes, truncates —
into your handlers, in order, with at-least-once delivery. The
replication core of tools like [Sequin](https://github.com/sequinstream/sequin)
or Debezium, sized as a library.

## Requirements

- PostgreSQL 14 or newer, running `wal_level = logical` (a start-level
  setting — a server restart is needed to change it).
- Clojure 1.11+ (developed on 1.12); any JVM pgjdbc supports.

## Quickstart

```clojure
;; deps.edn
net.clojars.ebifriedrice/postlog {:mvn/version "0.1.0-SNAPSHOT"}
```

```clojure
(require '[ebifriedrice.postlog.consumer :as consumer]
         '[ebifriedrice.postlog.decode :as decode])

(def c (consumer/start!
        {:db               {:host "localhost" :database "app"
                            :user "repl" :password "..."}
         :slot-name        "my_slot"
         :publication-name "my_pub"
         :tables           ["public.users"]        ; or omit for all tables
         :value-decoder    decode/decode-value     ; typed values
         :on-change        (fn [change] (prn change))}))

(consumer/stop! c)
```

The slot and publication are created if missing (and the tables added to
the publication). Changes arrive as maps:

```clojure
{:op :insert                       ; :insert :update :delete :truncate
 :schema "public" :table "users"
 :after  {"id" 7 "email" "x@y.io" "created_at" <OffsetDateTime>}}
{:op :update :schema "public" :table "users"
 :before {"id" 7}                  ; key-only under default replica identity
 :after  {"id" 7 "email" "z@y.io"}}
{:op :truncate :tables ["public.users"] :cascade? false :restart-identity? false}
```

Without `:value-decoder`, row values stay in Postgres' text format;
`decode/decode-value` decodes built-in types (numbers, booleans,
BigDecimal numerics, java.time temporals, bytea, arrays — unknown types
keep their text). Set `:on-transaction` instead of (or alongside)
`:on-change` to receive each commit as
`{:xid .. :commit-lsn .. :commit-ts .. :changes [..]}`.

## Delivery semantics

Delivery is **ordered** and **at-least-once**, on a single thread per
consumer — synchronous handlers are what provide backpressure. The
slot's confirmed position advances only at transaction boundaries after
your handler returns, so a handler failure, consumer crash, or
connection loss redelivers from the last confirmed transaction: make
handlers idempotent or dedupe (every change carries its `:xid`/LSNs via
`:on-transaction`).

- Handler exceptions retry with exponential backoff
  (`:retry {:retries 8 :base-ms 100}`); exhausted retries **fail** the
  consumer — nothing is skipped, order is never broken.
- Connection loss reconnects with the same backoff and resumes from the
  slot's confirmed position.

## Connections

Exactly one connection source is required:

- `:db` — a spec map; postlog opens and owns the replication and admin
  connections and closes them on stop.
- `:conn-fn` — `(fn [] Connection)` called for **every (re)connect**; it
  must return a fresh, dedicated, un-pooled replication connection
  (`replication=database`). postlog never closes it. A
  `DataSource`-backed factory is one line:
  `:conn-fn #(.getConnection ds)` — postlog deliberately does not accept
  a `DataSource` directly, for the same reason it does not accept a
  bare connection:

A bare connection instance is **not accepted, on purpose**: a
replication connection that dies mid-stream (network drop, server
restart, `pg_terminate_backend`) cannot be revived, and resuming the
slot means opening a *fresh* connection — which only a factory can
produce. Accepting an instance would make auto-reconnect silently
impossible.

`ensure-slot?` / `ensure-publication?` bootstrap needs a normal (admin)
connection: either `:db` or an explicit `:admin-conn` (never closed by
postlog). With only `:conn-fn` and neither, the slot and publication
must already exist.

## Lower-level namespaces

- `ebifriedrice.postlog.pgoutput` — decode pgoutput binary messages.
- `ebifriedrice.postlog.relations` — relation cache; shape frames into
  change maps (`:value-decoder` hook).
- `ebifriedrice.postlog.decode` — text-format value decoding.
- `ebifriedrice.postlog.replication` — open a `LogicalStream`
  (pgjdbc-backed; the consumer only needs the protocol).
- `ebifriedrice.postlog.admin` — slots, publications, LSN introspection.
- `ebifriedrice.postlog.lsn` — parse/format/diff log sequence numbers.

## A word of warning: slots retain WAL

A replication slot pins write-ahead log on the server from its
unconfirmed position onward. A consumer you stop and never restart, or a
slot you abandon, makes the primary retain WAL until disk pressure.
Monitor `pg_replication_slots` (`admin/slots`, `admin/slot-info`) and
`drop-slot!` what you no longer consume.

## Testing

```sh
clojure -M:test -m cognitect.test-runner    # unit + integration
```

The integration suite starts its own throwaway Postgres 17
(`compose.yaml`, port 55432) via docker compose — or rootless podman
where the docker socket is not writable — and skips with a note when no
engine is available. `POSTLOG_TEST_HOST/PORT/USER/PASSWORD/DATABASE`
points it at an existing server (must run `wal_level=logical`);
`POSTLOG_INTEGRATION=0` skips it entirely.

## Roadmap

Not in 0.1: consistent snapshot / backfill of existing rows, transform
and filter pipelines, sink connectors. postlog delivers decoded changes
with ordering guarantees; those belong in layers above (or future
releases).

## License

Copyright © 2026 Ebi

Distributed under the [Eclipse Public License 2.0](https://www.eclipse.org/legal/epl-2.0)
