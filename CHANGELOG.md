# Change Log
All notable changes to this project will be documented in this file. This change log follows the conventions of [keepachangelog.com](http://keepachangelog.com/).

## [Unreleased]
### Added
- Project scaffold from the `clj-new` library template.
- `ebifriedrice.postlog.lsn`: parse, format, and diff log sequence numbers.
- `ebifriedrice.postlog.pgoutput`: decoder for pgoutput protocol-v1 frames
  (begin/commit/relation/type/insert/update/delete/truncate/origin/message,
  tuple data with NULL and unchanged-TOAST values).
- `ebifriedrice.postlog.relations`: relation cache; shapes decoded frames
  into change maps with full and key-only before images, injectable
  value decoding.
- `ebifriedrice.postlog.decode`: text-format value decoding for built-in
  types — numbers, booleans, numerics, bytea, dates/times/timestamps
  (with infinity markers), and arrays.
- `ebifriedrice.postlog.admin`: replication slots and publications
  (create/ensure/drop/introspect), JDBC URL builders for regular and
  replication connections.
- `ebifriedrice.postlog.replication`: pgjdbc-backed logical streams
  (`LogicalStream` protocol: read-pending/ack/close).
- `ebifriedrice.postlog.consumer`: the consumer loop — option
  validation, slot/publication bootstrap, single-threaded delivery with
  exponential-backoff retry, transaction assembly, LSN acking at commit
  boundaries, reconnect with backoff, graceful stop.
- Integration suite against a self-managed Postgres 17 (docker compose
  with rootless-podman fallback; `POSTLOG_TEST_*` overrides).
