# Introduction to ebifriedrice/postlog

postlog is a Postgres logical-replication consumer library: it streams
commits from the write-ahead log and delivers decoded changes to your
handlers, in order, at-least-once.

See the [README](../README.md) for the quickstart, the change-map and
option reference, delivery semantics, and how to run the test suite.
