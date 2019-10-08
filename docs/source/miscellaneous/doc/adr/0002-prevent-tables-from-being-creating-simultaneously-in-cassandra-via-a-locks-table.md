# 2. Prevent tables from being created simultaneously in cassandra via a locks table

Date: 18/05/2016

## Status

Accepted

Superseded by [PR #3620: Use CQL to create tables](https://github.com/palantir/atlasdb/pull/3620).

## Context

Cassandra [has an issue](https://issues.apache.org/jira/browse/CASSANDRA-10699) which can cause data loss in the situation:

1. Node A and node B concurrently create table "some.table"
2. Both table creations succeed, creating tables with column family ids "123" and "456"
3. Cassandra picks "123" to be the correct table for "some.table"
4. Cassandra is restarted
5. After restart Cassandra gossips and decides that "456" is the correct table for "some.table"
6. All data that was written to "123" is now lost

To fix this we must prevent tables from being created at the same time.


## Decision

All schema mutations will globally synchronise via a check-and-set operation on a specific cell in a _lock table.

## Consequences

- If a node attempts to mutate the schema but dies before releasing the lock, then the lock will never be released. This will require manual intervention.
- We need to ensure that the lock table does not suffer from this bug, so the lock table must be created in a synchronous manner.
- Client startup will become slower as part of the startup will be linearized.
