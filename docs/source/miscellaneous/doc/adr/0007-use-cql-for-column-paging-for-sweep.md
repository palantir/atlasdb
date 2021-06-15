# 7. Use CQL for column paging for sweep
****************************************

Date: 22/08/2016

## Status

Proposed

## Context

As of version 0.12.0, our implementation of sweep for Cassandra relied upon the getPageWithRangeCreator method of
CassandraKeyValueService, which fetches values for all columns and timestamps from Cassandra. In cases where a cell with
a large value was overwritten (with other large values) many times, this caused Cassandra to run out of memory, even
if only a single row was fetched. (internal issue 44272).

We needed a way to run sweep without Cassandra running out of memory in such cases.

## Decision

We are unaware of any way to fetch columns and timestamps without also temporarily loading values into memory (within
Cassandra). Therefore, to avoid running out of memory, we needed to make it possible to fetch only a certain number of
cells (rather than rows) from Cassandra at once.

We decided to introduce a more granular batching solution. In particular, we decided to page through the
columns for each row. Briefly, the algorithm implemented is as follows:
1. Fetch a number of rows equal to the row batch size (but only one column per row).
2. Use CQL queries with limits to collect the <column, timestamp> pairs.

Further, since performing a number of additional CQL queries for every row will be less efficient, and unnecessary for
most users, we decided to add an optional parameter, timestampsGetterBatchSize, in CassandraKVSConfig, and use the method
described above only if this parameter is set.

We chose CQL queries over thrift because CQL queries are simpler to write, and have been empirically shown to have
higher throughput.

## Consequences

There is an additional parameter, timestampsGetterBatchSize, in CassandraKVSConfig. If the user sets this parameter and then
runs sweep, the sweeper will page through the columns of each row while searching for timestamps, resulting in a process
that is possibly less efficient, but is also more flexible, and less likely to cause Cassandra to run out of memory.
