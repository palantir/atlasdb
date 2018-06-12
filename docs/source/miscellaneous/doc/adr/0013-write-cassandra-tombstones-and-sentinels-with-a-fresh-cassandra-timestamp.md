# 13. Write Cassandra tombstones and sentinels with a fresh Cassandra timestamp

Date: 12/06/2018

## Status

Accepted

## Context

### Mutations

Mutations in Cassandra take place at a Cassandra *writetime* or timestamp. In normal Cassandra operation, these times
are the wall-clock times where a mutation took place. These are used to "resolve" conflicts when performing reads;
for a given column, the most recently written value will be read ("last-write-wins").

Notice that AtlasDB columns take the form `(row, column1, column2, value)` where `column2` is the Atlas timestamp.
These timestamps are unique - thus, for transactional tables, no overwrites should take place apart from removing
a cell that is no longer needed (e.g. because of sweep).

In the context of AtlasDB, reliance on wall-clock time is generally considered unacceptable. Thus, Atlas uses its
own timestamps:

- a cell written at a start timestamp `TS` is given the Cassandra timestamp `TS`
- a sweep sentinel is given the Cassandra timestamp `-1`
- a tombstone inserted by deleting a cell with start timestamp `TS` is given the Cassandra timestamp `TS + 1`
- a range tombstone from time `0` to `TS` exclusive is given the Cassandra timestamp `TS`

For transactional tables, there are several key invariants to be preserved:

- a tombstone or range tombstone that covers a cell with the timestamp `TS` must have a Cassandra timestamp greater
  than `TS`
- a tombstone or range tombstone that covers a sweep sentinel

Notice that in the existing system, switching from `CONSERVATIVE` sweep (writes a sentinel) to `THOROUGH` sweep
(which removes sentinels) and back is broken, in that the sentinels written by the conservative sweep after
the reversion of the strategy will not be visible to readers.

Things behave differently for non-transactional tables:

- most of these tables (`_timestamp`, backup lock, schema mutation lock, schema metadata) are written to at `0`. 
- the `_metadata` table is written to and read from at wall-clock time.

## Decision


## Consequences

