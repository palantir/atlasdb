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

- a tombstone or range tombstone that covers a cell with the Cassandra timestamp `TS` must have a Cassandra timestamp 
  greater than `TS`
- a tombstone or range tombstone that covers a sweep sentinel with Cassandra timestamp `TS` must have a Cassandra
  timestamp greater than `TS`
- the insertion of a fresh sweep sentinel after a tombstone or range tombstone at time `TS` must have a Cassandra
  timestamp greater than `TS`

Notice that the existing system does not satisfy the third invariant.
Switching from `CONSERVATIVE` sweep (writes a sentinel) to `THOROUGH` sweep
(which removes sentinels) and back is broken, in that the sentinels written by the conservative sweep after
the reversion of the strategy will not be visible to readers.

Things behave differently for non-transactional tables:

- most of these tables (`_timestamp`, backup lock, schema mutation lock, schema metadata) are written to at `0`. 
- the `_metadata` table is written to and read from at wall-clock time.

### Compaction

Cassandra stores its data in multiple SSTables which are periodically compacted together. Compactions can help to
clear deleted data and reclaim disk space. This is when tombstones are resolved as well. 

When compacting SSTables together, if the most recent value is a tombstone then Cassandra needs to determine whether 
the tombstone can safely be dropped or not. This is based on the minimum write timestamp of SSTables not involved in 
the compaction. More precisely, droppable tombstones are tombstones where the deletion timestamp is less than the 
lowest timestamp of all other SSTables that include the partition being deleted (otherwise we may have live data that 
the tombstone was covering that now becomes readable).

Since Atlas writes its sweep sentinels at timestamp `-1`, some SSTables will have a minimum timestamp of `-1` which
may prevent compactions on subsets of SSTables from dropping many legitimately droppable tombstones.

## Decision

Change the timestamps at which sweep sentinels, tombstones and range tombstones are written to be a fresh timestamp
from the timestamp service. In the case of sentinels and tombstones, a single API call to add a set of garbage
collection sentinels or delete a set of cells makes one call to the fresh timestamp service.

### Proof of Correctness/Safety
Recall the invariants we seek to preserve:

1. a tombstone or range tombstone that covers a cell with the Cassandra timestamp `TS` must have a Cassandra timestamp 
  greater than `TS`
2. a tombstone or range tombstone that covers a sweep sentinel with Cassandra timestamp `TS` must have a Cassandra
  timestamp greater than `TS`
3. the insertion of a fresh sweep sentinel after a tombstone or range tombstone at time `TS` must have a Cassandra
  timestamp greater than `TS`

A cell being covered with Cassandra timestamp `TS` must also have Atlas start timestamp `TS`. Given that this cell is
already written to the database, a fresh timestamp `TS'` is necessarily greater than `TS`, giving us statement 1.

Statements 2 and 3 follow from the guarantees of the timestamp service.

### Migrations/Cutover

## Consequences

- Key-value services now need a fresh timestamp for initialisation
- Inserting sentinels or writing tombstones/range tombstones requires one additional RPC.
  - A memoisation approach was considered, but naïve memoisation is wrong.