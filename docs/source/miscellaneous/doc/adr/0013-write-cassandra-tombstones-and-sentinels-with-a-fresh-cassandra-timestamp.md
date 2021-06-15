# 13. Write Cassandra tombstones and sentinels with a fresh Cassandra timestamp
*******************************************************************************

Date: 12/06/2018

## Status

Accepted

## Context

### Mutations

Mutations in Cassandra take place at a Cassandra *writetime* or timestamp. In normal Cassandra operation, these times
are the wall-clock times where a mutation took place. These are used to "resolve" conflicts when performing reads;
for a given column, the most recently written value will be read ("last-write-wins").

AtlasDB columns take the form `(row, column1, column2, value)` where `column2` is the Atlas timestamp. These timestamps
are unique - thus, for transactional tables, no mutations to the same column should take place apart from deleting
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
- the insertion of a fresh sweep sentinel after a tombstone that covers the sentinel, written at timestamp `TS`, must 
  have a Cassandra timestamp greater than `TS`

Notice that the existing system does not satisfy the third invariant.
Switching from `CONSERVATIVE` sweep (writes a sentinel) to `THOROUGH` sweep
(which removes sentinels) and back is broken, in that the sentinels written by the conservative sweep after
the reversion of the strategy will not be visible to readers.

Things behave differently for non-transactional tables:

- most of these tables (`_timestamp`, backup lock, schema mutation lock, schema metadata) are written to at `0`. 
- the `_metadata` table is written to and read from at wall-clock time.

### Compaction

Cassandra stores its data in multiple SSTables which are periodically compacted together. Compactions can help to
clear deleted data and reclaim disk space. This is when tombstones are reclaimed as well. 

When compacting SSTables together, if the most recent value is a tombstone then Cassandra needs to determine whether 
the tombstone can safely be reclaimed or not. Cassandra has a first check based on the real-time age of the tombstone
and `gc_grace_seconds`, which gives us some protection against zombie data when a failed node recovers. 
Assuming that check passes, we then look at the minimum write timestamp of SSTables not involved in 
the compaction. More precisely, reclaimable tombstones are tombstones where the deletion timestamp is less than the 
lowest timestamp of 

1. all other SSTables that include the partition being deleted, and
2. any cells within the same partition in memtables

Otherwise, we may have live data that the tombstone was covering suddenly become visible again.

Since Atlas writes its sweep sentinels at timestamp `-1`, some SSTables or memtables will have a minimum timestamp of 
`-1` which may prevent compactions on subsets of SSTables from dropping many legitimately reclaimable tombstones.

## Decision

Change the timestamps at which sweep sentinels, tombstones and range tombstones are written to be a fresh timestamp
from the timestamp service. In the case of sentinels and tombstones, a single API call to add a set of garbage
collection sentinels or delete a set of cells makes one call for a fresh timestamp to the timestamp service. Cell write
timestamps are unchanged (so a write at timestamp `TS` still receives a Cassandra timestamp of `TS`). Taking an RPC
overhead here is fine, because these operations don't happen on critical path operations (i.e. they don't occur as part
of normal client read/write transactions).

The write pattern for the metadata table is unchanged (still uses wall-clock time). Some of this is because of a
chicken and egg problem with embedded users of the timestamp service; additionally, there are existing CLIs that
manipulate the metadata table but don't have ready access to a timestamp service. Also, these tables receive minimal
writes and deletes compared to actual transactional data tables, and are thus less of a concern from a Cassandra
strain/performance perspective.

### Proof of Correctness/Safety
Recall the invariants we seek to preserve:

1. a tombstone or range tombstone that covers a cell with the Cassandra timestamp `TS` must have a Cassandra timestamp 
greater than `TS`
2. a tombstone or range tombstone that covers a sweep sentinel with Cassandra timestamp `TS` must have a Cassandra
timestamp greater than `TS`
3. the insertion of a fresh sweep sentinel after a tombstone that covers the sentinel, written at timestamp `TS`, must 
have a Cassandra timestamp greater than `TS`

A cell being covered with Cassandra timestamp `TS` must also have Atlas start timestamp `TS`. Given that this cell is
already written to the database, a fresh timestamp `TS'` is necessarily greater than `TS`, giving us statement 1.

Statements 2 and 3 follow from the guarantees of the timestamp service.

### Migrations/Cutover
Notice that even in the presence of values written under the old scheme, our invariants are preserved.
Statement 1 holds because cell write timestamps haven't changed (and we can re-use the proof above).
For statements 2 and 3, the timestamps of values we are covering would be `-1` and `deletionTimestamp + 1`;
we know that a fresh timestamp will be at least that (since timestamps aren't given out twice).

Timestamps for existing values are not changed, so a major compaction may be necessary before one begins to 
reap the benefits of better tombstone droppability. This may be especially relevant for larger tables that are marked
as `appendHeavyAndReadLight()` (because these use the `SizeTieredCompactionStrategy`) and also for heavier users in
general (there may be several `-1` timestamps in SSTables that are not frequently compacted, and thus an issue).

## Consequences

- Key-value services now need a fresh timestamp supplier for initialisation.

  - An alternative approach where this was optional was considered (and if not provided we would use the legacy mode).
    We decided to mandate providing the supplier as the benefits of better tombstone droppability are significant.
  - The fresh timestamp supplier is unused by key value services other than Cassandra.

- Inserting sentinels or writing tombstones/range tombstones requires one additional RPC.

  - These operations do not occur on critical paths (i.e. normal read/write transactions for clients) and may thus be
    less performance-sensitive. Reads and writes on critical paths do not suffer from any extra RPC overhead here.
  - A memoisation approach was considered, but naïve memoisation is wrong - there is a risk where if you write a value
    and then delete it shortly after, the memoized timestamp given to the tombstone could be less than the fresh
    timestamp given to the write. In that case, the value remains live when it should not.
  - A variant of the above that uses the max of the memoized timestamp and (writeTimestamp + 1) was briefly considered,
    though we opted for not memoizing for simplicity and because these are not hot codepaths.
    
