# 14. Targeted Sweep
********************

Date: 18/06/2018

## Status

**Accepted**

## Context

### Legacy Sweep

To achieve its transactional guarantees, AtlasDB maintains historical versions of cells that have been written to. Eventually,
all existing and future transactions will have a start timestamp large enough that some of these historical versions will never be
visible again, resulting in unnecessary cruft. This is not only an issue due to taking up storage space in the underlying KVS, but
also because certain access patterns require scanning over the obsolete historic versions, leading to significant performance
degradation over time. The process of removing old historic versions of cells from tables in AtlasDB is called **Sweep**.

We will refer to the current implementation of sweep that AtlasDB is relying on as **Legacy Sweep**. Legacy sweep is an iterative
procedure that, given a table reference and a start row, sequentially scans through all the historic versions of each cell searching
for candidates written in transactions with a start timestamp and a commit timestamp both lower than the **Sweep Timestamp**. The
sweep timestamp is a timestamp expected to be lower than the start timestamp of all open transactions, rendering all but the last
historic version for each cell prior to the sweep timestamp effectively invisible and thus obsolete. These versions can therefore be
safely deleted.

Legacy sweep continues these scans until enough candidates have been found, processing at least one full row of cells, and
then deletes each of the obsolete historic versions. There are two main modes for running legacy sweep:

1. Background Sweep, which is a background task that repeatedly chooses a table to sweep and then proceeds to run iterations
of sweep until all the rows have been processed.
2. Manual Sweep, which can be triggered through a REST endpoint or a CLI to perform an iteration or a full sweep for a given table
and start row.

Note that there are corner cases where the sweep timestamp may be mistaken: a read-only transaction that has been running
for longer than one hour (which is also subject to clock drift), or when a write transaction loses its lock while committing. In
tables that allow read-only transactions we must therefore defensively write a **garbage deletion sentinel**, which is an empty
value at a timestamp of -1, i.e., before any valid start timestamp. If a read-only transaction encounters a sentinel, that
signalizes there could have been a historic version it should be able to read that may have been deleted, and the transaction must
therefore abort. If a table allows read-only transactions, and therefore requires sentinels, is defined by the table's
**sweep strategy**: `CONSERVATIVE`, which allows read-only transactions and requires sentinels, and `THOROUGH`, which does not.

Over time we have identified a number of issues with the architecture and implementation of legacy sweep as outlined below.

#### Legacy Sweep is Slow

Even if a table has no historic versions of cells that can be swept, we still have to scan through all historic versions of
all cells in the table, which can take weeks in extreme cases. The performance only gets worse as more data is written to the
KVS, increasing the number of entries legacy sweep must iterate through. This is a particularly large problem for users with tables
whose access patterns mandate that they must be swept regularly for performance or stability reasons. If background sweep is busy
for weeks sweeping some other large table, we must resort to manual sweeps to avoid performance and stability degradation. This is
obviously error-prone and subject to random failures, since if the manual sweep is interrupted for any reason, it will not be
automatically retried.

Moreover, legacy sweep depends on complicated heuristics to decide which table to sweep next. Given that some tables must be swept
frequently, and the slowness of legacy sweep, significant developer time must be spent tweaking the heuristic to produce the desired
effect.

#### Legacy Sweep can Exert Significant Pressure on the Underlying KVS

Scanning through a table to find historic versions of cells can in some cases cause significant pressure on the underlying KVS,
in particular for Cassandra. Even though we are only interested in the start and commit timestamp of the transaction in which the
writes were performed, it is our understanding that Cassandra internally still loads the contents of the cell in memory regardless.

#### Legacy Sweep can Get Stuck

For tables that regularly have new rows added in increasing lexicographical order (for example, tables keyed on steadily  increasing
`FIXED` or `VAR_LONG` s), legacy sweep can end up sweeping the table indefinitely as each iteration will discover a new row, therefore
never declaring the table as fully swept. As a consequence, no other tables are swept at all until the issue is noticed and manually
resolved.

## Decision

The problems of legacy sweep are architectural, and cannot be solved by simply improving the implementation.
We have therefore decided to change the architecture of sweep so that it does not require scanning of tables to find candidates
to delete, and instead maintains a **Sweep Queue** which contains the information on all the writes into AtlasDB. This, in
conjunction with **ranged tombstones** (ranged deletions), which delete all versions of a cell between two timestamps, allows us to sweep all tables
in parallel without having to read data from the tables being swept. *Note that, at the time of writing this ADR, only the Cassandra
KVS implementation of ranged tombstones actually avoids reading from the table*.

### Targeted Sweep using a Targeted Sweep Queue

The **targeted sweep queue** is a persisted queue containing the relevant metadata for each write that was about to be committed into AtlasDB.
This metadata is encapsulated in a `WriteInfo` object and contains the following:

- `TableReference` of the table written to.
- `Cell` written to, i.e., the row and column names.
- A **start timestamp** of the transaction that performed the write.
- A flag specifying if the write was a **tombstone**, i.e., a delete, or a regular write.

On a high level, sweeping using the targeted sweep queue, i.e., **targeted sweep** is as follows:
1. Whenever a transaction is about to commit, before any of its writes are persisted to the KVS, for each of the writes put a
corresponding `WriteInfo` into the queue.
2. Targeted sweep reads an entry from the front of the queue. Depending on the sweep strategy for the table specified by the entry, we
acquire an appropriate *sweep timestamp*, and compare it to the *start timestamp* of the transaction.

  - If *sweep timestamp <= start timestamp*, we must pause and try later.

3. Check the *commit timestamp* of the transaction.

  - If the transaction is not committed, abort it.
  - If the transaction is aborted, delete the write from the KVS and pop the queue and read the next entry.
  - If the transaction is committed at a timestamp greater or equal to the *sweep timestamp*, pause and try later.

4. Otherwise, insert a ranged tombstone as follows:

  - If the strategy is conservative: write a garbage deletion sentinel, then put a ranged tombstone deleting all versions of the
    cell between 0 and write timestamp - 1, not deleting the sentinel or the write.
  - If the strategy is thorough

    - If the write was a tombstone, then put a ranged tombstone deleting all versions of that cell between -1 and write timestamp,
      deleting both a potentially existing sentinel and the write.
    - Otherwise, put a ranged tombstone deleting all versions of that cell between -1 and write timestamp - 1, deleting a
      potentially existing sentinel, but not the write.

5. Pop the queue and read the next entry.

For a detailed implementation of targeted sweep and the targeted sweep queue, refer to the implementation section below.

## Consequences

### Benefits
- Targeted sweep can perform sweeps several orders of magnitude times faster than legacy sweep (this has only been verified on Cassandra so far).
- The load on the underlying KVS is significantly reduced (this has only been verified on Cassandra so far).
- The order of sweeping is more fair and does not suffer from issues caused by frequently appending new rows to the end of a table.

### Drawbacks

- Added overhead to committing transactions, as the information must be persisted to the sweep queue as part of the commit. Note
  that this has not caused significant regression in our benchmarks.

## Implementation

Since we assume that threads may die at any moment, for most of the implementation details below, correct ordering is crucial.
Before we describe the targeted sweep schema, we will define the necessary terms. A **Fine Timestamp Partition** of a timestamp *ts*
is *ts* divided by *50_000*, using integer division. A fine timestamp partition of a write is the fine
timestamp partition of the start timestamp of the transaction that wrote it. A **Coarse Timestamp Partition** is analogous to the
fine partition, except the number divided by is *10_000_000*. A **Sweep Strategy** is a table-level property specifying how the
sweep timestamp should be calculated, if we need deletion sentinels, and whether the latest write should be swept away as well in
case it is a delete. We also use a sharding strategy
to enable better parallelisation of targeted sweep. The maximum number of **Shards** that is supported is *256*, effectively
splitting the queue into that number of disjoint queues. Note that once that the number of shards is increased, it cannot be
lowered again.

### Sweepable Cells Table

  This table stores the actual information about all the writes into AtlasDB. As a transaction is about to be committed, but before
  data is persisted to the KVS, all writes from the transaction are partitioned based on shard and sweep strategy
  of the table. Then, if there are 50 or fewer writes in a partition, we persist the necessary information in the same number of cells
  in a **non-dedicated row**, with the row components calculated as described below. If there are more than 50 writes in a
  partition, we only insert a single cell to the non-dedicated row acting as a reference to one or more **dedicated rows** which are
  used exclusively for the writes of this transaction and partition.

  **Row components**:

  - **timestamp_partition**: a `VAR_LONG` derived from the start timestamp of the writes in that row. For *non-dedicated* rows, this
    is the fine timestamp partition. For *dedicated rows*, this is the start timestamp itself.
  - **metadata**, a 4 byte `BLOB` encoding of a `TargetedSweepMetadata` as follows:

    - 1 bit for *sweep strategy*: 0 for thorough, and 1 for conservative.
    - 1 bit marking if this is a *dedicated row*: 0 for non-dedicated, 1 for dedicated.
    - 8 bits for *shard number*, between 0 and 255 inclusive.
    - 6 bits for use by dedicated rows, marking its ordinal number, between 0 and 63 inclusive.
    - 16 bits unused for now.

  Note that the row components are hashed to avoid hot-spotting.

  **Column components**:

  - **timestamp_modulus**: a `VAR_LONG` storing the start timestamp of the write modulo 50_000.
  - **write_index**: a `VAR_SIGNED_LONG` whose purpose is overloaded. In case we are storing 50 or fewer writes into the same row,
    the *write_index* is a non-negative increasing number used to deduplicate multiple writes with the same start timestamp. If we
    are storing more writes, we instead only put a single cell in the non-dedicated row acting as a reference to one or more
    dedicated rows. In this case, the *write_index* is a negative number between -1 and -64 indicating how many dedicated rows we are
    using (each dedicated row can contain up to *100_000* cells). In dedicated rows, the *write_index* is once again used for deduplication since all entries in the row will have the same
    *timestamp_modulus*.

  **Value**: a `WriteReference` containing the remaining required metadata for targeted sweep: the `TableReference`, `Cell`, and
    a `boolean` specifying if the write was a tombstone.

  Since a persisted size of a `Cell` object can be up to 3_000 bytes (dominating the size of the above entry), and we expect at
  most 12_500 transactions per row, by allowing at most 50 writes from the same transaction in a row, the size of one non-dedicated
  row should not exceed 2 GB and should in practice be lower than 100 MB. Note that we could technically have the full 50_000 transactions,
  in a row, but the probability of exceeding the values above is infinitesimal. We allow a maximum of 100_000 entries per dedicated row,
  which ensures that each dedicated row should not be
  larger than 300MB while still allowing up to 6.4 million writes in a single transaction, and in practice even more, as long as
  the number of shards is greater than 1. Note that for each cell in a non-dedicated row, we can calculate the write's start
  timestamp simply by multiplying the *timestamp_partition* by *50_000* and then adding its *timestamp_modulus*.

### Sweepable Timestamps Table

  This is an auxiliary table for locating the next row of the *sweepableCells* table to read since the timestamp partitions can be
  sparse, and therefore requiring many lookups to locate a nonempty row. Each non-dedicated row of *sweepableCells* is represented
  by a single cell in this table.

  **Row components**:

  - **shard**: a `VAR_LONG` containing the shard for which the row has entries for.
  - **timestamp_partition**: a `VAR_LONG` corresponding to the *coarse timestamp partition* of all entries in the row.
  - **sweep_conservative**: a `boolean` (encoded as a `BLOB`) specifying if the row contains entries for thorough (`0x00`) or
    conservative sweep (`0x01`).

  **Column components**:

  - **timestamp_modulus** is the **fine timestamp partition** of a row in SweepableCells that falls into the coarse partition
    specified in the timestamp_partition row component.

  **Value**: unused empty byte array.

  To locate the first row of *sweepableCells* with entries after timestamp *ts*, we start with the row of *sweepableTimestamps*
  corresponding to the coarse partition of *ts* and read from the first column with a great enough *timestamp_modulus*, increasing the
  row if necessary. If a cell is found this way, its *timestamp_modulus* is the *timestamp_partition* of the row of *sweepableCells* we
  were looking for.

### Sweep Progress (Per Shard) Table

  This table stores targeted sweep's progress, as well as the information about the number of shards the sweep queue is using.

  **Row components**:

  - **shard**: a `VAR_SIGNED_LONG` containing the shard for which we are tracking progress for.
  - **sweep_conservative**: a `boolean` (encoded as a `BLOB`) specifying if we are looking for conservative or thorough sweep as in
    *sweepableTimestamps*.

  **Named Column**:

  - **value**: a `VAR_LONG` containing the timestamp up to which targeted sweep has swept on that shard and strategy.

  To persist the number of shards sweep queue is using, we use a distinguished row of this table, with the row defined by
  *shard = -1* and *sweep_conservative = true*. Note that all writes to this table use atomic check and set, and the values are only
  allowed to increase. A request to update a cell to a lower value will have no effect.

### Writing to the Sweep Queue

Whenever a `SnapshotTransaction` is about to commit, before its writes are persisted into the KVS, it enqueues
them to the sweep queue:

1. The sweep queue creates a list of `WriteInfo` s containing the relevant information, partitions this list
according to the sweep strategies for the tables (this information is read from the table metadata and then cached), and into the
number of shards that the sweep queue is using (the shard is determined from the hash of the `TableReference` and `Cell` ).
2. For each of the above partitions, we then put an entry into the *sweepableTimestamps* table for the start timestamp of the
transaction.
3. Finally, for each of the partitions, we put the `WriteInfo` s into the *sweepableCells* table.

  - If there are 50 or fewer entries in the list, write that many cells into the table where all the row and column components are
    calculated as described above, with the *write_index* starting at 0 and increasing by 1 for each entry.
  - If there are more than 50 entries in the list, put a single cell into the table acting as a reference to dedicated rows. The row
    of the reference is the same as above, but the write_index is a negative number with an absolute value equal to the number of
    dedicated rows we will use (number of writes divided by 100_000, rounded up). Then, put the cells into dedicated rows, where
    the *timestamp_partition* is the start timestamp of the transaction (**not the fine partition!**), the metadata encodes that the
    row is a dedicated row and the row's ordinal number, and the rest is the same as above.

Note that we use the entire start timestamp as the *timestamp_partition* for dedicated rows to avoid clashes in case a non-dedicated
row has multiple references to dedicated rows.

### Reading from the Sweep Queue

Reading from the sweep queue is done in the same order as writing. For a given shard, strategy, **minimum exclusive
timestamp** (targeted sweep reads this from the *sweepProgressPerShard* table as described later), and **maximum exclusive
timestamp** (for targeted sweep, this is the sweep timestamp), we do the following:

  1. We want to locate the fine timestamp partition for the first row of *sweepableCells* that has entries greater than the minimum
     exclusive timestamp *minTs*. Starting with the coarse partition of *minTs + 1*, we use `getRowsColumnRange` to check if there
     is a cell in SweepableTimestamps satisfying the above condition. If not, increase the coarse partition and repeat until
     either a candidate is found, or the coarse partition grows larger than the coarse partition of *maxTs - 1*, where *maxTs* is
     the maximum exclusive timestamp.
  2. If the latter occurs, we are guaranteed that there are no entries in *sweepableCells* for the shard and strategy and any of the
     timestamps in the specified range.
  3. Otherwise, we now have a fine timestamp partition that is effectively a reference to a row of SweepableCells that is expected
     to contain at least one cell (this may not be true in degenerate cases where a thread writing or cleaning the queue dies during
     the process). We now read cells from that row, with start timestamp greater than the last swept timestamp, referring to
     dedicated rows as necessary, until we either finish the row, exceed the *maxTs*, or we have read 100_000 entries, plus any
     additional entries to ensure we have read all the entries with the same start timestamp as the latest one.

### Cleaning the Sweep Queue

Once an entire non-dedicated row of *sweepableCells* or a row of *sweepableTimestamps* are not needed anymore, we remove them as
follows.

1. Given a shard, strategy, and fine timestamp partition, delete all entries from *sweepableCells* for the corresponding row. Note
   that in Cassandra, an entire row can be deleted at once using a single tombstone.

    - First, we read through the non-dedicated row to find all the references to dedicated rows and delete them.
    - Then, delete the the non-dedicated row.

2. Then, if necessary, given a coarse timestamp partition, also delete the row defined by the shard, strategy, and the coarse
   timestamp partition in *sweepableTimestamps*.

### Targeted Sweep Implementation

Targeted sweep reads the write metadata from the sweep queue instead of sequentially scanning tables to find historic versions.
After an entry is read from the sweep queue, we check the commit timestamp of the transaction that performed the write.

- If both the start timestamp and the commit timestamp are lower than the sweep timestamp, we can use a single ranged tombstone
  to delete all prior versions of the cell. Note that the Cassandra implementation of the `deleteAllTimestamps` method that writes
  this ranged tombstone does not require reading any information from the KVS and therefore provides a substantial improvement in
  comparison to legacy sweep that needs to find all the previous timestamps so they can be deleted one by one.

- If the commit timestamp is greater than the sweep timestamp, targeted sweep must wait until the sweep timestamp increases
  enough so that the entry can be processed. This is generally not an issue since it is only likely to happen when targeted
  sweep is processing writes that were written within the last hour.

Targeted Sweep has a number of background threads for each strategy (as controlled by the install config) continuously cycling
through the shards. To find the next shard to sweep, the thread requests a TimeLock lock for the shard and strategy. If successful, it
starts an iteration of targeted sweep; otherwise, it requests a lock for the next shard, finally giving up and pausing if it cycles
unsuccessfully through all the shards. This mechanism ensures synchronization across multiple nodes of the service. Assuming the
thread successfully acquired a lock, we do the following

  1. Calculate the *sweep timestamp* for the sweep strategy used and read the *last swept timestamp* for the shard and strategy
  from *sweepProgressPerShard* .
  2. Get a batch of `WriteInfo` s from the sweep queue: read from the sweep queue as described above, where *minTs* is the *last
  swept timestamp* and *maxTs* is the *sweep timestamp* .

        - If we do not find any candidates, skip to step 6.

  3. For each of the start timestamps in the batch, we check if the transaction was committed; if not we must abort it (this is the
  same behaviour as legacy sweep). If a transaction was committed, but **after** the *sweep timestamp*, we must not progress
  targeted sweep past its start timestamp, so we remove it and all the writes with greater start timestamps from the batch.
  4. Delete all writes that are referenced to from aborted transactions. Note that these are direct deletes, not ranged tombstones.
  5. Partition the remaining `WriteInfo`s by `Cell` and `TableReference`, and then take the greatest start timestamp from each
  partition. We only need one ranged tombstone per partition here, as all the other writes to that cell and table in this batch
  are going to have a lower start timestamp and are therefore going to be deleted as well.

       - If the strategy is conservative: write a garbage deletion sentinel, then put a ranged tombstone deleting all versions of that
         cell between 0 and write timestamp - 1, not deleting the sentinel or the write.
       - If the strategy is thorough

            - If the write was a tombstone, then put a ranged tombstone deleting all versions of that cell between -1 and write timestamp,
              deleting both a potentially existing sentinel and the write.
            - Otherwise, put a ranged tombstone deleting all versions of that cell between -1 and write timestamp - 1, deleting a
              potentially existing sentinel, but not the write.

  6. If the new sweep progress (described in greater detail below) guarantees that the minimum start timestamp swept in the future
  will be in a greater fine, or coarse, partition than the previous *last swept timestamp*, then clean the sweep queue accordingly
  as previously explained.
  7. Persist the sweep progress in *sweepProgressPerShard* for the shard and strategy.
  8. Finally, regardless of the success or failure of the iteration, unlock the TimeLock lock for the shard and strategy and
  schedule the next iteration of sweep for the thread after a delay of 5 seconds.

**Calculating Sweep Progress**:

We wish to update progress to the greatest value we can guarantee we swept to. There are multiple cases to consider, in order:

- If we do not find a candidate row of *sweepableCells* while reading from the sweep queue, we can update to *sweep timestamp - 1*.
- If none of the timestamps from the batch were committed after the *sweep timestamp* and we have read all entries in
  *sweepableCells* up to *sweep timestamp*, then we can update to *ts - 1*, where *ts* is the minimum between
  the *sweep timestamp* and the first timestamp that would be written into the next row of *sweepableCells*.

- Otherwise, update to *ts*, where *ts* is the greatest timestamp among the writes in the batch.
