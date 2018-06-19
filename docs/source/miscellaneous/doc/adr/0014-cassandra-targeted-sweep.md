# 14. Cassandra Targeted Sweep

Date: 18/06/2018

## Status

Accepted

## Context

### Legacy Sweep

To achieve its transactional guarantees, AtlasDB maintains historical versions of cells that have been written to. However,
eventually all transactions will have a start timestamp large enough that some of these historical versions will never be
visible again, resulting in unnecessary cruft. This is not only an issue due to taking up storage space in the underlying
DB, but also because certain access patterns require scanning over these obsolete historic versions leading to significant
performance degradation over time. The process of removing old historic versions of cells from tables in AtlasDB is called
Sweep.

We will refer to the current implementation of sweep that AtlasDB is relying on as legacy sweep. Legacy sweep is an iterative
procedure that, given a table reference and a start row, scans through all the historic versions of each cell in order to find
candidates written in transactions with a start timestamp and a commit timestamp both lower than the `sweep timestamp`. The
sweep timestamp is a timestamp expected to be lower than the start timestamp of all open transactions, rendering the historic
versions of cells prior to it unable to be read and thus obsolete, with the exception of the last such write for each cell.
Legacy sweep continues these scans until enough candidates have been found, processing at least one full row of cells, and
then deletes each of the obsolete historic versions. There are two main modes for running legacy sweep:

1. Background Sweep, which is a background task that repeatedly chooses a table to sweep and then proceeds to run iterations
of sweep until all the rows have been processed.
- Manual Sweep, which can be triggered through a REST endpoint to perform an iteration or a full sweep for a given table
and start row.

Over time we have identified a number of issues with the architecture and implementation of legacy sweep as outlined below.

#### Legacy Sweep is Slow

Even if a table has no historic versions of cells that can be swept, we still have to scan through all historic versions of
all cells in the table, which can take weeks in extreme cases. The performance only gets worse as more data is written to the
DB, increasing the number of writes legacy sweep must iterate through. This is a particularly big problem for products that
have tables that must be swept regularly for performance reasons. If background sweep is busy for weeks sweeping some other
large table, we must resort to manual legacy sweeps to avoid performance degradation. This is error-prone and subject to random
failures as if the manual sweep is interrupted for any reason, it will not be automatically retried.

#### Legacy Sweep can Exert Significant Pressure on the Underlying DB

Scanning through a table to find historic versions of cells can in some cases cause significant pressure on the underlying DB,
in particular Cassandra. Even though we are only interested in the start and commit timestamp of the transaction in which the
writes were performed, the hypothesis is that Cassandra internally still loads the contents of the cell in memory regardless.
This is an issue with the implementation of the `getAllTimestamps` method.

#### Legacy Sweep can Get Stuck

For tables that have new rows appended to regularly, legacy sweep can end up sweeping the table indefinitely as each iteration
will discover a new row, therefore never declaring the table as fully swept.

## Decision

The problems of legacy sweep are on the level of its architecture, and cannot be solved by simply improving the implementation.
We have therefore decided to change the architecture of sweep so that it does not require scanning of tables to find candidates
to delete, and instead maintain a `sweep queue` which contains the information on all the writes into AtlasDB. This, in
conjunction with `ranged tombstones` allows us to sweep in a targeted way, on all tables in parallel, without having to read any
data from the tables being swept.

### Targeted Sweep Queue

The sweep queue is a persisted queue containing the metadata for each write that was about to be committed into AtlasDB. This
metadata is as follows:

- `TableReference` of the table written to.
- `Cell` written to, i.e., the row and column names.
- `Start timestamp` of the transaction that performed the write.
- Information specifying if the write was a tombstone, i.e., a delete, or a regular write.

#### Sweep Queue Implementation

Since targeted sweep must be aware of all writes persisted into the DB, we have to ensure that they are first persisted into
the queue. Furthermore, we can only delete entries from the queue after targeted sweep has processed them. Before we describe
the targeted sweep schema, we will define the necessary terms.

- **FINE TIMESTAMP PARTITION** is the start timestamp of the transaction performing the write divided by 50_000.
- **SWEEP STRATEGY** is a table-level property specifying how the sweep timestamp is calculated, whether we need `deletion
sentinels`, and if the latest write should be swept away too in case it is a delete. There are two currently supported
strategies: `CONSERVATIVE` and `THOROUGH`.
- **SHARDS** to enable better parallelisation of targeted sweep, we can decide to use up to 256 shards, effectively splitting
the queue into that number of disjoint queues.
- test
- bla

Text

1. `SweepableCells` is the table that stores the actual information about all the writes into AtlasDB.

  **Row components**:
  - `timestamp_partition`, i.e. the fine timestamp partition of the writes.
  - `metadata`, which is a blob encoding the sweep strategy, shard number, and the information on whether this is a dedicated
    row.

  All transactional writes into AtlasDB are partitioned according to their fine timestamp partition, sweep strategy of the
    table, and the shard number for that particular write. Each row of the SweepableCells table (with the exception of
    dedicated rows explained below) contains the information about all the writes in AtlasDB for that partition.

  **Column components**:
  - `timestamp_modulus` is the start timestamp of the transaction performing the write modulo 50_000.
  - `write_index`: is a non-negative increasing number used to deduplicate multiple writes in a single transaction.

  The timestamp modulus can be used, together with the timestamp partition to recompute the start timestamp of the
  transaction performing the write, while the write index is necessary since multiple writes from the same transaction can
  fall into the same partition.

  **Value**: a `WriteReference` containing the remaining required metadata for targeted sweep: the TableReference, Cell, and
    a boolean specifying if the write was a tombstone.

  Since a Cell size can be up to 3_000 bytes, and we can have at most 12_500 transactions per row, if we allow at most 50 writes
  from the same transaction in a row, we can ensure that the size of one row will not exceed 2 GB and should in practice be
  lower than 100 MB. However, we still need to persist writes even when there are more than 50 writes in a single transaction that
  should be put into the same row. When this occurs, we use dedicated rows.

  **Dedicated rows**: If we need to write information on more than 50 writes into the same row, we instead only put a single
  entry acting as a reference to one or more dedicated rows. This is encoded by setting the write index to a negative number
  indicating how many dedicated rows we are using, up to a maximum of 64 rows. Each dedicated row will contain only the 100_000


- second table

### Targeted Sweep

Targeted sweep reads the write metadata from the sweep queue instead of scanning a table to find historic versions. After an
entry is read from the sweep queue, we check the commit timestamp of the transaction that performed the write.

- If both the start timestamp and the commit timestamp are lower than the sweep timestamp, we can use a single ranged tombstone
to delete all prior versions of the cell. Not that the Cassandra implementation of the `deleteAllTimestamps` method that writes
this ranged tombstone does not require reading any information from the DB and therefore provides a substantial improvement in
comparison to legacy sweep that needs to find all the previous timestamps so they can be deleted one by one.

- If the commit timestamp is greater than the sweep timestamp, targeted sweep must wait until the sweep timestamp increases
enough so that the entry can be processed. This is generally not an issue since this is only likely to happen when targeted
sweep is processing writes that were written within the last hour, implying there are no obsolete historic versions anywhere in
the keyspace that were obsolete for more than an hour.

#### Targeted Sweep Implementation

