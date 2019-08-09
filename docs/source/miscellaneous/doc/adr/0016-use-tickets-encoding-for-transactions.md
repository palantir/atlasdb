# 16. Use the tickets encoding for the transactions table (_transactions2)

Date: 05/04/2019

## Status

Technical decision has been accepted.
This architectural decision record is still a work in progress.

## Context

### The Transactions Table in AtlasDB

In AtlasDB, the ``_transactions`` table keeps track of whether a transaction that had started at a given timestamp
was committed or aborted (and in the case of it being committed, its commit timestamp). Logically, this table is a
mapping of longs to longs, with a special value of ``-1`` meaning that the transaction was aborted. Transactions that
are in-flight and have yet to either commit or abort will not have an entry in the table.

    | startTimestamp | commitTimestamp |
    |             20 |              33 |
    |             28 |              42 |
    |             37 |              -1 |
    |        3141592 |         3141595 |

This table is accessed via the ``TransactionService`` class in AtlasDB, which offers a simple interface:

```java
public interface TransactionService {
    @CheckForNull
    Long get(long startTimestamp);

    Map<Long, Long> get(Iterable<Long> startTimestamps);

    void putUnlessExists(long startTimestamp, long commitTimestamp) throws KeyAlreadyExistsException;
}
```

In practice, calling both the single and multi-timestamp versions of ``get`` may read from this table, though
non-null results may be cached. ``putUnlessExists`` performs a put-unless-exists operation to the table - this
is supported by means of a Thrift ``cas`` call for Cassandra KVS, and by ``INSERT ... IF NOT EXISTS`` SQL statements
in relational KVSes.

### Physical Representation in Cassandra

All AtlasDB tables have a similar schema in Cassandra:

- there is a blob partition key used to store rows, called the ``key``
- the clustering key has two more components:
  - a blob for columns, called ``column1``
  - a big-integer for the timestamp, called ``column2``
- blob values.

In the ``_transactions`` table, ``column1`` is always a single byte corresponding to the string ``t``, and
``column2`` is a special value of ``-1``. In practice, we don't pay much attention to these values.
More interestingly, the ``key`` is a VAR_LONG encoding of the start timestamp, and the ``value``
is similarly a VAR_LONG encoding of the commit timestamp.

The Cassandra representation of the ``_transactions`` table introduced above may look as follows.

    |        key | column1 | column2 |                  value |
    |       0x14 |    0x74 |      -1 |                   0x21 |
    |       0x1c |    0x74 |      -1 |                   0x2a |
    |       0x25 |    0x74 |      -1 | 0xff80ffffffffffffffff |
    | 0xe02fefd8 |    0x74 |      -1 |             0xe02fefdb |

The details of VAR_LONG encoding are fiddly, but it exhibits several desirable properties:

1. VAR_LONG encoding is order-preserving over non-negative numbers; that is, if 0 <= ts1 < ts2
   then VAR_LONG(ts1) < VAR_LONG(ts2) and vice versa. This is significant, because it allows one to perform
   range scans of the transactions table (given that timestamps in Atlas are positive longs) in a straightforward
   way.
2. VAR_LONG encoding supports negative numbers, meaning that we can use the same encoding for both positive timestamps
   (for transactions that successfully committed) and ``-1`` (for transactions that were aborted).

However, our choice of encoding also has some issues. Two particularly relevant ones are as follows:

1. Under VAR_LONG encoding, numbers that are near to each other numerically will also be very close in byte-space
   (notice that the encoded forms of 3141592 and 3141595 only differ in their three lowest-order bits). Furthermore,
   most writes to the transactions table will take place at numbers that are numerically close, because these
   correspond to actively running transactions, and thus at keys that are close in byte-space. Considering that
   Cassandra uses consistent hashing to handle data partitioning, at any given point in time the majority of writes
   to the cluster will end up going to the same node ('hot-spotting'). This is undesirable, because we lose
   horizontal scalability; writes are bottlenecked on a single node regardless of the size of the cluster.
2. VAR_LONG encoding is not particularly efficient for our purposes in that it fails to exploit some characteristics
   of the distribution of our data. In particular:
   1. The special value ``-1`` is particularly large; under VAR_LONG encoding, negative longs will always encode to 10
      bytes.
   2. Storing the full value of the commit timestamp is also wasteful, given that we know that it is generally slightly
      higher than the value of the start timestamp.

### Principles for a Good Transaction Service

We thus define three principles that we can use to help guide our decisions as to what makes an implementation of
a transaction service a good one.

1. **Horizontal Scalability (HS)**: A good transactions service must be horizontally scalable; that is, it should be
   possible to increase write bandwidth by increasing the number of database nodes and/or service nodes that are
   performing writing.
2. **Compact Representation (CR)**: A good transactions service does not unnecessarily use excessive disk space.
   In addition to saving on disk usage, having a compact representation also improves the ability for query results to
   be cached in memory, and can improve time taken to execute queries if less data needs to be read through from disk.
3. **Range Scans (RS)**: A good transactions service supports range scans (without needing to read the entire table
   and post-filter it). This is used in various backup and restore workflows in AtlasDB, and it is important that we
   are able to execute restores in a timely fashion.
4. **Reasonable Usage Patterns (RUP)**: A good transactions service, if using an underlying service, must follow
   standard principles as to what constitutes a reasonable usage pattern of the underlying service.

## Decision

Implement the tickets encoding strategy, along with other features needed to support its efficient operation.
The strategy and supporting features will be introduced in the following sections.

### Tickets Encoding Strategy: Logical Overview

We divide the domain of positive longs into disjoint *partitions* of constant size - we call the size the
*partitioning quantum* (PQ). These partitions are contiguous ranges that start at a multiple of PQ - thus, the first
partition consists of timestamps from 0 to PQ - 1, the second from PQ to 2 * PQ - 1 and so on.

We assign a constant number of rows to a partition (NP), and seek to distribute start timestamps as evenly as possible
among these NP rows as numbers increase. In practice, the least significant bits of the timestamp will be used as
the row number; we would thus store the value associated with the timestamps k, NP + k, 2NP + k and so on in the same
row, for a given partition and value of k in the interval [0, NP). To disambiguate between these timestamps, we use
dynamic column keys - we can use a VAR_LONG encoding of the timestamp's offset relative to the base.

More formally, for a given timestamp TS, we proceed as follows (where / denotes integer division):

- we identify which row R TS belongs to; this is given by (TS / PQ) * NP + (TS % PQ) % NP.
- we identify the column C TS belongs to; this is given by (TS % PQ) / NP.

Notice that given R and C, we can similarly decode the original TS:

- we identify the relevant partition P; this is given by R / NP.
- we identify the offset from the column key, O1; this is given by C * NP.
- we identify the offset from the second part of the row key, O2; this is given by R % NP.
- the original timestamp is then P * PQ + O1 + O2.

It may be easier to think of the timestamp being written as a 3-tuple (P, O1, O2), where the row component is the
pair (P, O2) and the column key is O1; if NP divides PQ, then there is a bijection between such 3-tuples where O2 ranges
from 0 to NP (exclusive), and O1 ranges from 0 to PQ / NP (exclusive). Furthermore, this bijection is order-preserving
where ordering over the 3-tuples is interpreted lexicographically.

This diagram should illustrate more clearly how this works, for PQ = 1,000,000 and NP = 100.

![Illustration of the tickets encoding strategy](0016-tickets-encoding.png)

### Physical Implementation of Tickets

We store information about transactions committed under the new encoding scheme in the ``_transactions2`` table in
Cassandra.
Given that we want to avoid hot-spotting and ensure horizontal scalability, we need to ensure that the rows we may
be writing data to are distributed differently in byte-space. We thus reverse the bits of each row before encoding it.

```java
private static byte[] encodeRowName(long startTimestamp) {
    long row = (startTimestamp / PARTITIONING_QUANTUM) * ROWS_PER_QUANTUM
            + (startTimestamp % PARTITIONING_QUANTUM) % ROWS_PER_QUANTUM;
    return PtBytes.toBytes(Long.reverse(row));
}
```

We are using a fixed-long encoding here, which uses a constant 8 bytes (instead of variable between 1 and 9). This was
chosen to ensure that range scans are supported, as the reversed forms of variable length encodings tend to not be
amenable to range scans.

For the dynamic column keys, we simply use VAR_LONG encoding.

We changed the encoding of values as well. For transactions that successfully committed, we used a delta encoding
scheme instead, where we take the VAR_LONG of the difference between the commit and start timestamps, which is
typically a small positive number. This helps to keep the size of the table down. Separately, for transactions that
were aborted, we store an empty byte array as our special value instead of a negative number, because that is
unnecessarily large.

### Choosing PQ and NP

We choose values of PQ and NP based on characteristics of the key-value-service in which we are storing the timestamp
data, recalling principle RUP. To simplify the discussion in this section, we assume NP divides PQ.

For Cassandra, following the Atlas team's recommended Cassandra best practices, we seek to bound the size of an
individual row by 100 MB. Considering that a VAR_LONG takes at most 9 bytes for positive integers, and we
can explicitly use an empty byte array to represent a transaction that failed to commit, we can estimate the
maximum size of a row for a given choice of values of PQ and NP.

Notice that the number of timestamps we actually need to store is given by PQ / NP (since each of the rows in the
partition is different). For a given start/commit timestamp pair, the row key occupies 8 bytes; that said, in SSTables
the row key only needs to be represented once. We thus focus on the column keys and values.

The column key is a VAR_LONG encoded number that is bounded by PQ / NP, as that's the largest offset we might actually
store. The value theoretically could go up to a full 9 bytes for a large positive number, though in practice is likely
to be considerably smaller.

We selected values of PQ = 25,000,000 and NP = 16. Under this configuration, each row stores at most 1,562,500
start-commit timestamp pairs. Thus, the numbers for the rows only go up to 1,562,500, and VAR_LONG encoding is able
to represent these within 3 bytes. We do need to account for a bit more space as Cassandra needs to create a
composite buffer to include the ``column2`` part of our physical row, but this should not take more than an additional
9 bytes. We thus have 12 bytes for the column key and 9 bytes for the value, leading to a total of 21 bytes per
start-commit timestamp pair. Each row is then bounded by about 32.04 MB, which leaves us quite a bit of headroom.

It is worth mentioning that in practice, for implementation reasons it is very unlikely that we have a full 1,562,500
start-commit timestamp pairs in a single row, and in practice values are likely to be only 2 or 3 bytes rather than
9 bytes. In any case, even under these adverse circumstances we still avoid generating excessively wide rows.

### Streamlining putUnlessExists

The putUnlessExists operation is performed at a serial consistency level in Cassandra, meaning that reads and writes
go through Paxos for consensus. Thrift exposes a check-and-set operation on its APIs.

```
  CASResult cas(1:required binary key,
                2:required string column_family,
                3:list<Column> expected,
                4:list<Column> updates,
                5:required ConsistencyLevel serial_consistency_level=ConsistencyLevel.SERIAL,
                6:required ConsistencyLevel commit_consistency_level=ConsistencyLevel.QUORUM)
       throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te)
```

This was sufficient in the original transactions schema, because each row key only stores information about one
start timestamp; a putUnlessExists operation is then a CAS from an empty row to a row that has one column.
However, notice that this is not sufficient for transactions2, because each row may contain data about multiple start
timestamps. This API requires us to provide a list of the old columns, and we don't know that beforehand.

We considered alternatives of reading the existing row and then adding the new columns, or using the CQL API, because
the behaviour of INSERT IF NOT EXISTS for columns matches the semantics we want. However, both of these solutions were
found to have unacceptable performance in benchmarking.

We thus decided to extend the Thrift interface to add support for a multi-column put-unless-exists operation that
has the semantics we want. This is different from CAS from an empty list, in that this succeeds as long as any of
the existing columns in the column family for the provided key do not overlap with the set of columns being added.

```
  CASResult put_unless_exists(1:required binary key,
                              2:required string column_family,
                              3:list<Column> updates,
                              4:required ConsistencyLevel serial_consistency_level=ConsistencyLevel.SERIAL,
                              5:required ConsistencyLevel commit_consistency_level=ConsistencyLevel.QUORUM)
       throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te)
```

#### Multinode Contention and Residues

This is an improvement, but still runs into issues when clients (whether across multiple service nodes or on the same
node) issue multiple requests in parallel, because each put_unless_exists request requires a round of Paxos. Cassandra
maintains Paxos sequences at the level of a partition (key), so these requests would contend as far as Paxos is
concerned, even if the columns are actually disjoint. Internally, Cassandra nodes are trying to apply updates to the
partition; whether these updates are applied and the order in which they take place is agreed on using Paxos.
Although the nodes will be OK with accepting multiple proposals if they don't conflict, only one round of consensus
can be committed at a time (since updates are conditional). Also, Cassandra uses a leaderless implementation of
Paxos, meaning that the 'dueling proposers' issue might slow an individual round of the protocol down if multiple nodes
are trying to concurrently propose values.

Batching requests on the client side for each partition could be useful, though that is still limited in that
performance would be poor for services with many nodes.

### Cassandra Table Tuning

#### Bloom Filters

#### Index Intervals

#### Compression Chunk Length

### Cell Loader V2

#### Background on Cell Loading

AtlasDB loads most of its data through the ``multiget_slice`` Cassandra endpoint.

```
map<binary,list<ColumnOrSuperColumn>> multiget_slice(1:required list<binary> keys,
                                                     2:required ColumnParent column_parent,
                                                     3:required SlicePredicate predicate,
                                                     4:required ConsistencyLevel consistency_level=ConsistencyLevel.ONE)
  throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te)
```

A ``SlicePredicate`` is a Cassandra struct that allows clients to specify which columns they want to read - these
columns are loaded for each of the keys presented. Notice that this method supports multiple keys but just one
predicate.

Thus, in Atlas when we try and perform a get of a collection of cells (which are row-column pairs), we first
group the pairs by column and then, in parallel, dispatch requests to Cassandra for each row that is relevant.
For example, if one's cells were ``(A, 1), (A, 2), (B, 1), (C, 2), (C, 3), (D, 3)``, then Atlas would send three
requests:

- column ``1`` and keys ``[A, B]``
- column ``2`` and keys ``[A, C]``
- column ``3`` and keys ``[C, D]``

Note that in practice, the requests we make have to use a range predicate on Cassandra, because cells don't include
timestamps, and the latest timestamp at which our cell existed isn't something we know a priori.

#### Multiget Multislice

The above model does not work well for transactions2. Transactions2 cells end up being distributed reasonably evenly
among the columns 0 through PQ / NP (which, in our case, is 312,500). Thus, when attempting to determine whether some
Atlas values had been committed, we will perform many requests in parallel. These requests will end up using many
resources from the Cassandra connection pool; they also incur a lot of overhead in terms of scheduling and network I/O.

We want to be able to batch these calls together. To do this, we added another endpoint to the Thrift interface that
Palantir's fork of Cassandra provides:

```
struct KeyPredicate {
    1: optional binary key,
    2: optional SlicePredicate predicate,
}

map<binary,list<list<ColumnOrSuperColumn>>> multiget_multislice(1:required list<KeyPredicate> request,
                                                                2:required ColumnParent column_parent,
                                                                3:required ConsistencyLevel consistency_level=ConsistencyLevel.ONE)
  throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te)
```

Implementing this endpoint on the Cassandra side was not too difficult. It may seem a little wasteful in that this may
require keys and predicates to be specified more than once, but for transactions2 these would likely be mostly
distinct.

While this improved performance, we still faced significant regressions relative to the v1 cell loader. We determined
that this was because Cassandra has a worker pool for loading values to satisfy a ``ReadCommand``, but the calling
thread in requests is also allowed to participate. Thus, creating large batches would turn out to likely be detrimental
to read performance, even when the Cassandra nodes are actually able to handle higher concurrency safely.

#### Selective Batching

We thus settled on a compromise between sending large singular requests and inundating Cassandra with smaller ones;
unlike in the original ``CellLoader``, we make the batch parameters configurable. There are two parameters:

- cross column load batch limit (CC); we may combine requests for different columns in one call to the DB, but merged
  calls will not exceed this size.
- single query load batch limit (SQ); a single request should never be larger than this size. We expect SQ >= CC.

We still partition requests to load cells by column first. Thereafter,

- if for a given column the number of cells is at least CC, then the cells for that column will exclusively take up
  one or more batches, with no batch having size greater than SQ.
- otherwise, the cells may be combined with cells in other columns, in batches of size up to CC. There is no guarantee
  that all cells for a given column will be in the same batch.

In terms of implementation, we simply maintain a list of cells eligible for cross-column batching, and partition this
list into contiguous groups of size CC. In this way, no row key will be included more than twice. It may be possible
to reduce the amount of data sent over the wire to Cassandra and possibly some of the internal read bandwidth by
solving the underlying bin-packing problem to ensure that each row-key only occurs once; consider that we duplicate
many keys if we want to load CC - 1 cells from many columns. This may be worth considering in the future (while
bin-packing is NP-complete, an algorithm like first-fit decreasing will give us a good approximation), but we have not
implemented it yet as the overhead is only a constant factor, and in many cases with transactions2 we expect the
number of cells per column to be small. Consider that assuming a uniform distribution, even if a single transaction 
reads 1,000,000 values with PQ / NP = 312,500, the maximum batch size will probably not exceed 20.

#### Benchmarking

We tested the selective batching cell loader ("CL2") against the original algorithm ("CL1") and a full-batching
algorithm that always batches cells up to CC, regardless of what rows or columns they are from. We tested these loaders
against both general AtlasDB user workloads (100 rows/100 static columns and 1000 rows/10 static columns), and
workloads more specific to transactions2 (16 rows/500 dynamic columns). This is important as we would prefer not to
have to use a separate codepath for transactions2; current behaviour with loading queries on rows with many different
columns (regardless of table) had also previously been observed to be inefficient.

We first ran the benchmarks with a single thread against the aforementioned workflows. In our tests, CC = 50,000 and
SQ = 200; the dynamic columns are random and are unlikely to have overlaps.

| Rows |     Columns | Metric | CellLoader 1 | Full Batching  | CellLoader 2 |
|-----:|------------:|-------:|-------------:|---------------:|-------------:|
|  100 |  100 static |    p50 |        124.2 |          164.4 |        118.9 |
|  100 |  100 static |    p95 |        169.7 |            204 |        163.5 |
|  100 |  100 static |    p99 |        204.1 |          237.2 |        195.2 |
| 1000 |   10 static |    p50 |        122.6 |          169.4 |        118.6 |
| 1000 |   10 static |    p95 |        164.3 |          222.7 |        161.5 |
| 1000 |   10 static |    p99 |        170.9 |          269.1 |        188.0 |
|   16 | 500 dynamic |    p50 |        328.5 |          144.6 |        102.7 |
|   16 | 500 dynamic |    p95 |        432.3 |          195.5 |        143.8 |
|   16 | 500 dynamic |    p99 |        473.3 |          254.8 |        162.1 |

Notice that for the 100 rows test, CellLoader 2 performs marginally better than CellLoader 1, probably because it is
able to make 50 RPCs instead of 100 (recall that SQ = 200). The full batching algorithm performs the worst, probably
owing to Cassandra latency as there is only one requestor thread apart from the worker pool executing the request.

For the 1000 rows test, CellLoader 1 and 2 performance is very similar. This is expected, as the underlying calls to
the Cassandra cluster are the same (for each column, there is a single RPC). As before, the full batching algorithm
performs poorly.

However, for the 16 rows / 500 dynamic columns test, CellLoader 1 performance is very poor, as it may need to make as
many as 8,000 distinct RPCs owing to the different column keys. The full batching algorithm still suffers from having
just one requestor thread. CellLoader 2 is able to divide this into approximately 40 parallel RPCs, and performs
best overall.

We also ran the benchmarks with 10 concurrent readers on the same workflows:

| Rows |     Columns | Metric | CellLoader 1 | Full Batching  | CellLoader 2 |
|-----:|------------:|-------:|-------------:|---------------:|-------------:|
|  100 |  100 static |    p50 |       1042.0 |         1248.5 |       1027.4 |
|  100 |  100 static |    p95 |       1355.2 |         1636.1 |       1365.1 |
|  100 |  100 static |    p99 |       1530.6 |         1783.2 |       1530.8 |
| 1000 |   10 static |    p50 |       1205.9 |         1172.5 |       1199.7 |
| 1000 |   10 static |    p95 |       1515.1 |         1656.9 |       1502.8 |
| 1000 |   10 static |    p99 |       1595.8 |         1928.3 |       1618.6 |
|   16 | 500 dynamic |    p50 |       3141.8 |          899.2 |        888.4 |
|   16 | 500 dynamic |    p95 |       3390.1 |         1350.6 |       1189.0 |
|   16 | 500 dynamic |    p99 |       3497.2 |         1635.8 |       1307.2 |

The magnitude by which full batching does not perform as well as CellLoader 2 is also much less, possibly because
the worker pool has a finite size and even with the full batching algorithm in this case, each reader contributes
one requesting thread, and although CellLoader2 also spins up many more requesting threads on the Cassandra side,
the Cassandra cluster was unable to actually have these therads all do work concurrently.

### Determining Which Transaction Service To Use

### Live Migrations and the Coordination Service

Transactions2 was written with some of the heaviest users of AtlasDB in mind. These are core Palantir services where
shutdown upgrades are costly, and we thus implemented a mechanism for performing upgrades. Effectively, this involves
defining an internal schema version for AtlasDB, and implementing a mechanism for performing schema transitions
while ensuring nodes that want to commit transactions agree on the state of the world

Effectively, we want to define an internal schema version for AtlasDB, and implement a mechanism for performing schema
transitions while ensuring nodes that want to commit transactions agree on the state of the world - or at least,
agree sufficiently that they won't contradict one another.

For a given start timestamp, we want to be able to decide what mechanism to use to find the commit timestamp (or,
alternatively, if the transaction was aborted).

#### Coordination Service

We introduce a notion of a coordination service, which agrees on values being relevant or correct at a given timestamp.
The sequence of values being agreed needs to evolve in a *backwards consistent* manner - that is, one should avoid
changes that may affect behaviour for decisions made at timestamps before the point where one is making one's update.
More formally, if we read a value for some timestamp ``TS``, then all future values written must then ensure that
decisions made at ``TS`` would be done in a way consistent with our initial read.

Values provided are valid up to a specific timestamp, called the validity bound. When a transform is applied, a fresh
timestamp ``F`` is taken, and the new value will be written with a validity bound ``F + C`` for some constant ``C``.
Going forward, the behaviour for updates up to the previous bound must be preserved (which means decisions would be the
same whether other service nodes read the new value or not). Nodes must not make decisions based on values read with a
validity bound less than their timestamp of interest.

```java
public interface CoordinationService<T> {
    Optional<ValueAndBound<T>> getValueForTimestamp(long timestamp);

    CheckAndSetResult<ValueAndBound<T>> tryTransformCurrentValue(Function<ValueAndBound<T>, T> transform);

    // ONLY FOR METRICS AND MONITORING - NOT FOR PRODUCTION DECISIONS.
    Optional<ValueAndBound<T>> getLastKnownLocalValue();
}
```

##### Coordination Service Persistence

The coordination service needs to persist state; we've implemented this as a non-transactional table in the AtlasDB
key-value service called ``_coordination``. Within this table, each sequence of values being agreed on takes up one row.
The values themselves are stored as dynamic columns with a var-long dynamic column key.

- The dynamic column value stored with dynamic column key 0 is a ``SequenceAndBound`` which indicates the sequence
  number of the currently valid value and the validity bound.
- The values to be agreed upon are stored as dynamic column values. These are stored at dynamic column keys which
  correspond to fresh timestamps taken at the time they were written. Dynamic column keys are stored as ``VAR_LONG``s.

Conceptually, the state of the table may look as follows:

| Sequence Id | Sequence Number |                              Value |
|------------:|----------------:|-----------------------------------:|
|           m |               0 | {"sequence":13007,"bound":5013007} |
|           m |           10001 |                  <metadata object> |
|           m |           13007 |        <different metadata object> |

The sequence ID is the same, to reflect that these values are a part of the same sequence; ``m`` is chosen for
AtlasDB's internal schema metadata in general, and isn't particularly meaningful in and of itself.

The dynamic column value stored at dynamic column key 0 indicates that the ``<different metadata object>`` stored with
dynamic column key ``13007`` is the value currently agreed on, and may be used to make decisions for timestamps up to
``5013007``. The dynamic column value stored at dynamic column key ``10001`` is not active (also note that it may never
actually have been agreed on, if its write was concurrent with that of the value at ``13007``).

Notice that objects in the coordination service aren't inherently timestamp aware (as we wanted to maintain some level
of abstraction for the coordination service). Where needed (and, for example, this *is* needed for transactions2),
it is up to the implementation of coordination service clients to partition the timestamp space on their own.
Generally, in these cases, it is good practice to introduce a layer of abstraction so that clients don't have to
manipulate such mappings directly.

The main reason for this scheme is that while the validity bound itself changes frequently (approximately every
``ADVANCEMENT_QUANTUM`` timestamps, which is ``5,000,000`` at time of writing), the value itself changes much less
often. For example, with transactions2, in the absence of rollbacks, the value is expected to change just once in the
service's lifetime, keeping track of the point where reading from transactions2 should begin.
To further support this scheme, the coordination service recognises identity transformations from the current value,
and doesn't write a new value in these cases.

We have not implemented cleanup of the coordination service at this time, as we don't expect the number of values
in a coordination sequence to be large. Furthermore, we don't ever range scan this table.

##### Physical Implementation

The schema above may be implemented in a mostly straightforward way. Note that this table is read and written
non-transactionally, so the timestamp column is unused and always zero.

Recall that for dynamic columns in Cassandra, ``key`` corresponds to the row, ``column1`` to dynamic column keys,
``column2`` to timestamps and ``value`` to dynamic column values.

|  key | column1 | column2 |                                                                  value |
|-----:|--------:|--------:|-----------------------------------------------------------------------:|
| 0x6d |    0x00 |      -1 | 0x7b2273657175656e6365223a31333030372c22626f756e64223a353031333030377d |
| 0x6d |  0xa711 |      -1 |                                     0x7b2276657...3334227d (216 bytes) |
| 0x6d |  0xb2cf |      -1 |                                     0x7b2276657...3930227d (380 bytes) |

The sequence IDs are encoded as a ``STRING``. The column IDs are ``VAR_LONG`` encoded.

A consequence of this in Cassandra KVS is that all entries for a single coordination sequence will be stored in the
same replication group. However, this is expected to be acceptable as we don't range scan this table, and the number
of values to be written is small.

#### Determining Transaction Schema Versions

We still need to retain the ability to read from the old transactions table, when checking if/when values written
before the transactions2 migration occurred. Thus, we replace the old ``TransactionService`` (that only reads/writes
from transactions1) with one that delegates between multiple TransactionServices (concretely, just V1 and V2 at this
time) depending on the specific timestamps involved in calls.

The mechanism for determining which ``TransactionService`` to use goes through the aforementioned
``CoordinationService``. Nodes agree on a ``TimestampPartitioningMap``, a mapping of timestamps to transaction schema
versions, using the coordination service; this map is always the current value of the coordination store.
This is a range map, as we want to support rollbacks easily. The map also is guaranteed to span the
ranges ``[1, +∞)`` and be connected; note that this does not mean future behaviour is fixed, since in practice this map
is read together with a validity bound. Thus, even though the map will contain as a key some range ``[C, +∞)`` for a
constant ``C``, it should be valid up to some point ``V > C`` - and behaviour at timestamps after ``V`` may subsequently
be changed.

Note that the ranges are based on the start timestamp; since we accessed the original ``TransactionService`` via the
start timestamp, we will use the start timestamp against the range map to find out the behaviour at that point.
To give a concrete example, if our range map is ``{[1, 5000) = 1, [5000, +∞) = 2}`` and we want to commit a transaction
that started at timestamp 4990 and finished at 5010, it is still stored under schema version 1 (even if other
transactions that started later may have already written to schema version 2 - if they started at 5000 and finished at
5001, for instance).

We attempt to read the latest version of the range map, and if our timestamp falls within the validity bound, we
retrieve the version. If it does not, we submit an identity transformation, which extends the validity bound of the
current value. We then read the value again, and keep trying until we know what version to use. If we read a version
we don't support yet, we throw (presumably, this is part of a rolling upgrade and should resolve itself soon).

#### Changing Transaction Schema Versions

AtlasDB clients continuously run a background thread that polls the runtime configuration. If that sets the
target schema version to a specific version (which at time of writing can be 1 or 2), a transform will be run
setting the transactions schema version going forward to that version.

This is optional; if not configured, we will not attempt to run any transforms so the transaction schema version will
remain what it is. If this was never configured at all, we default to using transactions1, to be consistent with
previous behaviour.

### Live Migrations

Transactions2 was written with some of the heaviest users of AtlasDB in mind. These are core Palantir services where
shutdown upgrades are costly or even verboten, and we thus implemented a mechanism for performing upgrades without
downtime. The key problem here involves reasoning about service nodes running different binary versions that
may be running different AtlasDB versions, that are concurrently operating on an AtlasDB deployment.

#### Legacy Versions

AtlasDB is deployed as a library in client services. Versions of AtlasDB may be partitioned based on their support
for Transactions2 into the following categories:

- **(A1) I always use transactions1.** These versions don't even know that the coordination service exists, and they
  will read and write from transactions1 regardless of what other nodes are doing.
- **(A2) I know about the coordination service, but not about transactions2.** These versions of AtlasDB will check
  the coordination service, but they don't know how to read from/write to transactions2, and so they will throw if
  they need to write to transactions2, or if they need to read the commit timestamp of a value written to
  transactions2.
- **(A3) I know about the coordination service and about transactions2.** This is fairly self-explanatory.

Client services come with a version of AtlasDB, and at some point they typically enable transactions2 by default.
We can thus similarly partition them:

- C1 and C2 are defined as client service versions using versions of AtlasDB that would be classified as A1 and A2
  respectively.
- C3 refers to client service versions that use an A3 version of AtlasDB, but don't enable transactions2.
- C4 refers to client service versions that use an A3 version of AtlasDB and enable transactions2. (Transactions2
  cannot be enabled with A1 or A2 versions of AtlasDB, as it doesn't exist in those versions!)

Now, we can consider how multiple client service versions interact:

- Any sets of versions that don't include C4s are safely interoperable, because in these versions transactions2 is never
  installed anyway, so we always use transactions1. Even C1s and C3s can safely interoperate, since although C1s never
  read the coordination table, their effective assumption that it contains everything mapping to 1 is correct.
- C1s and C4s running concurrently can lead to **SEVERE DATA CORRUPTION**, since the C4s might install transactions2
  and read/write from transactions2, while the C1s will continue to read/write from transactions1. This means that
  values committed by C1s once transactions2 became active will show as uncommitted to C4s, and vice versa.
- C2s and C4s running concurrently is safe, but not advised, as once transactions2 takes effect, C2 nodes will not be
  able to commit any write transactions. Furthermore, read-only transactions may also be affected, as C2 nodes will also
  throw if they encounter a version that was written with a start timestamp that would need to be written to
  transactions2.
- C3s and C4s can run concurrently, as they both understand how to read/write transactions2.

#### Implementing Safe Upgrades

To do rolling or blue/green upgrades from C1s or C2s to C4s, we thus recommend a checkpointing process that ensures that
C1s (and C2s) never run concurrently with C4s.

1. Upgrade the version of AtlasDB to an A3 version.
2. Release your product (this is a C3), making this a *checkpoint release* - that is, all upgrades to subsequent
   versions must go through this version.
3. Enable transactions2, and release your product (this is a C4).

## Consequences

### Write Performance

### Read Performance

### Data Compression

### Backup and Restore

### Cassandra Dependencies

## Alternatives Considered

### Use TimeLock / other Paxos mechanism for transactions

### The Row-Tickets Algorithm

## Future Work

### DbKVS and Transactions2
