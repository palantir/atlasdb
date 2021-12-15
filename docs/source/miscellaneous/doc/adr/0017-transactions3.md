17. Use a two-stage encoding for transactions table values (_transactions3)
***************************************************************************

Date: 03/12/2021

## Status

Technical decision has been accepted.
This architectural decision record is still a work in progress.

## Context

### Cassandra Timestamps

Values in Cassandra are written together with a timestamp. Note that this differs from the `column2` column of the
AtlasDB schema. For CAS operations, Cassandra typically uses wall-clock time on the server after the Paxos has completed
as the timestamp for the writes to be applied.
If multiple nodes in a quorum disagree when reading a value from a key, *latest write wins*: we pick the value with
the highest write timestamp.

However, before returning, we perform a *blocking read repair*: we update dissenting nodes in the quorum with
information on the latest write. This update happens before we return the value to the user (which is important for the
correctness of our protocol).

### Cassandra Lightweight Transaction Weaknesses

There is a known consistency issue with Cassandra’s check-and-set (CAS) operation. When a CAS operation occurs, the 
following happens on Cassandra (this is implemented in `StorageProxy#cas`):

1. A round of Paxos (both phases) is run, and the Paxos state is stored in the paxos system table.
2. If the round of Paxos was successful,
    1. the proposed CAS is evaluated (the value is read, and fails if it does not match the expected value).
    2. If successful, requests are made seeking the Paxos round to be committed on all nodes (this applies the mutation 
       of the proposed CAS too).
    3. The Paxos round is then committed on that node (as part of response handlers of (b)), plus other nodes.
    4. We then block until we get read consistency many nodes (QUORUM or EACH_QUORUM for us).

It is possible that we may fail in 2(ii) or 2(iii). However, some nodes may have performed the mutation recommended 
from a successful Paxos round and not others, with the user receiving a failure response (or no response at all).

This seems fine as long as we ensure that any relevant Paxos rounds for values we read have resolved; in practice this 
is accomplished with the SERIAL consistency level. However, reading at SERIAL is costly. Furthermore, there is another
problem: Cassandra uses a TTL for its system keyspace of 3 hours (and this includes the Paxos system table, in which
Cassandra keeps track of its Paxos promises). Thus, a "failed" CAS where the mutation was actually applied on some nodes
followed by an extended failure or network partition could result in inconsistent reads. In particular, supposing a 
three-node cluster `[A, B, C]`, on a successful CAS of a value from `V1` to `V2` where A was the coordinator and only 
successfully applied the mutation to itself, a quorum read of this cell could read `V1` (if the quorum chosen was B, C) 
*or* `V2` (if the quorum included A: this value is newer, assuming no weird write-time shenanigans).

This is a problem, because in AtlasDB we rely on putUnlessExists for the transactions table (which uses Cassandra's 
CAS). In practice, this means that a transaction at a given timestamp could be read both as being committed and being 
uncommitted. Furthermore, it is possible that a quorum of nodes that believe the transaction is uncommitted decide that 
the transaction should be rolled back (as they would accept a putUnlessExists of `-1` to that cell, which is interpreted
in Atlas as meaning the transaction was aborted).

### Range Movements

Cassandra determines which nodes participate in the Paxos based on the replication group of the relevant key. This, 
however, may change at runtime (e.g., if there are changes in cluster topology), which can naturally lead to disjoint 
quorums. Cassandra is not currently resistant to this case, and this would require substantial changes to the Paxos 
algorithm. We're thus treating dealing with these cases as outside the scope of this workstream.

## Decision

### Criteria

A good solution to this problem should demonstrate the following characteristics:

- *Repeatable reads*: After a non-null value (a commit timestamp, or ABORTED) has been read from the table for a given 
  timestamp, all future reads should return the same value. This is our primary issue with the current system.
- *Eventual read efficiency*: Eventually, reading a cell in the transactions table should require just 1 RPC from
  AtlasDB to the coordinating Cassandra node, and within Cassandra just 1 RPC between each pair of Cassandra nodes in
  the relevant replication group.
- *Write efficiency*: Writing a cell in the transactions table should require as few RPCs as possible, bearing in mind 
  the other constraints. Optimising for this property is less important than the other two.

### Theory

#### PUE Tables
We want to define a *put-unless-exists table*, which supports only two operations: putUnlessExists and get. While this
interface may look superfluous in that the `KeyValueService` interface already has very similar endpoints, it *must*
support repeatable reads.

```java
public interface PutUnlessExistsTable<K, V> {
    void putUnlessExistsMultiple(Map<K, V> keyValues) throws KeyAlreadyExistsException;

    ListenableFuture<Map<K, V>> get(Iterable<K> keys);
}
```

As a primitive, we define a *put-unless-exists value* as a pair of a value and a state enumeration. This state indicates
whether the value is *pending* or *committed*.

```java
public interface PutUnlessExistsValue<V> {
    V value();

    PutUnlessExistsState state();
}

public enum PutUnlessExistsState {
    STAGING,
    COMMITTED;
}
```

To perform a write (putUnlessExists) of a value `V`, we use the following protocol (noting that these are KVS-level 
operations).

1. PUE((V, STAGING))
2. PUT((V, COMMITTED))

To perform a read, we use the following protocol.

1. Read the current value from the database.
2. If it is (V, COMMITTED), return V.
3. If it is not present, return null.
4. If it is (V, STAGING),
   1. perform a CAS((V, STAGING), (V, COMMITTED)),
   2. then return V.

Notice that this protocol meets the criteria outlined above. We have eventual read efficiency, as in the steady state
most values are going to be COMMITTED, and the read protocol for a COMMITTED value just involves a single read at QUORUM
consistency. 

The argument for read consistency is a bit subtler.

<Insert details of Grgur's proof work here>

#### Transactions3 Service

We can use a put-unless-exists table to provide an implementation of the transactions3 service. The functionality
required for a transaction service essentially matches that of a `PutUnlessExistsTable<Long, Long>`.

We aren't actively aware of any improvements to the tickets encoding strategy used in `_transactions2` for cells,
so we can just use that strategy when figuring out where to put the individual values.

#### The PUT problem

One challenge we face when working with Cassandra is that we need to choose suitable timestamps for our writes, and
while Cassandra seems to be willing to tolerate some inconsistency because of clock drift, this is not allowed in
AtlasDB. 

For a lightweight transaction, Cassandra will apply writes at the server-side timestamp of the coordinator, though this
is combined with a bit of conditional logic that ensures that a write that takes place in terms of Paxos after another
one will have a writetime that is greater. Assuming that Paxos was consistent, this is fine. However, our write protocol
performs a hard PUT in its second step, and we need to choose a timestamp for this.

We settled on choosing a large constant (but not maximal!) timestamp for the hard PUT. This should exceed all reasonable
timestamps we'd encounter in practice, but still allow for deletion if a user needs to perform a clean transactions
range workflow as part of backup and restore.

Plausible alternatives here could have included exposing a new endpoint on Cassandra's thrift API that copies the
"get a timestamp later than what's there" logic and allows users to hit it directly (but this would require a new 
endpoint and thus a new dependency on our Cassandra fork, making rollout messy), or having the second step of the write
protocol be CAS((V, STAGING), (V, COMMITTED)) - but that requires another round of Paxos on writes.

### Implementation

We define the `PutUnlessExistsTable<K, V>` interface described above and switch our transaction services to use it
rather than a key-value service directly. This is useful because it allows us to avoid
having to change transaction service logic very much; deployments using key-value-services which don't actually have
this problem (e.g., Oracle and Postgres) simply instantiate a much simpler implementation that passes through the
relevant calls to the key-value service. As part of doing this, we needed to rework the `checkAndSetCompatibility`
classification of key-value services to separately track failure details and consistency guarantees.

The remainder of this section largely focuses on implications for deployments running Cassandra.

#### Consensus forgetting stores

We introduce a new abstraction, a `ConsensusForgettingStore`. This is a table that is intended to capture the behaviour
of Cassandra that we have identified: reads are not repeatable, but there still remain some consistency guarantees which
we will define.

The main operations on a `ConsensusForgettingStore` are those required to support the protocol outlined above:

```java
public interface ConsensusForgettingStore {
    /**
     * An atomic put unless exists operation. If this method throws an exception, there are no consistency guarantees:
     *   1. A subsequent PUE may succeed or fail non-deterministically
     *   2. A subsequent get may return Optional.of(value), Optional.empty(), or even Optional.of(other_value) if
     *   another PUE has failed in the past non-deterministically
     */
    void putUnlessExists(Cell cell, byte[] value) throws KeyAlreadyExistsException;

    /**
     * An atomic operation that verifies the value for a cell. If successful, until a
     * {@link ConsensusForgettingStore#put(Cell, byte[])} is called subsequent gets are guaranteed to return
     * Optional.of(value), and subsequent PUE is guaranteed to throw a KeyAlreadyExistsException.
     */
    void checkAndTouch(Cell cell, byte[] value) throws CheckAndSetException;

    ListenableFuture<Optional<byte[]>> get(Cell cell);

    /**
     * A put operation that offers no consistency guarantees when an exception is thrown. Multiple puts into the same
     * cell with different values may result in non-repeatable reads.
     */
    void put(Cell cell, byte[] value);
}
```
Note that there are multi-cell versions in the actual implementation that we have omitted in the interest of 
conciseness.

The implementation of *these* methods generally involve passing values through to the KVS, perhaps with a small amount
of wiring. Nonetheless, this interface is useful as it allows us to simulate legitimate failures in this layer
as part of our testing.

#### ResilientCommitTimestampPutUnlessExistsTable

We implement the `PutUnlessExistsTable<K, V>` interface, using a `ConsensusForgettingStore<K, V>` and applying our
protocol discussed in the Theory section.

We also here need to make decisions about how we are going to store our (table-level) keys and values in the key-value
service. This is implemented as `TwoPhaseEncodingStrategy`. As discussed earlier, we re-use the logic from 
`TicketsEncodingStrategy` for encoding a start timestamp (a key) into a cell. This ensures that we still profit from
performance optimisations that were written specifically for _transactions2 (e.g. shared modulus generation on
TimeLock, protections against overall hotspotting).

For the values, we also re-use the `TicketsEncodingStrategy`'s delta-encoded `VAR_LONG`. We simply append one byte to
the end of the value: `{0}` for a `STAGING` value and `{1}` for a `COMMITTED` value. The primary concerns we should have
here would be space and readability. While this is not optimal for space (consider that there are likely to be methods
that achieve better efficiency by adding one bit rather than one byte; also, since the majority of values should be 
`COMMITTED`, we could simply have the zero byte for `STAGING` and not have one for `COMMITTED`), there is probably some
value in the relative simplicity of this approach, as well as its defense against users attempting to read data using
an incorrect encoding strategy.

#### Internal backup services

## Alternatives Considered

- One possible solution could be to perform reads at SERIAL consistency, and override the default system keyspace TTL in 
  Cassandra to be longer. However, we would still need to pick a reasonable bound after which the Paxos logs and other
  contents of the system keyspace could be kept for. Furthermore, reads at SERIAL consistency will not achieve eventual
  read efficiency; these require Cassandra nodes to perform a round of Paxos internally, adding two *more* RPCs between
  the coordinator (which acts as a Paxos proposer) and the other nodes.
- Another possible approach involves a state machine where a user is allowed to propose their own value as part of the
  read protocol (as opposed to being required to commit the staging value that was written). The author thinks this
  could probably work, but it is more difficult to reason about (e.g. one has to be more concerned about the behaviour
  of concurrent proposals, and whether a dueling proposers-style situation is possible).

## Consequences
As transactions3 is rolled out globally to Cassandra deployments, they will no longer be exposed to this correctness
bug. We hope (though don't have strong evidence owing to small sample sizes and extreme difficulty in root causing) 
that this will reduce the incidence of AtlasDB corruption tickets.

Backup and restore workflows, or any use cases that manually manipulate one of the transaction tables will need to be
aware that new serialized forms exist. In particular, the transactions2 table should not be read in isolation without
caution - while values are unambiguous in indicating whether they are written with transactions2 or transactions3,
a user needs to be aware that a STAGING value is not safe to use as though it is final.

It is possible, even likely, that the performance of transactions3 is worse than that of transactions2 (we do strictly
more work, and values that need to be passed to and from the database are strictly larger). However, we don't expect
this to add more than a constant amount of overhead, and this should be very small in the steady state. This isn't a
very fair comparison, in any case, as the transactions2 approach is not correct.
