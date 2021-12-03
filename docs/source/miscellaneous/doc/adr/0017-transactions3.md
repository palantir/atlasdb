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

- The transactions table should have repeatable reads: after a non-null value (a commit timestamp, or ABORTED) has been 
  read from the table for a given timestamp, all future reads should return the same value.
- In the average case, reading a cell in the transactions table should require 1 RPC from AtlasDB to the coordinating
  Cassandra node, and just 1 RPC between each pair of Cassandra nodes in the relevant replication group.
- Writing a cell in the transactions table should require as few RPCs as possible, bearing in mind the other 
  constraints.

### Theory

#### PUE Tables

#### Transactions3 Service

#### The PUT problem

### Implementation

#### Consensus forgetting stores

#### PutUnlessExistsTable

## Alternatives Considered

## Consequences