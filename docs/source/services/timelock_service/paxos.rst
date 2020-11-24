.. _timelock-paxos:

=================
Paxos and AtlasDB
=================

The Paxos Algorithm
===================

`Paxos <https://www.microsoft.com/en-us/research/wp-content/uploads/2016/12/paxos-simple-Copy.pdf>`__ is an algorithm
for *distributed consensus* - that is, getting servers to agree on a value that some server *proposes*, in the presence
of communication and server failures. Its guarantees include:

#. Safety: All servers will agree on the same value.
#. Nontriviality: The value must have been *proposed* by at least one of the servers.
#. Liveness: If a majority of servers are up and able to communicate, then the protocol can make progress.

Paxos is a symmetric consensus protocol; technically speaking, each server (which plays all of the proposer, acceptor
and learner roles) behaves in the same way. However, this can potentially lead to a situation known as the
*dueling proposers* problem, where servers behave as follows:

#. Server A proposes a value V1, with term (1, A)
#. It receives a majority of promises not to accept values with terms greater than (1, A)
#. Server B proposes a value V2, with term (1, B)
#. It receives a majority of promises for (1, B)
#. Server A requests servers to accept V1; servers reject
#. Server A proposes V1 with term (2, A)
#. It receives a majority of promises for (2, A)
#. Server B requests servers to accept V2; servers reject

This can be (and is in AtlasDB) mitigated by having a random backoff between proposals.

How we use Paxos
================

The TimeLock servers may use a Paxos based algorithm for two purposes:

#. **Leadership election:** We choose who will be the leader in the TimeLock cluster. The leader is the only server capable
   of responding to lock and timestamp requests. Leader election is often preferable for performance reasons confirming
   you are the leader is more efficient than choosing the value to respond with.
#. **Timestamp bound:** TimeLock needs to store upper bounds on timestamps that the leader is allowed to hand out.
   If configured to use Paxos timestamp persistence, TimeLock chooses this bound via Paxos.

It is worth noting that the Paxos implementations differ slightly for choosing a leader and choosing a timestamp bound.
When choosing a leader, all servers can propose leadership and have the potential to be elected. However, once a leader
is established, only that server is allowed to propose timestamp bounds.
