========
Clusters
========

The Timestamp Service can be run in clustered mode. The main benefit of this is that you get high availability;
timestamps can be issued as long as a majority of nodes in a cluster are available and can communicate. It may
be worth noting that there is a performance tradeoff involved here, as distributed consensus requires us to communicate
with the other servers in the cluster quite frequently (and thus becomes more costly as more servers are added).

The cluster will agree on a leader using leader election. Thereafter, the leader will be the only node that actually
issues timestamps (other nodes will return a 503). The leader also writes an upper bound for the timestamps that
it has given out to a persistent store; in practice, we write this to a timestamp table in the key-value store.

Note that the leader still needs to talk to a quorum of nodes before entertaining a timestamp request, to check
that it is still the leader. This was a deliberate design decision to maintain safety in the face of clock drift.
However, this only requires one round trip to a quorum of nodes, as opposed to two for Paxos.

Safety
======

Our leader election allows us to guarantee that there will only be one leader at any time. Since only this leader
is allowed to give out timestamps, and we require that it writes an upper bound *before* it is allowed to issue any
timestamps that would take it past the previous upper bound, this is safe. Should the current leader fail mid-way and
then recover, the cluster will have elected a new leader which would have seen the current leader's upper bound.

Liveness
========

We rely on the resilience of our leader election algorithm here. The guarantees our implementation of Paxos gives
include that as long as a majority quorum (strictly more than half) of the nodes are able to communicate with each
other, the protocol can make progress (and in that way decide on a leader).
