.. _timelock-paxos:

=====
Paxos
=====

Paxos is an algorithm for *distributed consensus* - that is, getting servers to agree on a value that some server
*proposes*, in the presence of communication and server failures. Its guarantees include:

#. Safety: All servers will agree on the same value.
#. Nontriviality: The value must have been *proposed* by at least one of the servers.
#. Liveness: If a majority of servers is up and able to communicate, then the protocol can make progress.

If the Timelock Server is configured to use Paxos, it will actually use separate instances of Paxos to agree on the
current cluster leader (which is the only node allowed to give out timestamps and locks), as well as the timestamp
bound for each client supported by the Timelock server. This could be multiplexed, but we decided to separate them to
reduce contention as well as allow future optimisations involving load balancing.

Leader Election
===============

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

This can be (and is in AtlasDB) mitigated by having a random backoff between proposals. However, leader election is
often preferable for performance reasons; we use a round of Paxos which is symmetric to perform this election. The
leader is the only node allowed to propose timestamp bounds for each clients.

We are considering extending this notion of leader election to agreeing on a balanced scheme of work across nodes
in the future, to relieve load on the leader.
