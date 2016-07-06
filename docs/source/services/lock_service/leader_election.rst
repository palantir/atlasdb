========================
Palantir Leader Election
========================

Palantir Leader Election is an implementation of paxos.

``leader-election-api`` and ``leader-election-impl`` projects contains the relevant java classes.
A good starting point to read how paxos is implemented is AwaitingLeadershipProxy.java

Although we will not discuss paxos here, we will discuss how it is used by the lock service.
Paxos is a distributed consensus algorithm, which allows our services to agree on values.
An example would be agreeing on a lock service to be the leader lock service, which would then serve requests.

Lock clients invoke methods on the lock leader.
If a lock client invokes a method on a non-leader, the lock service will indicate to the client that it does not believe it is the leader by returning a NotCurrentLeader exception.


First guarantee of the lock service
-----------------------------------

Our implementation of paxos guarantees that we do not have two lock services that are acting as leader at the same time.
If the lock client invokes a method on a node which believes it is the leader, it will call a ``quorum`` to ensure it is still the leader prior to method invocation.
A ``quorum`` is a meeting of nodes to agree on a single value.
If the ``quorum`` resolves to indicate that the invocation target is still the leader, then the method will be invoked.
If the ``quorum`` resolves to indicate that the invocation target is not the leader, the method invocation will fail.

.. note::
   This behavior is clear from AwaitingLeadershipProxy::invoke()


Second guarantee of the lock service
------------------------------------

Although it is unrelated to paxos, the other property that our implementation of paxos guarantees for the lock service is that if the lock leader fails, the other lock services will learn this information and elect a new leader, assuming that greater than N / 2 nodes are still active. The node which has failed will attempt to restart. Thus, if you send SIGTERM to the JVM running the lock service with leader election enabled, the service should restart.

Although we have not discussed how paxos ensures these two properties (and, in fact, the second property is not related to paxos) we claim that these two properties enable a highly available solution for the lock service.
