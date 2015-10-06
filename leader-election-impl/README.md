This project contains a paxos implementation used for leadership election.


It is used by wrapping a `Supplier` with an impl in an `AwaitingLeadershipProxy`.
This proxy throws a `NotCurrentLeaderException` in the case this process isn't the leader.
It blocks on the LeaderElectionService until it becomes the leader then does
a `get()` from the `Supplier` and starts letting requests through to the impl.


For every call that is forwarded to the impl, a call to `isStillLeading` is made.
This is required because it ensures a happens after relationship between the
request and this node leading.

