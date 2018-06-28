# 15. Batch asynchronous post-transaction unlock calls

Date: 28/06/2018

## Status

Accepted

## Context

As part of the AtlasDB transaction protocol, write transactions acquire locks from the lock service. They typically
acquire two types of locks:

- An *immutable timestamp lock*, which AtlasDB uses as an estimate of the oldest running write transaction. The
  state of the database at timestamps less than the lowest active immutable timestamp lock is considered immutable, and
  thus eligible for cleanup by Sweep.
- *Row locks* and *cell locks* (depending on the conflict handler of the tables involved in a write transaction) for
  rows or cells being written to. These locks are used to prevent multiple concurrent transactions from simultaneously
  writing to the same rows and committing.

Transactions may also acquire additional locks as part of AtlasDB's pre-commit condition framework. These conditions
are arbitrary and we thus do not focus on optimising these, btu.

After a transaction commits, it needs to release the locks it acquired as part of the transaction protocol. Releasing
the immutable timestamp lock helps AtlasDB keep as few stale versions of data around as possible (which factors into
the performance of certain read query patterns); releasing row and cell locks allows other transactions that need to
update these to proceed.

Currently, these locks are released synchronously and separately after a transaction commits. Thus, there is an
overhead of two RPCs on the lock service between a transaction successfully committing and control being returned to 
the user.

Correctness of the transaction protocol is not compromised even if these locks are not released (though an effort
should be made to release them for performance reasons); consider that it is permissible for an AtlasDB client to
crash after performing `putUnlessExists` into the transactions table.

## Decision

Instead of releasing the locks synchronously, release them asynchronously so that control is returned to the user very
quickly after transaction commit. However, maintaining relatively low latency between transaction commit and unlock
is important to avoid unnecessarily blocking other writers or sweep.

Two main designs were considered:

1. Maintain a thread pool of `N` consumer threads and a work queue of tokens to be unlocked. Transactions that commit 
   place their lock tokens on this queue; consumers pull tokens off the queue and make unlock requests to the lock
   service.
2. Maintain a concurrent set of tokens that need to be unlocked; transactions that commit place their lock tokens
   in this set, and an executor asynchronously unlocks these tokens.

Solution 1 is simpler than solution 2 in terms of implementation. However, we opted for solution 2 for various reasons.
Firstly, the latency provided by solution 1 is very sensitive to choosing `N` well - choosing too small `N` means that
there will be a noticeable gap between transaction commit and the relevant locks being unlocked. Conversely, choosing 
too large `N` incurs unnecessary overhead. Choosing a value of `N` in general is difficult and would likely require
tuning depending on individual deployment and product read and write patterns, which is unscalable.

Solution 2 also decreases the load placed on the lock service, as fewer unlock requests need to be made.

In our implementation of solution 2, we use a single-threaded executor. This means that on average the additional
latency we incur is about 0.5 RPCs on the lock service (assuming that that makes up a majority of time spent in
unlocking tokens - it is the only network call involved).

### Concurrency Model

We use a read-write lock...

## Consequences

### Improvements

Transactions no longer need to wait ...

### Drawbacks

Transactions may hold row locks for an average of half