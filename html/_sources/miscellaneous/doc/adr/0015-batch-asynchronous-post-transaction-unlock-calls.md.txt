15. Batch asynchronous post-transaction unlock calls
****************************************************

Date: 28/06/2018

## Status

Accepted

Functionally, this decision remains accepted.
The implementation has been superseded; we now use Disruptor Autobatchers.

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
are arbitrary and we thus do not focus on optimising these.

After a transaction commits, it needs to release the locks it acquired as part of the transaction protocol. Releasing
the immutable timestamp lock helps AtlasDB keep as few stale versions of data around as possible (which factors into
the performance of certain read query patterns); releasing row and cell locks allows other transactions that need to
update these to proceed.

Currently, these locks are released synchronously and separately after a transaction commits. Thus, there is an
overhead of two lock service calls between a transaction successfully committing and control being returned to 
the user.

Correctness of the transaction protocol is not compromised even if these locks are not released (though an effort
should be made to release them for performance reasons). Consider that it is permissible for an AtlasDB client to
crash after performing `putUnlessExists` into the transactions table, in which case the transaction is considered
committed.

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

### tryUnlock() API

`TimelockService` now exposes a `tryUnlock()` API, which functions much like a regular `unlock()` except that the user
does not need to wait for the operation to complete. This API is only exposed in Java (not over HTTP).

This is implemented as a new default method on the `TimelockService` that delegates to `unlock()`; usefully, remote
Feign proxies calling `tryUnlock()` will make an RPC for standard `unlock()`. This also gives us backwards
compatiblity; a new AtlasDB/TimeLock client can talk to an old TimeLock server that has no knowledge of this endpoint.

### Concurrency Model

It is essential that adding an element to the set of outstanding tokens is efficient; yet, we also need to ensure that 
no token is left behind (at least indefinitely). We thus guard the concurrent set by a (Java) lock that permits both 
exclusive and shared modes of access.

Transactions that enqueue lock tokens to be unlocked perform the following steps:

1. Acquire the set lock in shared mode.
2. Read a reference to the set of tokens to be unlocked.
3. Add lock tokens to the set of tokens to be unlocked.
4. Release the set lock.
5. If no task is scheduled, then schedule a task by setting a 'task scheduled' boolean flag. 
   This uses compare-and-set, so only one task will be scheduled while no task is running.

For this to be safe, the set used must be a concurrent set. 

The task that unlocks tokens in the set performs the following steps:

1. Un-set the task scheduled flag.
2. Acquire the set lock in exclusive mode.
3. Read a reference to the set of tokens to be unlocked.
4. Write the set reference to point to a new set. 
5. Release the set lock.
6. Unlock all tokens in the set read in step 3.

This model is trivially _safe_, in that no token that wasn't enqueued can ever be unlocked, since all tokens that can
ever become unlocked must have been added in step 3 of enqueueing, and unlocking a lock token is idempotent modulo
a UUID clash.

More interestingly, we can guarantee _liveness_ - every token that was enqueued will be unlocked in the absence of
thread death. If an enqueue has a successful compare-and-set in step 5, then the token must be in the set
(and is visible, because we synchronize on the set lock). If an enqueue does _not_ have a successful compare-and-set,
then some thread must already be scheduled to perform the unlock, and once it does the token must be in the relevant
set (and again must be visible, because we synchronize on the set lock).

To avoid issues with starving unlocks, we use a fair lock scheme. Once the unlocking thread attempts to acquire the set
lock, enqueues that are still running may finish, but fresh calls to enqueue will only be able to acquire the set lock
after the unlocking thread has acquired and released it. This may have lower throughput than an unfair lock,
but we deemed it necessary as 'readers' (committing transactions) far exceed 'writers' (the unlocking thread) -
otherwise, the unlocking thread might be starved of the lock.

### TimeLock Failures

In some embodiments, the lock service is provided by a remote TimeLock server that may fail requests. There is retry 
logic at the transport layer underneath us.

Previously, running a transaction task would throw an exception if unlocking row locks or the immutable timestamp
failed; we now allow user code to proceed and only emit diagnostic logs indicating that the unlock operation failed.
This is a safe change, as throwing would not make the locks become available again, and user code cannot safely
assume that locks used by a transaction are free after it commits (since another thread may well have acquired them). 

In practice, locks will be released after a timeout if they are not refreshed by a client. This means that not
retrying unlocks is safe, as long as we do not continue to attempt to refresh the lock. AtlasDB clients automatically
refresh locks they acquire; we ensure that a token being unlocked is synchronously removed from the set of locks
to refresh *before* it is put on the unlock queue.

## Consequences

### Improvements

- Transactions no longer need to wait for their immutable timestamp lock and row/cell write locks to be unlocked
  before returning. We anticipate this will make transactions faster (from a user-code perspective) by two
  round-trips to the lock service. In many deployments, the lock service is a remote TimeLock server, meaning that
  we save two network calls.
- Transactions can now succeed even if there were problems when unlocking locks after the transaction committed.
- Load on the TimeLock server will be reduced, as fewer unlock calls need to be made (though each call is larger and
  the total number of tokens is still the same, constant overheads e.g. of the transport layer will be reduced). 

### Drawbacks

- Transactions may hold row locks for an average of half a round-trip time between an AtlasDB client and TimeLock
  longer. This is partially mitigated by TimeLock load being lower (feeding in to performance improvement across
  multiple calls in the transaction protocol).
- One background thread is allocated to unlock the locks where needed, which may incur some overhead in deployments
  with small hardware.
