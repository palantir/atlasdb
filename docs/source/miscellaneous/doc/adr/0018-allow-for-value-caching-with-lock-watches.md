18. Allow for value caching with Lock Watches
*********************************************

Date: 16/12/2022

## Status

Accepted

## Context

### Caching

Caching values is a useful optimisation that systems can employ to reduce request latency, as instead of repeating the
computation to load the value (which can require database reads, remote procedure calls and/or complex logic), a read 
can be performed to return the result from memory.

However, from a correctness perspective caching is only sound as far as we are able to *invalidate* values from our 
cache that may no longer be accurate. This is in the general case difficult in a distributed system, since our cache
needs to be resilient to writes performed by another service node.

### Locking in AtlasDB

Transactional writes in AtlasDB take out TimeLock locks before anything is persisted to the KVS, and unlock the locks 
when the transaction completes -- either successfully, by committing; or unsuccessfully. The exact type of lock 
guarding a cell in this way depends on the table’s conflict handler.  Unless a table has no conflict handling 
(IGNORE_ALL), the locks taken out will be row level locks or cell level locks.

As a consequence, if we can guarantee that no locks guarding a cell `C` were taken out since the last time the cell was
read, we can use the cached value from the last read as no transaction could have written to `C` since then.

The idea behind lock watches is to provide a TimeLock server API for specifying interesting locks to be monitored 
(registering lock watches) and getting the information about the last time they were taken out (since this can be used
to cache values).

#### Construction of Lock Descriptors

By the time a lock descriptor reaches TimeLock, it is generally a binary string. The way this is generated varies
depending on whether the locks are row level or cell level.

Row level lock descriptors are of the form `tableName || 0 || rowName` and cell level descriptors of the form 
`tableName || 0 || rowName || 0 || columnName`, where the `||` operator denotes concatenation and `0` is a
zero byte. Table names should not contain the zero byte, but there are no such restrictions on row names (e.g.
these can have type `BLOB`), so while a row level lock descriptor is unique, a cell level descriptor is not. Also,
given a lock descriptor it may not always be possible to determine what type of descriptor it is.
For example, the lock descriptor `0x61006200630064` could be any of:

- a row lock descriptor for row `b[null]c[null]d` for the table `a`,
- a cell lock descriptor for row `b` and column `c[null]d` for the table `a`, and
- a cell lock descriptor for row `b[null]c` and column `d` for the table `a`.

## Decision

### Criteria

A good solution to this problem should demonstrate the following characteristics:

- *Correctness*: Lock watches must accurately report all lock and unlock events for lock descriptors that are being
  watched, under all circumstances.
- *Minimal noise*: Lock watches may report lock and unlock events for lock descriptors that are not being watched, but
  this should be kept to a minimum.
- *Independence to wall-clock time*: AtlasDB operates under the assumption that wall-clock time is not to be relied on,
  and this should not be changed by Lock Watches.
- *No excessive performance overhead on critical path operations*: Lock refreshes, transaction starts or transaction 
  commits should not exhibit large performance regressions. (We do not expect to encounter performance regressions on 
  locks and unlocks, but we don't value that as much as these are generally not critical path operations.)

### Implementation: TimeLock Server

#### Registering Interest

```yaml
  ConjureLockWatchingService:
    name: Lock Watching service
    default-auth: header
    package: com.palantir.atlasdb.timelock.lock.watch
    base-path: /lw
    endpoints:
      startWatching:
        http: POST /sw/{namespace}
        args:
          namespace:
            type: string
            safety: safe
          request: LockWatchRequest
```

A `LockWatchRequest` wraps a set of `LockWatchReference`s, which indicate to TimeLock what kinds of locks this
client wants to watch. Currently, only two types are supported: *full table* lock watches and *exact row match* lock
watches; the former is generally exposed to users while the latter is currently not.

We currently only expose full table lock watches to users because these are in many cases sufficient, and are also less
prone to error; for various reasons, lock watches work effectively for cells that are not updated frequently, but are
extremely sensitive to an accidental inclusion of a cell that is, and exposing a more complex and expressive API would
allow users to shoot themselves in the foot easily: for example, if a user was to enable lock watches on a row prefix 
`foo` without realising that a specific row with this prefix, say `food`, was actually updated very frequently, this
would cause _all_ Lock Watch caching to become much less effective.

#### Lock Event Log

The TimeLock leader maintains a `LockEventLog` for each namespace on the server side. The purpose of this event log is
to be able to answer user queries on what the state of the locks that are watched might be.

```java
@Value.Immutable
public interface LockWatchVersion {
  UUID id();
  long version();
}

public interface LockEventLog {
  LockWatchStateUpdate getLogDiff(Optional<LockWatchVersion> fromVersion);
}
```

A `LockWatchStateUpdate` is a union type that is either a `Success` indicating events that happened since the last known
version up to some fixed sequence number, or a `Snapshot` indicating the state of the world (as far as watched locks are
concerned) and its sequence number.

The implementation internally tracks a bit more state to facilitate updates, including:

- a UUID which identifies this lock event log,
- a ring buffer tracking the 1,000 most recent `LockEvent`s,
- a long indicating what the sequence number of the next event should be, and
- a reference to the `HeldLocksCollection`, mainly for taking snapshots.

The UUID is generated on the creation of the event log, and is used to ensure that clients know that the state of the
log needs to be completely invalidated after each leader election (in particular, even in the edge case where a node
loses and regains leadership; since all lock state is cleared).

As lock and unlock are called, TimeLock will evaluate the relevant lock descriptor against the set of registered lock 
watches. If there is a match, timelock will enqueue a `LockEvent` indicating that a given descriptor was locked or 
unlocked into the ring buffer before returning. This is done synchronously.

User queries to `getLogDiff` must then consider the provided `fromVersion` argument: if this is 
- absent, or 
- from a different log, as determined by the UUID not matching, or
- too far (more than 1,000 events) behind

we use the `HeldLocksCollection` to give the user a snapshot of the world (that is, the version, the set of
active lock watches, and the set of lock descriptors corresponding to locks that had already been taken out that are
also matching the lock watches). Otherwise, the relevant part of the ring buffer is served to users.

#### Creation Events

We implement registration of a new lock watch as being another event in the log. Recall our definitions of correctness
and minimal noise: it is costly to synchronise all matching lock and unlock requests when a lock watch is being
created, and we don't make or need those guarantees. It suffices to first add the lock watch to the set of tracked lock
watches (so that new lock/unlock requests evaluate themselves against it), then look at currently open locks and add
lock/unlock events so clients are aware of the open locks, *then* add a LockWatchCreated event to the log.

We haven't implemented deregistration, though this is conceptually simpler: the client needs to flush everything that it
has cached, but that's about it.

#### Starting Transactions

TimeLock provides an API through which Atlas clients start transactions. The Atlas client will track the last version
it knows about and provides this as part of the request body. That said, starting an AtlasDB transaction requires
TimeLock to do multiple things, and we need to be careful about ordering. Starting a batch of transactions requires

- Locking the immutable timestamp.
- Getting fresh timestamps, which will be the start timestamp of the transactions.
- Providing the correct `LockWatchStateUpdate` to the client, based on a `lastKnownVersion` they provide in the 
  `StartTransactionRequest`.

The update we return need not be entirely up to date, but we must ensure that information about all lock requests 
completed before this request started has already been included in the log returned to the user. So we take out the
start timestamps before looking at the lock log - there could be more events that occurred in the interim that we
didn't see, but that's allowed. Consider that any lock event happening in parallel must relate to a transaction that
has already started and run its transaction task, but by definition of the Atlas transaction protocol has not got its
commit timestamp yet. Since we had already taken out our start timestamp, we know that our start timestamp must be less
than the other transaction's commit timestamp, and so we cannot see their write so it doesn't matter to us.

### Implementation: AtlasDB

#### Requirements

We need AtlasDB to track where we are in terms of events we know about (so our `lastKnownVersion`). We also
need to ensure that transactions are able to read transactionally from the cache; in particular, if an older transaction
runs for some time, and we have subsequent transactions which write and read a watched cell, the old transaction should
not have to invalidate values it reads from that cell.

AtlasDB also needs to expose schema configuration points to allow users to specify that they want caching on some of
their tables, and based on that also needs to actually register the watches with TimeLock.

#### Starting a Transaction
TODO

## Deployment and Testing
TODO

## Consequences
TODO
