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

- *Correctness*: Lock watches must be accurate with respect to actual lock and unlock events under all circumstances.
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

#### Lock Event Log

The TimeLock leader maintains a `LockEventLog` on the server side; this event log wraps a ring buffer tracking up to
1000 `LockEvent`s, and this limit is not currently configurable. 

#### Lock and Unlock Workflows

When lock or unlock are called, TimeLock will evaluate the lock descriptor against the set of registered lock watches.
If there is a match, timelock will enqueue an event encoding the fact that a lock or unlock happened in a ring buffer;
this buffer

It is worth noting that in the general case, because of how Atlas generates lock descriptors from its schema, it is
not always possible to determine definitively whether a row


### Implementation: AtlasDB

#### R

## Deployment and Testing
TODO

## Consequences
TODO
