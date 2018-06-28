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
crash after performing ``putUnlessExists`` into the transactions table.

## Decision

## Consequences

### Improvements

### Drawbacks

Transactions may hold row locks for an average of half