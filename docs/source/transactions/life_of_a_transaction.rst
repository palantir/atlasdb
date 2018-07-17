.. _life-of-a-transaction:

=====================
Life of a Transaction
=====================

This document seeks to outline the efficiency of our implementation of the AtlasDB protocol for write transactions.
We do this by establishing a strict ordering on the steps of the protocol.

Stages of a Transaction
-----------------------

A user can run a ``TransactionTask`` through a ``TransactionManager``. Running the user's task in a transaction can be
seen as a four step process.

1. Start an AtlasDB transaction.
2. Execute the user's transaction task (which has access to the transaction we started).
3. Commit the transaction.
4. Cleanup resources used by the transaction.

Startup
=======

When a write transaction is created, we first need to perform some setup work. This involves locking the immutable
timestamp and then getting a fresh timestamp. The ordering here is significant - if we get a fresh timestamp and then
lock the immutable timestamp, our transaction's timestamp is smaller than the immutable timestamp. This is dangerous
because the immutable timestamp is used by Sweep to determine which stale versions of data can be removed.
If there is a cell that has a commit timestamp in between our start timestamp and the immutable timestamp we locked,
then we may or may not read it.

However, the operations can be batched as a single request to TimeLock - this improves performance particularly if one
is using external TimeLock services.

User Task
=========

A ``TransactionTask`` is an arbitrary ``Function<Transaction, T>`` that can throw a pre-defined checked exception ``E``.
In practice, users will use this task to read from the database and buffer up writes (though these are only executed
at commit time). The task has access to the lock and timestamp services and may have side effects.

In terms of ordering, the start timestamp must precede the task since we need to know what timestamp to read values at.
The transaction task must precede committing, as that opens with constraint checking which needs to know what writes
were performed.

Commit
======

Committing a write transaction is a multi-stage process:

1. Check constraints if applicable.
2. Lock rows or cells (depending on the table conflict handler) for a given table.
3. Persist information about writes to the targeted sweep queue.
4. Persist key-value pairs that were written to the database.
5. Get the commit timestamp.
6. For serializable transactions, check that the state of the world at commit time is same as that at start time.
7. Verify that locks are still valid.
8. Verify user-specified pre-commit conditions (if applicable).
9. Atomically putUnlessExists into the transactions table.

After step 9 successfully executes, the transaction is considered committed.

Cleanup
=======

Minimising TimeLock RPCs
------------------------
