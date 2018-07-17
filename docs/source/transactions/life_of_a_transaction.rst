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
   This involves re-executing the relevant read queries we made and checking that we read the same *values*
   (note that ABA situations where the state of the world changed and changed back are permitted).
7. Verify that locks are still valid.
8. Verify user-specified pre-commit conditions (if applicable).
9. Atomically putUnlessExists into the transactions table.

After step 9 successfully executes, the transaction is considered committed.

The ordering of these steps is important:

1. Checking TODO TODO
2. If writes are made to the targeted sweep queue before we take out locks, TODO TODO
3. If we write to the database before writing to the targeted sweep queue, we may write values to the database and
   then crash. We then have no knowledge of these uncommitted values, meaning that we'll permanently have cruft
   lying around. It's possible that Background Sweep can be used to clear any such values, but that may take a very
   long time.
4. If we get the commit timestamp before we write key-value pairs to the database, another transaction could start
   after we commit, and may read cells that we have written to. Our own writes may not have been made, but the
   other transaction must see them.
5. The serializable commit check requires us to know the commit timestamp.
6. For conservatively swept tables, the ordering of this step and step 7 is not critical (consider that our check
   must not pass when it should fail, and once we read a value at the commit timestamp we will always read the same
   value).
   For thorough swept tables, there is an edge case. Suppose we read no data for some key, but someone wrote to it
   in between our start and commit, and someone else deleted the value after our commit. If we pass our lock check, but
   then lose our locks and have a very long GC, Thorough Sweep might clear all evidence of the conflict, meaning that
   we miss a read-write conflict.
   (This would be safe for conservative sweep because of the deletion sentinel.)
7. This step may be run in parallel with step 8, though it must strictly be run before step 9 as we cannot
   finish our commit if we can't be certain we still have locks.
8. We need to check that the pre-commit conditions still hold before we can finish committing.

Cleanup
=======

Minimising TimeLock RPCs
------------------------
