.. _life-of-a-transaction:

=====================
Life of a Transaction
=====================

This document seeks to outline the efficiency of our implementation of the AtlasDB protocol.
We do this by establishing a strict ordering on the steps of the protocol.

Stages of a Write Transaction
-----------------------------

A user can run a ``TransactionTask`` through a ``TransactionManager``. Running the user's task in a transaction can be
seen as a four step process.

1. Start an AtlasDB transaction.
2. Execute the user's transaction task within the transaction.
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
3. Check for write/write conflicts.
4. Persist information about writes to the targeted sweep queue.
5. Persist key-value pairs that were written to the database.
6. Get the commit timestamp.
7. For serializable transactions, check that the state of the world at commit time is same as that at start time.
   Please see :ref:`Isolation Levels<isolation-levels>` for more details.
8. Verify that locks are still valid.
9. Verify user-specified pre-commit conditions (if applicable).
10. Atomically putUnlessExists into the transactions table.

After we have successfully written to the transactions table, the transaction is considered committed.

The ordering of these steps is important:

1. Checking constraints must be done before putUnlessExists. These constraints do not depend on the rows being locked,
   so we should do this before acquiring locks (generally we want to minimise critical sections).
2. If we check for write/write conflicts before acquiring locks, another transaction could write a conflicting value
   after we perform the check, but before we acquire locks. This should cause a write-write conflict, but we will miss
   it. Taking locks first means that this is only possible if we've lost our locks before the check - but if that is
   the case then we will not commit as we will fail when we check our locks (step 8).
3. Checking for write/write conflicts must be done before checking that locks are still valid (step 8), as we could
   otherwise lose the lock and have a thorough sweep clear out all evidence of spanning/dominating writes.
   It may be possible to postpone this, though we would be doing unnecessary work.
4. If we write to the database before writing to the targeted sweep queue, we may write values to the database and
   then crash. We then have no knowledge of these uncommitted values, meaning that we'll permanently have cruft
   lying around. It's possible that Background Sweep can be used to clear any such values, but that may take a very
   long time.
5. If we get the commit timestamp before we write key-value pairs to the database, another transaction could start
   after we commit, and may read cells that we have written to. Our own writes may not have been made. The AtlasDB
   protocol is that writes take place at commit time, so our writes must be observable to the other transaction
   if it has a higher start timestamp than our commit timestamp, which is possible here.
6. The serializable commit check requires us to know the commit timestamp.
7. For conservatively swept tables, the ordering of the serializable conflict check and lock check is not critical
   (consider that our check must not pass when it should fail, and once we read a value at the commit timestamp we
   will always read the same value).
   For thoroughly swept tables, there is an edge case. Suppose we read no data for some key, but someone wrote to it
   in between our start and commit, and someone else deleted the value after our commit. If we pass our lock check, but
   then lose our locks and have a very long GC, Thorough Sweep might clear all evidence of the conflict, meaning that
   we miss a read-write conflict. This would be safe for conservative sweep because of the deletion sentinel.
8. The kock check may be run in parallel with pre-commit condition checks, though it must strictly be run before writing
   to the transactions table, as we cannot finish our commit if we can't be certain we still have locks.
9. We need to check that the pre-commit conditions still hold before we can finish committing.

Read-Only Variant
~~~~~~~~~~~~~~~~~

.. note::

    This section looks at write transactions that perform only reads (as opposed to pure read transactions).
    This is motivated by a shift towards thoroughly swept tables which can have better performance characteristics,
    especially for workflows involving row or dynamic column range scans.

Transactions that do not write have a much simpler commit stage:

1. Verify user-specified pre-commit conditions (if applicable).
2. Verify that the immutable timestamp lock is still held.

Notice that these are analogous to the lock and pre-commit condition checks for transactions that write. They
can be run in parallel (though we haven't implemented this as the expected gain is currently not large).

Cleanup
=======

We need to unlock row/cell locks and the immutable timestamp lock. This need not be strictly immediate, though
should be fast to avoid contention on future writes. Also, note that if we fail to do this (e.g. our server crashes),
the locks will time-out (by default after 2 minutes).

We unlock these locks asynchronously, placing them on a queue and periodically clearing them out. See ADR 15 for a
more detailed discussion.

Minimising TimeLock RPCs
------------------------

Principles
==========

1. Synchronous RPCs are expensive, so we seek to minimise them.
2. A sequence of exclusively TimeLock operations can be batched as a single call to TimeLock, even if there is an
   ordering constraint on these operations (because TimeLock can enforce them).
3. Suppose E1, E2 and E3 are three events that must occur in that order. E1 and E3 are TimeLock calls; E2 is not.
   Then, E1 and E3 cannot be batched together.

Write Transactions
==================

The write transaction protocol requires several calls to timestamp and lock services:

1. Startup: 3 calls (get immutable timestamp, lock immutable timestamp, get start timestamp)
2. User task: 0 calls by default, though user code can call for timestamps or locks directly
3. Commit: 3 calls (lock rows/cells, get commit timestamp, check locks)
4. Cleanup: 2 calls (unlock rows, unlock immutable timestamp)

However, some of these calls can be batched together, and others can be executed asynchronously.
Our current implementation has:

1. Startup: 1 call (startAtlasDbTransaction, which executes the three steps in order)
2. User tasks: 0 calls
3. Commit: 3 calls (lock rows/cells, get commit timestamp, check locks)
4. Cleanup: 0 synchronous calls; <=2 asynchronous calls

Efficiency
==========

We claim that for the current AtlasDB protocol, the remaining four synchronous RPCs must be separate.
We show that each successive pair of timestamp calls has an event that must happen after the first call but before
the second, thus splitting up the calls. Following principle 3, the calls must then be distinct.

1. The startup call must run before the user task; this guarantees that sweep won't remove values that were read
   before we commit or fail, and also gives us a start timestamp which the user is allowed to use in the task.
   Locking rows or cells must take place after the task (otherwise we don't know which locks to acquire).
2. Locking rows or cells must happen before writing to the database, which must happen before we get the commit
   timestamp (see the Commit section above).
3. Getting the commit timestamp must happen before the serializable commit check, which must happen before
   we check our locks (see the Commit section above). Interestingly, for snapshot transactions it appears possible to
   merge these last two calls (since the serializable check is a no-op).

Also, these four calls have to be synchronous. In current AtlasDB usage, running a transaction task is synchronous.
The last timelock call (checking locks) must happen before putUnlessExists which marks the end of the task, so we need
to wait for it. Each of the other TimeLock calls must happen before the last timelock call as well, so we also need to
wait for them.

Some tasks may be run with locks; in these cases, it may be possible to merge the AtlasDB transaction lock check and
the user-defined lock check together, which could save one RPC for tasks run with locks (though this is not currently
implemented).

There is scope for reducing the number of asynchronous calls. In particular, locks could be released immediately after
verification. However, we have avoided this for now because there is a risk of livelock where transactions roll back
one another after acquiring locks, preventing a successful commit.
