================
Physical Cleanup
================

When using a MVCC scheme, there will be old data that is no longer
needed. We need to keep it around for a while because long running
read-only transactions may be reading it, but we eventually need to
clean up this old data.

AtlasDB has a variety of options for maximal flexibility.

.. _physical-cleanup-sweep:

Sweeping
========

Please consult the :ref:`Sweep documentation <sweep>` for more details on sweep.

Sentinel Values
---------------

The primary objective of AtlasDB is correctness. This manifests by never
relying on time for correctness. This means that we can't just assume
that a transaction that has been running for a day is aware that is
supposed to stop reading. It may continue reading and we want it to
throw rather than returning the wrong data. Returning data that
represents a state of the world that isn't the snapshot it's reading
would totally break the transactional guarantees we want.

We achieve this by writing an empty byte array at the ``-1``
timestamp called the sentinel value. Then we delete any data we think
should be cleaned up. Now, if a transaction reads the sentinel value, it
knows that it is missing data that it should have read. We throw in this
case rather than allowing inconsistent reads.

Sweep Strategies
================

SweepStrategy.NOTHING
---------------------

This disables sweeping for this table (including for manual sweep).

SweepStrategy.CONSERVATIVE
--------------------------

This is the default, but will always leave the most recent value and
also always keep a sentinel value (-1 timestamp). Even if the most
recent value is a delete, there will be 2 entries for each cell. (row,
col, -1, byte[0]) and also a delete entry at (row, col,
most\_recent\_ts, byte[0])

SweepStrategy.THOROUGH
----------------------

This sweep mode will clean up sentinel values and also the most recent
value if it is a delete. This is great for space savings where you have
a ``CELL_REFERENCING`` index that has a lot of churn.

There are 2 main downsides to THOROUGH:

1. Read only transactions are not allowed to read THOROUGH tables. This
   is because we clean the sentinel and a read only transaction may read
   a cell as a delete when it should have read data and we can no longer
   detect this case because we removed the sentinel.

2. Any readers of this table have to ensure their locks are still valid
   at read time. This is to prevent the case where the sweeper moves
   past your transaction because your immutable timestamp lock has
   expired. If the sweeper moved past your transaction, it may have
   cleaned up stuff you should have read.

Scrubbing
=========

If a transaction is marked as ``TransactionType.HARD_DELETE`` or
``TransactionType.AGGRESSIVE_HARD_DELETE`` all the rows that have been
written are kept track of in the \_scrub table. The scrubber will
inspect these rows and perform the same type of deletes as sweep, but
the data will be cleaned up much faster than a sweep which has to do a
big getRange over everything.

AGGRESSIVE\_HARD\_DELETE
------------------------

``AGGRESSIVE_HARD_DELETE`` will immediately call scrub after the
transaction is complete within the ``TransactionManager.run*`` call and
block until these rows are scrubbed. This must block until the
immutableTimestamp has passed the transaction, so it may take a second.
This will run all the OnCleanupTasks for the tables affected and block
again if those cause more rows to be cleaned.

Another side effect of AGGRESSIVE\_HARD\_DELETE is that read only
transactions may be aborted if they would read data that has been
aggressively scrubbed (even if they haven't timed out yet).

So basically you want the data deleted NOW maybe due to legal issues and
you do this at the expense of having some read only transactions fail.
