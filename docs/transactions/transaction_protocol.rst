Protocol
========

NOTE: the AtlasDB transaction protocol is inspired by (but different
from) google's percolator transaction protocol. For additional reading
please see
`Percolator <http://research.google.com/pubs/pub36726.html>`__.

AtlasDB is a standard MVCC Snapshot Isolation protocol. Each transaction
has a start timestamp and a commit timestamp. You can view all rows that
have a commit timestamp less than your start timestamp. You will get a
write-write conflict for any cells that you write that were modified and
that committed at a timestamp between your start timestamp and your
commit timestamp.

Write Protocol
--------------

1. We buffer writes in memory until commit time. At commit time we grab
   row locks for all the rows we are about to write (this is to detect
   write/write conflicts). We also grab the start timestamp row lock for
   the Transaction table.

2. Now that we have our locks we check for write/write conflicts. If any
   cell has been modified by a transaction that committed after our
   startTS then we have a write conflict.

3. Write the data to the KV store with TS = startTs.

4. Now we get a fresh commit timestamp.

5. Then (this is important) we make sure our locks are still valid. If
   our locks expire after this point that is ok, but they have to be
   valid now.

6. Then we atomically do a "putUnlessExists" into the transaction table
   with our commit timestamp.

7. Unlock the locks

Read Protocol
-------------

Lets assume we are reading Cell c.

1. Read from the KV store and get the most recent data with TS <
   startTs.
2. Get a read lock on the transaction row for c.startTs (not needed if
   c.startTs is less than immutatableTs) This is to wait to make sure
   the transaction that wrote it is done.
3. Read the transaction table for the commitTs.
4.  

-  If commitTs doesn't exists try to roll back this transaction and
   start over. If it is -1 (been rolled back) delete the associated data
   and start over.
-  If c.commitTs greater than your startTs skip it and move on to the
   next highest TS for the cell.

Immutable Timestamp
-------------------

The point in time right before the oldest currently executing
transaction is referred to as the Immutable timestamp. This is because
nothing before this point in time will change. (All writes are available
to read and are either committed or pending commit.)

Any timestamp before the oldest open transaction's start timestamp may
be called the immutable timestamp, but generally it refers to the most
recent TS for which this is true.

To implement this we grab a new TS (PRE\_START\_TS) and lock that before
we begin our transaction. We have a feature in the lock server to return
the minLockedInVersion. if PRE\_START\_TS is the oldest, then lock
server will return this as the minLockedVersion. The write protocol
ensures that this lock is still held after writes are done to the
underlying store. This is the only part of the lock server that doesn't
shard well because we have to get the global min. However we can just
ask each lock server what its min is and take the global min. We can
also cache this value for a bit and we don't have to recompute it each
time. Normally clients don't need the absolute most recent
Immutable\_TS, but just a relatively modern one.

Tricky points regarding the immutable timestamp:

-  A transaction with start\_ts < immutable\_ts may be stuck on the
   putUnlessExists part of its commit (its locks are timed out,
   otherwise immutable\_ts < start\_ts). This is ok because if we read
   any of its values we will try to roll back their transaction and we
   will either see it as committed or failed, but either way it will be
   complete.
-  The immutable timestamp is not guaranteed to be strictly increasing.
   This is because the action of grabbing PRE\_START\_TS and the action
   of locking it are not performed together atomically. This doesn't
   cause correctness issues, though, since we wait until we have locked
   PRE\_START\_TS before grabbing our start timestamp. For example, if
   the current immutable timestamp is immutable\_ts\_1 and transaction T
   locks in a lower value immutable\_ts\_0, then T's start\_ts must be
   greater than immutable\_ts\_1, so any readers who grabbed
   immutable\_ts\_1 will still grab locks when trying to read rows
   written by T.

Cleaning Up Old Values
----------------------

Since we are doing away with historical transactions, we can clean up
old values. We are still allowing long running read transactions, but we
should cap them so they can only run for a couple days or weeks. This
means that we can go through and clean up old values if they have been
written over for at least x days (let's just say 10 days for now).

One issue is that we don't have a mapping from TS to real time. Also we
don't trust real time anywhere else and don't plan to start now. We can
take a ts every hour or so and pick one before 10 days ago. This does
impose a small relationship to "time" but we will mitigate it in the
next paragraph.

What if we have a reader that is still reading but is very old (before
10 days ago (or so we think)). We solve this case by writing a dummy
value for a Cell we are going to clean up with a negative timestamp and
then cleaning up old rows from oldest to newest. This means that if you
are still reading and stumble on a row that has been cleaned that you
would have read, then you will read a row with a negative TS which will
get turned into a TooOldReaderException (which is a retriable
exception). This ensures that we can do cleanup of old values safely
even in the presence of arbitrarily old readers.

If we want to support reading of values older than "10 days" then these
readers will have to start reserving more time to push out cleanup up
old values. Basically every so often a long running read should "check
in" which will ensure old values won't get cleaned up out from under it
for another day or so. Note this cleanup has implications with respect
to hard delete and we may want to force a cleanup sooner and allow these
long running reads to fail in the name of hard delete.

Cleaning up old nonce values
----------------------------

Part of doing cleanup is writing an empty value at a negative timestamp
for some cell. This works to prevent old read only transactions from
reading empty value when really they should have read a cleaned value.
However these negative timestamp values can build up and take up a lot
of space and also make range scans really slow if the whole table is
full of these nonce values.

For specific tables we allow these old nonce values to be removed from
the KV store, but at the cost of never being able to read this table in
a read-only transaction. This seems like a good trade-off and lets us
build indexes with status variables and be able to delete old values
completely and still support range scans.

Read/Write Conflicts
--------------------

The transaction protocol has write/write conflicts built into it. If two
transactions touch the same row, one will be rolled back (as long as the
table does write/write conflict detection (which is the default)). What
if a user wanted some way to set up read write locks. This can be built
into the protocol fairly easily. Currently a table can be set up either
to ignore all conflicts or to have write/write conflicts. There is a
third option we can do called read\_write\_conflicts. The semantics we
want are if your transaction reads a value and a new (different) value
for this cell has been committed then we should rollback. Similarly if
you write a value and an already committed transaction read the value
then you should retry.

The way we accomplish this is very similar to write/write conflicts. If
we are storing back the same value we read (read side of the
read/write), then we are looking for transactions that committed after
our start that wrote a different value to this cell. If we are writing a
new value (write side) then we should roll back if we see any new
commited rows regardless of if they are different than what was there
before.

This could be used to implement acl changes for objects that don't
require locking for the duration of the transaction. We could just have
a table set up as READ\_WRITE\_CONFLICTS and in this table we have a row
for each object with a counter in it. Every time there is a security
change to an object we increment this counter. Every time we do any
other write operation to this object we read and touch this counter.

The main problem with read/write conflicts if that you can't control the
fairness of these transactions. If read operations keep coming in and
are fast then a write operation may keep retrying and get starved and
never complete.

.. raw:: html

   <div>

The easiest way to implement this read/write conflict would be to check
the last value that was successfully committed to the cell and see if it
was equal

.. raw:: html

   </div>

.. raw:: html

   <div>

to the value being stored. This way if you are just doing a touch you
are basically checking that the last committer put the value that you
are storing. This will work the same as a compare and swap check. This
version is more scalable because you only have to check the most recent
successful commit and not all commits after your start time. The
downside if you don't get true read/write exclusion, you basically just
get CAS semantics. This isn't a big deal because using a counter is the
most common way to use this type of exclusion anyway.

.. raw:: html

   </div>

Proof of Correctness
====================

If we want to prove that this protocol works this means that we need to
show that if a transaction commits before our start timestamp then we
will read that data.

Reading All Writes Before Transaction Start
-------------------------------------------

We must ensure writes committed before our start are read. If we look at
the write protocol then we know that all writes are complete to the KV
store THEN get a commit timestamp and THEN verify our locks are still
valid. Then it proceeds to putUnlessExists to the transaction table.

This means that if a commitTs is less than our startTs then the KV store
will already have these rows written. We require that the underlying KV
store has durable writes so these rows will be read.

Lock Timeouts After Validation
------------------------------

What if locks time out after we do the check that they are still valid?
If locks time out while writing to the transaction table we depend on
the putUnlessExist to arbitrate whether a transaction is committed or
not. If the transaction hangs while trying to commit then it is possible
a reader will come roll it back. In this case we will need to retry our
transaction, but we don't expect this to happen in normal cases. If the
lock server is restarted and forgets all its locks then this becomes
more likely. This means that the transaction table must have strong
consistency guarantees, but the rest of the system only has to have
durable writes. The standard way of getting this level of consistency is
to use a write ahead log to know what has/hasn't been committed.
Bookkeeper is an example of a project that implements this kind of log.

Ignoring Writes Committed After Transaction Start
-------------------------------------------------

We need to ensure that writes committed after our startTs are not read.
If we get back a row from the KV store then we know that the txn that
wrote it has a startTs less than ours, but it may still be in progress
or committed. We postfilter on the transaction table. If we find that
the locks for this txn are no longer held, but there still isn't a row
in the transaction table, then we force it to be rolled back. This will
ensure that when the txn tries to commit then it will fail and have to
retry. If our rollback fails because txn did actually commit then we
read that value and carry on. We can retry until the value is there, but
usually we just throw and retry the current transaction if there is a
remoting failure.

Ignoring Failed Transactions
----------------------------

This is achieved because we post-filter all reads through the
transaction table. If we find that transaction is rolled back, then we
just delete it and retry the read.

Non-Obvious Semantics
=====================

Read Rollbacks
--------------

Reads must rollback transactions they find that are uncommitted. If a
read doesn't go out of its way to roll back an uncommitted row and just
skips it and keeps looking in the past for a committed row, then it
cannot be sure that this row doesn't get committed later. The committing
transaction may be stuck right before the "putUnlessExists" part of the
write protocol. If this is the case, we can't be sure that transaction
isn't going to have a commit timestamp before our start timestamp, so we
have to make sure this transaction will be failed for sure before we can
skip past it.

Serializable Isolation
======================

AtlasDB can be extended to have serializable isolation semantics.
Basically instead of looking at your write set and detecting writes that
commit in between your start and commit timestamps we should look at the
read set and detect writes the same way. The only tricky bit is handling
range scans. There are a few proofs that removing this read-write
conflict is sufficient to achieve serializability. The simplest proof is
from "A Critique of Snapshot Isolation" and basically states that if you
remove all writes that could commit between your start and commitTs,
then you can make a serial ordering by just compressing down all the
actions of a transaction to happen right before its commit timestamp.
This works because all reads you do will be the same at the startTs as
they are at the commitTs.

Removing read-write conflicts is sufficient to get serializability if
every single transaction does this. However sometimes it is desirable to
run with a mix of SI and SSI. This means that transactions that choose
Serializable should also check for write-write conflict so they are
compatible with SI transactions.

One of the best features of Serializable Isolation is that you get true
Linearizability. Each transaction can be treated like it is just
happened instantaneously at its commit timestamp and all invariants hold
at all times.

The main downside to this approach is that all the reads need to be done
after the commit timestamp is allocated and therefore after all the
writes are done to the underlying store. What this means is that other
transactions may have to block on these written values while we do reads
to ensure they haven't changed. The good news is that the only times a
transaction would wait is if it could have a read-write conflict. This
means that the waiting may result in a rollback anyway so waiting isn't
a huge hit. To mitigate this issue we should make transactions that
write hot rows not have a huge read set that needs to be verified.
