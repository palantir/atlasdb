=================
Performance Ideas
=================

Intended Use
============

AtlasDB is designed to be an OLTP distributed transactional DB. This
means that it is good at writing small chunks of work very quickly with
full ACID support. Below are some ideas of how to design your write path
to get the most out of AtlasDB.

Short-Lived Writes
==================

A long lived writing transaction may cause problems in the following
cases

-  Consistent backups cannot advance past the start time of this
   transaction
-  If we do any sort of cross data center replication it can't advance
   past this open transaction
-  We may be doing a bunch of work that will be rolled back at commit
   time due to a write/write conflict
-  If you grabbed locks before starting the transaction, other users
   could be blocking on these locks
-  Our locks may have timed out and we won't find out until commit time
   and have to retry all this work

The general theme here is that you are blocking others and you may have
to be rolled back and redo all the work. AtlasDB just buffers up writes
and doesn't check for conflicts/expired locks until commit time. If your
transaction takes tens of seconds, it's time to break it up.

Advisory Locking Prevents Write/Write Conflicts
===============================================

If you want to ensure that your transaction doesn't run into a conflict
then advisory locking is for you. The main reason you would want this is
when the input into your transaction is not immutable. An example of
this is working with legacy code and migrating to use AtlasDB. This is
bad design for sure, but instead of having to fix all the code that
might change the input, we can just use advisory locking to ensure that
we won't need to roll back this transaction.

AtlasDB has lock checking built into its
`protocol <AtlasDB-Transaction-Protocol>`__. This means that you can do
your own locking, then pass those locks to
``LockAwareTransactionManager.runTaskWithLock(...)``. If your
transaction commits successfully this means that no other transaction
concurrently held these locks and committed successfully. (Concurrently
means that either your commitTs falls between their startTs and commitTs
or vice versa)

Limit Write Size
================

The entirety of the write set is buffered in memory before you commit.
You will trigger warn and error logging at 10MB and 100MB respectively
if you write too much data. If you keep a transaction under 10MB, you
should be good to go.

Limit Row Size
==============

Every row should be able to be loaded into memory. A good rule of thumb
is to make sure that no row will be bigger than 10MB. If you imagine
that your rows will become bigger than this then you should maybe use
range scans instead of dynamic columns to store your data. This is the
total row size across all the cells possibly stored in many different
transactions.

Prefilter Columns When Possible
===============================

When we query the underlying data store for a getRow call for a named
table, usually we issue a bunch of getCell calls instead for each column
name in the table. getCell usually has to load less data off disk so
this is an IO optimization. This means that if you don't actually expect
those cells to be there, you should restrict the columns you are
selecting by using a ColumnSelection.

Lookups May Be Faster Than Range Scans
======================================

Some data stores have optimizations for doing an explicit lookup of a
row name like using bloom filters. These bloom filters are useless when
doing a range scan. This means that you may have to do more disk reads
to do a range scan than a lookup.

Highly Overwritten Dynamic Columns Need Garbage Collection
==========================================================

This one is a little tricky to explain. On some data stores
(hem...Cassandra...hmm) loading a row for a dynamic table basically
means loading the whole row across all history. This means that if you
do a lot of overwrites, you will be loading old values off disk and then
filtering them away. If you minimize overwrites (or you clean up old
values frequently) then you can avoid this extra I/O.

Snapshot Isolation
==================

This isn't a performance consideration, but is worth noting that
snapshot isolation only detects and throws on WRITE/WRITE conflicts.
This means that if you need to ensure that a value has not changed, you
have to "touch"it. Basically read its value and write it back. This is
called "materializing" the conflict.

This also means that you can't detect someone adding a cell. Let's say
you are trying to delete all the rows between 100 and 1000 and the only
row there is 500. So you do a read for all the stuff between 100 and
1000 and you get back 500 and delete it. Another transaction could be
concurrently writing 600 and you won't be able to detect it. The final
state of the world after you are done is not "everything between 100 and
1000 is deleted" like you expected.

This can be fixed by using Serializable, but note this also comes with a
performance penalty. When detecting write/write conflict, we need to do
reads on the "write set" at commit time. When detecting read/write
conflicts we need to also do another set of reads on our "read set" to
ensure they are all still the same. This also means our read set needs
to fit in memory because we need to keep track of everything to ensure
it's the same.

Basically, you need to think about the types of queries you want to run
and what your invariants are. Then you need to enforce these invariants
using "materialized conflicts" or "advisory locking" or Serializable
depending on which is best for you.

Transactions Should Have Immutable Inputs
=========================================

If your input is immutable it means that your transaction can be retried
easily. This is just good programming practice and ensures you are using
transactions correctly. If you are porting legacy code and this isn't
possible then see above section about advisory locking.

Ignore Conflicts When Possible
==============================

The cost of doing conflict detection is an extra read done at commit
time to ensure that no one has written a newer value to that cell.

Here is an example of where I can safely use IGNORE\_ALL. Let's say that
whenever I write to table A I also write a row in table B which is an
index on A. I don't need write/write conflicts on the index because the
every time there is a conflict on B, there is also a conflict on A. So I
can just use ``TableConflictHandler.RETRY_ON_WRITE_WRITE`` for A, and
``IGNORE_ALL`` for table B.

Tombstones and Range Scans
==========================

Since each delete is a tombstone that lives in the db if a table
contains 99% deletes then range queries will have to keep paging through
these deleted values before it finds any non-deleted values. This may
mean that simple index lookups become full table scans (This includes
range queries like isEmpty).

This is especially painful if you are iterating over a table from the
beginning and deleting values as you process them. This causes n^2
behavior because you have to scan past all deleted values before you
find new values for each page. This is similar to the sql performance
bug where you use *limit* and *offset* on an *order by* query that
doesn't have an index with that order.

We may allow removing sufficiently old tombstones at some point in the
future, but this is not currently planned.

Conflict Detection
==================

Conflicts are at the cell level, but write locking is at the row level
Normally lock contention in atlasdb is very low. If two transactions
write to the same cell there will be a small amount of lock contention
for the 2nd transaction and it will be rolled back. However, if you have
many clients writing to different cells in the same row, they will
experience the same lock contention, but no write/write conflict.

Avoid designs where you expect multiple clients to write into the same
row but different cells.

===================

Note: It is a good practice to call ``TransactionManager.close()`` when you
intend to no longer run transactions.
