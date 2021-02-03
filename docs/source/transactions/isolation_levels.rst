.. _isolation-levels:

================
Isolation Levels
================

Snapshot vs Serializable vs No Conflict
=======================================

Transaction types (snapshot or serializable) provide different
transactional properties.

-  Serializable

   -  Read and Writes to this table both happen at commit time
   -  Implemented by checking that reads made at startTs return the
      same results at commitTs. ABA scenarios are permitted.

-  Snapshot

   -  Reads happen at the startTs
   -  Writes happen at the commitTs
   -  Possibly has write skew

-  No Conflict

   -  Reads happen at the startTs
   -  Writes happen at the commitTs, with the caveat that when there are
      concurrent transactions sometimes the earlier committing
      transactions value is kept instead of the later committing
      transaction
   -  This means that writes may appear to be lost if they happen
      concurrently with a transaction that overwrites them.

-  Read only

   -  All reads happen at startTs

Having mixed transaction types in different table may produce weird
behavior where some tables read and write at commitTs, but other tables
are reading at startTs and the states are incompatible because
transactions have committed in between these times.

Serializable Transactions Theory
================================

The Serializable Transactions in AtlasDB are built using a few
techniques developed in the last decade.

The main papers used to figure out the best approach for this are

Cahill 2009:
https://ses.library.usyd.edu.au/bitstream/handle/2123/5353/michael-cahill-2009-thesis.pdf

FEKETE 2005: "Making snapshot isolation serializable"

The main inspiration is that an SI transaction that has the same values
it read at its startTs and its commitTs is serializable. This can be
seen by making every read only transaction happen instantly as though it
did all its work at its startTs. Every write transaction can be seen to
do all its work instantly at its commitTs. We know that write SI
transactions don't actually work this way, they read values at their
startTs. However if we ensure these reads are exactly the same at
commitTs then we can act like all the work was done instantly at the
commitTs.

So our impl is quite obvious, except for one hiccup. We do the same
atlasdb transaction protocol but after we get our commitTs, we do a
check to verify that none of our read set has changed. This implies that
we need to keep track of everything we read and its result (including
ranges and also which values were missing). The only issue with this
approach is now we may have a deadlock where a bunch of transactions
have their commitTs allocated, but are waiting to commit because these
read only transactions are blocking waiting for each other to decide if
they want to commit or not.

Here is where the papers come in to play. We know that any deadlock that
exists must be composed of currently running transactions. If the cycle
had one transaction already committed in it, then the transaction
blocking on that one would be able to complete its read and there would
be no deadlock. Since all the transactions are concurrent, we know at
least one dependency between them must be a r/w dependency (fekete2005).
Ideally we would only abort one of these transacitions, but that may be
difficult. Instead we simplify and abort any transaction which may block
on a startTs which is greater than their startTs. This situation is
almost always going to be a r/w conflict unless there is a write
failure. This will surely break the deadlock and also is easy to check.
