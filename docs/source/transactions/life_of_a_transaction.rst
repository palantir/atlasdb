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

Commit
======

Cleanup
=======

Minimising TimeLock RPCs
------------------------
