========
Benefits
========

Deadlock Free
=============

AtlasDB supports Snapshot Isolation (SI) as well as full Serializable
Snapshot Isolation (SSI). SI is naturally deadlock free however a transaction may need to
be retried due to a write/write (w/w) conflict.

SSI may have a cycle of transactions that block on each other, but one
of them will be restarted because of a read/write conflict (which is
guaranteed to exist).

Physical Representation Agnostic
================================

All key value stores have the same transactional semantics. One of the
great benefits of AtlasDB is that your unit tests can run on an
in-memory K/V store and semantically it will behave the same as when you
run it on Cassandra (or HBase or whatever). This provides a quick way to
get started and move up as needed.

A common way to start is to use Postgres (or even leveldb) as the
backing store and migrate to Cassandra when high availability or more
scale is needed. Since we have a transaction layer in between your
client code and the underlying K/V store, we can do an online migration
from one store to another. Basically you just make a new K/V store that
writes to both postgres and Cassandra. Then you backfill all your
postgres data over. Then you can flip reads to Cassandra. Then you can
just do writes to Cassandra and shut down the postgres db.

Indexes
=======

Once you have transactions, indexes are super easy to keep consistent.
Whenever you write a row, you just update the index and it goes in a
single transaction. This may be the most painful thing missing from K/V
stores.

One quick note is that some systems like Cassandra have indexes, but to
query them you have to talk to all nodes. This is because they don't
have transactions so the indexes have to live locally with the primary
data. An index lookup or range read in AtlasDB just talks to 1 node. It
is efficient to answer the query "give me the next row after this row".

Clear Transaction Semantics
===========================

Because this is open source code, it is easy to have full control over
what your transactions semantics are. There is some level of trust with
some proprietary RDBMS systems that they do the right thing and their
transaction levels are what you think they are. AtlasDB allows you very
precise control over your consistency/performance trade-off. Conflict
handlers are specified per table and the default is Snapshot Isolation.

Advisory Locking
================

Advisory Locking is a first class feature in AtlasDB. An example with a
traditional DB would be grabbing a lock/lease to ensure that only one
thread touches a specific row at a time. The main issue with this is
that your lock/lease may time out right before you call commit(). Even
if you refresh your lock right before commit, it may still happen. This
means that another thread may have grabbed the lock and is violating
your guarantees. One of the design goals of AtlasDB is correctness, so
this type of race condition is unacceptable. Who wants to eat the
performance penalty of using transactions and locks just to have them
fail when the going gets tough?

When you open a transaction in atlasdb you can pass in locks that you
want to hold for the duration of the transaction. You get a proper
guarantee that either your transaction will be aborted, or any
transaction that holds any of these locks will see your committed data
(assuming these locks are exclusive locks).

All AtlasDB transactions are optimistic, so this feature is useful if
there are long running or heavy weight transactions that you don't want
to run optimistically.
