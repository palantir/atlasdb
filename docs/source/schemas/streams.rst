.. _schemas-streams:

=======
Streams
=======

Overview
========

When storing large objects in an AtlasDB database, the underlying
database can impose limits on the size of the object and its performance
characteristics. Postgres has a 1GB entry limit, and Cassandra generally
does not like transmissions of over 30MB over the wire at a time. 

If it is known beforehand that a particular column of a table will store
large objects, the objects should instead be stored in a stream store table.
AtlasDB will automatically segment large objects in a stream store into 
smaller chunks, and storage of streams happens on separate transactions and
does not count toward your byte limit for your local trasaction.

The minimal parameters required to define a stream store are shown below:

.. code:: java

    public void addStreamStoreDefinition(
        new StreamStoreDefinitionBuilder(String shortName, String longName, ValueType valueType).build()
    )
    
- ``shortName`` is the prefix which will be used in the actual database tables.
- ``longName`` is the name of the stream store in the generated Java code.
- ``valueType`` specifies the value type of the id for each stored object.

Additional options for the builder include:

- ``hashFirstRowComponent``
- ``isAppendHeavyAndReadLight``
- ``compressBlocksInDb``
- ``compressStreamInClient``: transparently decompresses and compresses the 
  stream via the LZ4 algorithm upon reads and writes, respectively. 
  Compression is performed client side before any network communication to 
  the underlying database.
- ``inMemoryThreshold(int inMemoryThreshold)``: specifies the largest size
  object (in bytes) which AtlasDB will cache in memory in order to boost
  retrieval performance.

.. note::

    In some places, we load whole numbers of blocks into memory, so if the in-memory threshold is smaller than the block size (1MB), we will still load a whole block.

Storing Streams
===============
Users have two options to store streams.

1. ``getByHashOrStoreStreamAndMarkAsUsed(Transaction t, Sha256Hash hash, InputStream stream, byte[] reference)`` is called *within* a transaction. It will store the stream and mark it as used by ``reference``. This method is the preferred method to store small (<100 MB) streams. 
2. ``storeStream(InputStream stream)`` is called *outside* a transaction. It will store a stream without marking it as used, and users should mark the stream as used via ``markStreamAsUsed(Transaction t, long streamId, byte[] reference)`` immediately after. Users of this method need to retry if their stream was cleaned up between storing and marking. This method is the preferred method for very large streams (>100 MB) that would otherwise cause the transaction within which the stream is being stored to be too long running.

Regular AtlasDB tables should refer to the object stored in stream stores by their ``streamId``, and the ``reference`` that users mark the stream with should correspond to a value computable based on the parameters of the cell the ``streamId`` is stored in. If the ``streamId`` is deleted from that cell, users need to mark the stream as unused by that reference to enable garbage collection.

.. note::
    
    References to streams are *manually* added and removed by users. AtlasDB monitors the number of references to each stream, and streams that are unreferenced will be garbage collected.

Performance
===========

Streams are marked as ``cachePriority(CachePriority.COLD)`` by default.
This is a hint to the key value store that these can potentially be
stored on different media than regular tables. One simple way to handle
this is to write a ``KeyValueService`` implementation that just
delegates to 2 other ``KeyValueService`` impls and sends COLD tables to
one and other tables to another.

Streams can be optionally compressed to boost performance via the 
``compressStreamsInClient`` option on the ``StreamStoreDefinitionBuilder``.

Transactionality
================

When you call ``loadStream(transaction, id)``, only the first section of the stream data is pre-loaded as part of that transaction.
The amount loaded is determined by the block size (1MB) and the in-memory threshold (4MiB by default); we load at least one block,
a whole number of blocks, and (if the in-memory threshold is at least the block size) as many blocks as fit inside the in-memory threshold.
So by default, we load 4 blocks (``4*10^6`` bytes, slightly less than the default in-memory threshold of ``4*2^20`` bytes).
If your stream does not fit inside the in-memory threshold, then the remainder of the data will be buffered in separate transactions.

This has some subtle transactionality implications.
Suppose you store a mapping of keys to stream IDs, and at ``start_timestamp``, ``key`` maps to ``stream_id_1``.
You then call ``loadStream(transaction,stream_id_1)``, and keep the resulting ``InputStream`` open.
Before you reach the end of the stream, another thread stores a mapping from ``key`` to ``stream_id_2``.
In this case, streaming of the original stream will continue and complete successfully, even though the data being streamed is out of date.

If you wish to guard against this case, you should kick off another read transaction after closing the stream, to verify that ``key`` still maps to ``stream_id_1``.

Non-Duplication
===============

A single stream of bytes may be stored multiple times and have many
references to it, but will only be stored once under the covers. When
all references to a stream are removed it will be cleaned up by the
OnCleanupTask that is registered with the cleaner. See
`Cleanup <Cleanup>`__ for more details.
