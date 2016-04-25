=======
Streams
=======

Overview
========

When storing large objects in an atlasdb database, the underlying
database can impose limits on the size of the object and its performance
characteristics. Postgres has a 1GB entry limit, and Cassandra generally
does not like transmissions of over 30MB over the wire at a time. If it
is known beforehand that a particular column of a table will store large
objects, the object and an associated id can be instead stored in a
stream store table. AtlasDB will automatically segment large objects
into smaller chunks and store them in a separate stream store table. The
stored object can then be streamed or updated by the associated id.
AtlasDB will also reference count these objects and clean them up when
they are not used anymore. Also, the storage of the streams happens on
separate transactions and does not count toward your byte limit for your
local trasaction.

.. code:: java

    public void addStreamStoreDefinition(String long_name, String short_name, ValueType streamIdType, int inMemoryThreshold);

The long name is the name of the stream store in the generated java
code, the short name the prefix which will be used in the actual
database tables. The stream id type argument specifies the ValueType of
the id for each stored object. The in-memory threshold argument
specifies the largest size object (in bytes) which atlasdb will cache in
memory in order to boost retrieval performance.

Performance
===========

Streams are marked as ``cachePriority(CachePriority.COLD)`` by default.
This is a hint to the key value store that these can potentially be
stored on different media than regular tables. One simple way to handle
this is to write a ``KeyValueService`` implementation that just
delegates to 2 other ``KeyValueService`` impls and sends COLD tables to
one and other tables to another.

Non-Duplication
===============

A single stream of bytes may be stored multiple times and have many
references to it, but will only be stored once under the covers. When
all references to a stream are removed it will be cleaned up by the
OnCleanupTask that is registered with the cleaner. See
`Cleanup <Cleanup>`__ for more details
