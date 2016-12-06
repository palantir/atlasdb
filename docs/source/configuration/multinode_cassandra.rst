=================================
Multinode Cassandra HA Guarantees
=================================

A multinode Cassandra cluster can generally be in one of three states:

    1. All nodes are up.
    #. Some nodes are down, but there is a quorum of up nodes, i.e., the number of nodes that are down is fewer than half the replication factor (RF).
    #. Less than a quorum of the nodes are up.

In the case where all nodes are up, the entire Cassandra Key Value Service (KVS) API can be used. In the latter two cases, in order to guarantee consistency and correctness, usage of the KVS API is restricted as documented below.

A minority of nodes are down
============================

The following behaviour is guaranteed when interacting with a Cassandra cluster with three nodes, RF three, and one node down. More generally, this is the expected behaviour for a cluster that has at least one but fewer than RF / 2 nodes down.

.. list-table::
    :widths: 40 40
    :header-rows: 1

    *    - Cassandra KVS API Method
         - Behaviour

    *    - ``addGarbageCollectionSentinelValues``
         - Same as when all nodes are up.

    *    - ``cleanUpSchemaMutationLockTablesState``
         - Same as when all nodes are up.

    *    - ``close``
         - Same as when all nodes are up.

    *    - ``compactInternally``
         - Same as when all nodes are up.

    *    - ``create``
         - Same as when all nodes are up.

    *    - ``createTable``
         - Throws ``IllegalStateException``

    *    - ``createTables``
         - Throws ``IllegalStateException``

    *    - ``delete``
         - Throws ``PalantirRuntimeException``

    *    - ``dropTable``
         - Throws ``IllegalStateException``

    *    - ``dropTables``
         - Throws ``IllegalStateException``

    *    - ``get``
         - Same as when all nodes are up.

    *    - ``getAllTableNames``
         - Same as when all nodes are up.

    *    - ``getAllTimestamps``
         - Throws ``PalantirRuntimeException``

    *    - ``getLatestTimestamps``
         - Same as when all nodes are up.

    *    - ``getMetadataForTable``
         - Same as when all nodes are up.

    *    - ``getMetadataForTables``
         - Same as when all nodes are up.

    *    - ``getRange``
         - Same as when all nodes are up.

    *    - ``getRangeOfTimestamps``
         - Throws ``InsufficientConsistencyException`` (`when the iterator is accessed`).

    *    - ``getRows``
         - Same as when all nodes are up.

    *    - ``getRowsColumnRange``
         - Same as when all nodes are up.

    *    - ``multiPut``
         - Same as when all nodes are up.

    *    - ``put``
         - Same as when all nodes are up.

    *    - ``putMetadataForTable``
         - Throws ``IllegalStateException`` after 1 minute.

    *    - ``putMetadataForTables``
         - Throws ``IllegalStateException`` after 1 minute.

    *    - ``putUnlessExists``
         - Same as when all nodes are up.

    *    - ``putWithTimestamps``
         - Same as when all nodes are up.

    *    - ``truncateTable``
         - Throws ``PalantirRuntimeException``

    *    - ``truncateTables``
         - Throws ``PalantirRuntimeException``

Less than a quorum of nodes are up
==================================

Superuser authentication will fail with a ``AuthenticationException``, since quorum is necessary for this operation. A non-super user authentication may still succeed, but all of the above operations should fail. `Note that this behaviour is still being verified!`
