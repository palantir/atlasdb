=================================
Multinode Cassandra HA Guarantees
=================================

A multinode Cassandra cluster can generally be in one of three states:

    1. All nodes are up.
    #. Nodes down, but there is a quorum of up nodes (more than half the nodes are up).
    #. Less than a quorum of the nodes are up.

In the case where all nodes are up, the entire KVS API can be used. In the latter two cases, in order to guarantee consistency and correctness, usage of the KVS API is restricted as documented below.

A quorum (but not all) of nodes are up
======================================

The following behaviour is guaranteed when interacting with a three node Cassandra cluster with one node down. More generally, this is the expected behaviour for a cluster that has one node down, but still has quorum.

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
         -

    *    - ``create``
         - Same as when all nodes are up.

    *    - ``createTable``
         - Throws ``IllegalStateException``

    *    - ``createTables``
         - Throws ``IllegalStateException``

    *    - ``delete``
         - Throws ``IllegalStateException``

    *    - ``dropTable``
         - Throws ``IllegalStateException``

    *    - ``dropTables``
         - Throws ``IllegalStateException``

    *    - ``get``
         - Same as when all nodes are up.

    *    - ``getAllTableNames``
         - Same as when all nodes are up.

    *    - ``getAllTimestamps``
         - Same as when all nodes are up.

    *    - ``getLatestTimestamps``
         - Same as when all nodes are up.

    *    - ``getMetadataForTable``
         - Same as when all nodes are up.

    *    - ``getMetadataForTables``
         - Same as when all nodes are up.

    *    - ``getRange``
         - Same as when all nodes are up.

    *    - ``getRangeOfTimestamps``
         - Same as when all nodes are up.

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
         - Throws ``IllegalStateException``

    *    - ``truncateTables``
         - Throws ``IllegalStateException``

Less than a quorum of nodes are up
==================================

Authentication will fail with a ``AuthenticationException``, since quorum is necessary for this operation. It is therefore guaranteed that the cluster is inaccessible in this case.
