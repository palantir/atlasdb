=======================
Multinode Cassandra
=======================

High Availability Guarantees
============================

The following behaviour is expected when interacting with a three node Cassandra cluster with one node down (or, more generally with a cluster that has one node down, but still has quorum):

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Cassandra KVS API Method
         - Behaviour

    *    - addGarbageCollectionSentinelValues
         - Successful

    *    - cleanUpSchemaMutationLockTablesState
         - Successful

    *    - close
         - Successful

    *    - compactInternally
         -

    *    - create
         - Successful

    *    - createTable
         - Throws `IllegalStateException`

    *    - createTables
         - Throws `IllegalStateException`

    *    - delete
         - Throws `IllegalStateException`

    *    - dropTable
         - Throws `IllegalStateException`

    *    - dropTables
         - Throws `IllegalStateException`

    *    - get
         - Successful

    *    - getAllTableNames
         - Successful

    *    - getAllTimestamps
         - Successful

    *    - getLatestTimestamps
         - Successful

    *    - getMetadataForTable
         - Successful

    *    - getMetadataForTables
         - Successful

    *    - getRange
         - Successful

    *    - getRangeOfTimestamps
         - Successful

    *    - getRows
         - Successful

    *    - getRowsColumnRange
         - Successful

    *    - multiPut
         - Successful

    *    - put
         - Successful

    *    - putMetadataForTable
         - Throws `IllegalStateException`

    *    - putMetadataForTables
         - Throws `IllegalStateException`

    *    - putUnlessExists
         - Successful

    *    - putWithTimestamps
         - Successful

    *    - truncateTable
         - Throws `IllegalStateException`
