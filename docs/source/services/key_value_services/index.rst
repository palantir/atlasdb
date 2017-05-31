==================
Key Value Services
==================

Overview
========

This matrix details the current state of available Key Value Services supported by the AtlasDB development staff.

.. list-table::
    :widths: 5 5 5 5 80
    :header-rows: 1

    *    - Key Value Service
         - State
         - KVS Versions Supported
         - Recommended Versions
         - Errata

    *    - :ref:`Cassandra KVS <cassandra-configuration>`
         - Supported
         - 2.0+
         - 2.2.8
         - 

    *    - :ref:`DB KVS (Postgres) <postgres-configuration>`
         - Supported
         - 9.2+
         - 9.3.4
         - Amazon RDS explicitly supported, other HA postgres variants not currently supported

    *    - :ref:`DB KVS (Oracle) <oracle-configuration>`
         - Beta Support
         - 11g+
         - 12c
         - Amazon RDS explicitly supported, RAC not currently supported

    *    - CQL KVS
         - Unsupported
         -
         -
         -

Details
=======

.. contents::
   :local:

.. _cassandra-kvs:

Cassandra KVS
-------------

The Cassandra KVS allows AtlasDB to use `Apache Cassandra <http://cassandra.apache.org/>`__ as it's backing key value service.
For it to be sole backing KVS for AtlasDB needs Cassandra Lightweight Transactions, otherwise earlier version of Cassandra can be used to back AtlasDB.

.. _db-kvs:

DB KVS
------

DB KVS allows AtlasDB to use Postgres and Oracle as a backing store.

.. _cql-kvs:

CQL KVS
-------

CQL KVS is not currently supported, but we plan on supporting this KVS eventually.


Deleted
=======

ClusterKeyValueService - Intended to be run on top of a MySQL/Cluster
RocksDbKVS - Removed in 0.44
JDBCKVS - Removed in 0.44
