==================
Key Value Services
==================

Overview
========

This matrix details the current state of available Key Value Services supported by the AtlasDB development staff.

.. list-table::
    :widths: 5 5 5 5
    :header-rows: 1

    *    - Key Value Service
         - State
         - KVS Versions Supported
         - Recommended Versions

    *    - :ref:`Cassandra KVS <cassandra-configuration>`
         - Supported
         - Cassandra 2.0+
         - Cassandra 2.2.8

    *    - :ref:`DB KVS (Postgres) <postgres-configuration>`
         - Supported
         - Postgres 8+
         - 9.3.4

    *    - :ref:`DB KVS (Oracle) <oracle-configuration>`
         - Beta Support
         - ?
         - ?

    *    - CQL KVS
         - Unsupported
         -
         -

    *    - JDBC KVS
         - Unsupported
         -
         -

    *    - RocksDB KVS
         - Unsupported
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

.. _jdbc-kvs:

JDBC KVS
--------

JDBC KVS existed prior to DB KVS and was the primary means for deploying applications on Postgres.
JDBC KVS has been deprecated in favor of DB KVS.

.. _rocksdb-kvs:

RocksDB KVS
-----------

Java RocksDB is not well supported, and we have seen major corruption under normal usage, internal ticket QA-95061.

Deleted
=======

ClusterKeyValueService.java - Intended to be run on top of a MySQL/Cluster
