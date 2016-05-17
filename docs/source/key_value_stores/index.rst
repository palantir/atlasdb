==================
Key Value Services
==================

Overview
========

This matrix details the current state of available Key Value Services supported
by the AtlasDB development staff.

+--------------------------------------+--------------------------------------+-------------------------------------------------------------+------------------------------------------+----------------------------------------------+
| Key Value Service                    | State                                | Service Versions Supported                                  | Known Issues                             | Details                                      |
+======================================+======================================+=============================================================+==========================================+==============================================+
| Cassandra KVS                        | Supported                            | Everything: Cassandra 2.0+ Data tables only: Cassandra 1.2+ | none                                     | :ref:`cassandra-kvs`.                        |
+--------------------------------------+--------------------------------------+-------------------------------------------------------------+------------------------------------------+----------------------------------------------+
| DB KVS                               | Supported                            | Postgres 8+, Oracle ?                                       |                                          | :ref:`db-kvs`                                |
+--------------------------------------+--------------------------------------+-------------------------------------------------------------+------------------------------------------+----------------------------------------------+
| CQL KVS                              | Unsupported                          | ?                                                           |                                          | :ref:`cql-kvs`                               |
+--------------------------------------+--------------------------------------+-------------------------------------------------------------+------------------------------------------+----------------------------------------------+
| JDBC KVS                             | Unsupported                          | ?                                                           | #371                                     | :ref:`jdbc-kvs`                              |
+--------------------------------------+--------------------------------------+-------------------------------------------------------------+------------------------------------------+----------------------------------------------+
| RocksDB KVS                          | Unsupported                          | ?                                                           | none                                     | :ref:`rocksdb-kvs`                           |
+--------------------------------------+--------------------------------------+-------------------------------------------------------------+------------------------------------------+----------------------------------------------+

Details
=======

.. _cassandra-kvs:

Cassandra KVS
-------------

The Cassandra KVS allows AtlasDB to use `Apache Cassandra <http://cassandra.apache.org/>`__ as it's backing key value service.  For it to be sole backing KVS for AtlasDB needs Cassandra Lightweight Transactions, otherwise earlier version of Cassandra can be used to back AtlasDB.

.. _db-kvs:

DB KVS
------

DB KVS Pending port from internal software

.. _cql-kvs:

CQL KVS
-------

Untestest, caused test failures on internal infrastructure

.. _jdbc-kvs:

JDBC KVS
--------

Untested

.. _rocksdb-kvs:

RocksDB KVS
-----------

Java RocksDB is not well support and we have seen major corruption, internal ticket QA-95061.
