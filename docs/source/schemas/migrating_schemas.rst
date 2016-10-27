.. _migrating-schemas:

=================
Schema Migrations
=================

AtlasDB does not provide any mechanism or tooling specific to performing schema migrations.
Products using AtlasDB need to validate the existing schema, create the proper tables, and migrate data as necessary and should not rely on AtlasDB to natively perform these operations.

.. _schema-mutation-lock:

Schema Mutation Lock (Cassandra only)
=====================================

Cassandra 2.1 introduced a race condition that allows clients to create two versions of a table with the same create table command.
Upon restarting your Cassandra cluster, one table will be chosen arbitrarily and the other one will be deleted, corrupting your database.
This is a known issue with Cassandra and is being tracked on `CASSANDRA-10699 <https://issues.apache.org/jira/browse/CASSANDRA-10699>`__.
To get around this in the meantime, we do two things:

#. All `schema mutations` - creating, altering, or dropping tables - are protected by a non-expiring lock called the `schema mutation lock`, and the state of this lock lives in the _locks table in Cassandra.
   A lock holder will free the lock after finishing the schema mutation.
   A free lock will be denoted by ..., a ``Long.MAX_VALUE``. A held lock will be anything other than the cleared value.
#. We designate a lockCreator in the :ref:`Cassandra KVS config <cassandra-kvs-config>` to ensure we do not hit the race condition while creating the _locks table.

Occasionally we will lose an AtlasDB client while performing a schema mutation, and since the schema mutation lock does not expire, you will need to manually clear the lock.
See :ref:`clearing-schema-mutation-lock` for steps on how to manually clear the lock.