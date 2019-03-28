.. _internal-schema-configuration:

=============================
Internal Schema Configuration
=============================

This document covers configuration pertaining to settings about the internal schema that AtlasDB uses for storing
some of its state. These settings should be specified under the ``internalSchema`` key in an
``AtlasDbRuntimeConfiguration``.

For a full list of the configurations available at this block, see
`InternalSchemaConfig.java <https://github.com/palantir/atlasdb/blob/develop/atlasdb-impl-shared/src/main/java/com/palantir/atlasdb/internalschema/InternalSchemaConfig.java>`__.

Target Transactions Schema Version
==================================

.. warning::

   _transactions2 is currently only supported for Cassandra and In-Memory KVSes.

AtlasDB needs to persist information about the start and commit timestamps of transactions that have committed.
This may be done in various ways, and is configurable. We currently support two strategies:

- version 1, which variable-length encodes the start and commit timestamps and stores them in the ``_transactions``
  table.
- version 2, which variable-length encodes the start and commit timestamps following the
  `TicketsEncodingStrategy <https://github.com/palantir/atlasdb/blob/develop/atlasdb-impl-shared/src/main/java/com/palantir/atlasdb/transaction/encoding/TicketsEncodingStrategy.java>`__,
  storing them in the ``_transactions2`` table.

If specified, this AtlasDB client will attempt to install the provided transaction schema version. This can be done in
a live fashion without downtime, though there are two caveats to be aware of:

- The ``TransactionSchemaInstaller`` which installs new versions of transactions schemas only reads the configuration once every 10 minutes.
  If a faster installation is required, you can (rolling) bounce your services. After installation is done, a log message of the following form will be logged:

.. code-block::

  "We attempted to install the transactions schema version {}, and this was successful. This version will take effect no later than timestamp {}. (newVersion: 2, timestamp: 25161223)"

- The AtlasDB timestamp still needs to progress forward to that point before we use the new transactions schema version.
  If needed, one can force the new version to be used by fast-forwarding the timestamp to that point. To do this, see
  :ref:`Timestamp Service Management <timestamp-service-management>`.
