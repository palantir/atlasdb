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

   _transactions2 and _transactions3 are currently only supported for Cassandra and In-Memory KVSes.

.. warning::

   Schema versions 1 and 2 are vulnerable to a Cassandra consistency issue. Cassandra users should use
   schema version 3. Users of other key-value-services are not affected by this issue. If you are configured to use
   Cassandra and are on schema version 1 or 2, we will automatically install schema version 3.

AtlasDB needs to persist information about the start and commit timestamps of transactions that have committed.
This may be done in various ways, and is configurable. We currently support three strategies:

- version 1, which variable-length encodes the start and commit timestamps and stores them in the ``_transactions``
  table.
- version 2, which variable-length encodes the start and commit timestamps following the
  `TicketsEncodingStrategy <https://github.com/palantir/atlasdb/blob/develop/atlasdb-impl-shared/src/main/java/com/palantir/atlasdb/transaction/encoding/TicketsEncodingStrategy.java>`__,
  storing them in the ``_transactions2`` table.
- version 3, which variable-length encodes the start and commit timestamps following the
  `TwoPhaseEncodingStrategy <https://github.com/palantir/atlasdb/blob/develop/atlasdb-impl-shared/src/main/java/com/palantir/atlasdb/transaction/encoding/TwoPhaseEncodingStrategy.java>`__,
  storing them in the ``_transactions2`` table. This encoding strategy is for start timestamps consistent with the
  TicketsEncodingStrategy, meaning that it is permissible to use the same table, but the encoding of commit timestamps
  uses a two-phase commit protocol to avoid consistency issues.

If specified, this AtlasDB client will attempt to install the provided transaction schema version. This parameter is
optional; if it is not specified, this AtlasDB client will not install any new transaction schema versions, and will
run with whatever version is already installed (1 by default).

New versions can be specified in a live fashion without downtime, though there are two caveats to be aware of:

- The ``TransactionSchemaInstaller`` which installs new versions of transactions schemas only reads the configuration once every 10 minutes.
  If a faster installation is required, you can bounce one service node, as this is read on startup. In HA configurations, this will not
  require any downtime. After installation is done, a log message of the following form will be logged:

.. code-block:: none

  "We attempted to install the transactions schema version {}, and this was successful. This version will take effect no later than timestamp {}. (newVersion: 2, timestamp: 25161223)"

- The AtlasDB timestamp still needs to progress forward to that point before we use the new transactions schema version.
  If needed, one can force the new version to be used by fast-forwarding the timestamp to that point. To do this, see
  :ref:`Timestamp Service Management <timestamp-service-management>`.

Users can also consider the metrics produced in ``MetadataCoordinationServiceMetrics`` - currently, the following
metrics are available:

- ``eventualTransactionsSchemaVersion`` which should change to the target version after the intention to upgrade the
  transactions schema version has been installed by the coordination service. Note that the AtlasDB timestamp
  still needs to progress to the point where the new version actually comes into effect.
- ``currentTransactionsSchemaVersion`` which, at the time the metric measurement was taken, yields the schema version at
  a fresh timestamp. Note that this value may be cached (by default for up to 10 seconds) and metrics readings may only
  be taken at an interval, so it may be slightly out of sync, though it should be close.

The AtlasDB ``CoordinationStore``, ``CoordinationService`` and ``TransactionService`` interfaces are also instrumented with 
standard histograms for endpoint call rates and service times. Also, if using Cassandra KVS, it may be useful to 
consider Cassandra metrics for the ``_coordination`` table.
