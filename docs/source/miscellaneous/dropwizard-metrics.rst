.. _dropwizard-metrics:

==================
Dropwizard Metrics
==================

AtlasDB makes use of the Dropwizard `Metrics library <http://metrics.dropwizard.io/>`__ to
expose a global ``MetricRegistry`` called ``AtlasDbRegistry``. Users of AtlasDB should use ``AtlasDbMetrics.setMetricsRegistry``
to make use of our ``MetricRegistry``.

We expose the metrics below. For the Cassandra client metrics with ``<host>``, we will expose metrics specific to every
Cassandra node in your cluster. For more details on what information each type of metric provides, we recommend reading
the Metrics `Getting Started Guide <http://metrics.dropwizard.io/3.1.0/getting-started/#>`__.

**Gauges**

 - ``com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool.numBlacklistedHosts``
 - ``com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool.<host>.requestConnectionExceptionProportion``
 - ``com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool.<host>.requestFailureProportion``
 - ``com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool.requestConnectionExceptionProportion``
 - ``com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool.requestFailureProportion``
 - ``com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPoolingContainer.<host>.meanActiveTimeMillis``
 - ``com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPoolingContainer.<host>.meanBorrowWaitTimeMillis``
 - ``com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPoolingContainer.<host>.meanIdleTimeMillis``
 - ``com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPoolingContainer.<host>.proportionDestroyedByBorrower``
 - ``com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPoolingContainer.<host>.proportionDestroyedByEvictor``

**Histograms**

- ``com.palantir.atlasdb.transaction.impl.SnapshotTransaction.byteSizeTx``

**Meters**

- ``com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool.<host>.requestConnectionExceptions``
- ``com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool.<host>.requestExceptions``
- ``com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool.<host>.requests``
- ``com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool.requestConnectionExceptions``
- ``com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool.requestExceptions``
- ``com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool.requests``
- ``com.palantir.atlasdb.transaction.api.LockAwareTransactionManager.runTaskReadOnly.failures``
- ``com.palantir.atlasdb.transaction.api.LockAwareTransactionManager.runTaskReadOnly.failures.java.lang.IllegalArgumentException``
- ``com.palantir.atlasdb.transaction.api.LockAwareTransactionManager.runTaskWithRetry.failures``
- ``com.palantir.atlasdb.transaction.api.LockAwareTransactionManager.runTaskWithRetry.failures.feign.FeignException``
- ``com.palantir.atlasdb.transaction.api.LockAwareTransactionManager.runTaskWithRetry.failures.java.lang.IllegalArgumentException``
- ``com.palantir.atlasdb.transaction.api.LockAwareTransactionManager.runTaskWithRetry.failures.java.lang.IllegalStateException``

**Timers**

- ``com.palantir.atlasdb.transaction.api.LockAwareTransactionManager.getLockService``
- ``com.palantir.atlasdb.transaction.api.LockAwareTransactionManager.runTaskReadOnly``
- ``com.palantir.atlasdb.transaction.api.LockAwareTransactionManager.runTaskWithRetry``
- ``com.palantir.atlasdb.transaction.impl.SnapshotTransaction.commitAcquireLocks``
- ``com.palantir.atlasdb.transaction.impl.SnapshotTransaction.commitCheckingForConflicts``
- ``com.palantir.atlasdb.transaction.impl.SnapshotTransaction.commitPutCommitTs``
- ``com.palantir.atlasdb.transaction.impl.SnapshotTransaction.commitTotalTimeSinceTxCreation``
- ``com.palantir.atlasdb.transaction.impl.SnapshotTransaction.commitWrite``
- ``com.palantir.atlasdb.transaction.impl.SnapshotTransaction.getRows``
- ``com.palantir.atlasdb.transaction.impl.SnapshotTransaction.millisForPunch``
- ``com.palantir.atlasdb.transaction.impl.SnapshotTransaction.processedRangeMillis``
- ``com.palantir.atlasdb.transaction.impl.SnapshotTransaction.transactionMillis``
- ``com.palantir.atlasdb.transaction.impl.SnapshotTransaction.waitForCommitTsMillis``
