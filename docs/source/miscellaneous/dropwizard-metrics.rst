.. _dropwizard-metrics:

==================
Dropwizard Metrics
==================

AtlasDB makes use of the Dropwizard `Metrics library <http://metrics.dropwizard.io/>`__ to
expose a global ``MetricRegistry`` called ``AtlasDbRegistry``. Users of AtlasDB should use ``AtlasDbMetrics.setMetricRegistry``
to inject their own ``MetricRegistry`` for their application prior to initializing the AtlasDB transaction manager.

Each AtlasDB client will expose their own KeyValueService.<useragent> metrics, as well as CassandraClientPool metrics
for every Cassandra host.
We expose sweep metrics specific to every table that has been swept, as well as aggregate metrics.

For more details on what information each type of metric provides, we recommend reading
the Metrics `Getting Started Guide <http://metrics.dropwizard.io/3.1.0/getting-started/#>`__.

The full list of metrics exposed by AtlasDB can be found below.

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
 - ``com.palantir.atlasdb.cache.TimestampCache.startToCommitTimestamp.cache.estimated.size``
 - ``com.palantir.atlasdb.cache.TimestampCache.startToCommitTimestamp.cache.eviction.count``
 - ``com.palantir.atlasdb.cache.TimestampCache.startToCommitTimestamp.cache.hit.count``
 - ``com.palantir.atlasdb.cache.TimestampCache.startToCommitTimestamp.cache.hit.ratio``
 - ``com.palantir.atlasdb.cache.TimestampCache.startToCommitTimestamp.cache.load.average.millis``
 - ``com.palantir.atlasdb.cache.TimestampCache.startToCommitTimestamp.cache.load.failure.count``
 - ``com.palantir.atlasdb.cache.TimestampCache.startToCommitTimestamp.cache.load.success.count``
 - ``com.palantir.atlasdb.cache.TimestampCache.startToCommitTimestamp.cache.miss.count``
 - ``com.palantir.atlasdb.cache.TimestampCache.startToCommitTimestamp.cache.miss.ratio``
 - ``com.palantir.atlasdb.cache.TimestampCache.startToCommitTimestamp.cache.request.count``

**Histograms**

- ``com.palantir.atlasdb.transaction.impl.SnapshotTransaction.byteSizeTx``
- ``com.palantir.atlasdb.sweep.SweepMetrics.cellTimestampPairsExamined``
- ``com.palantir.atlasdb.sweep.SweepMetrics.staleValuesDeleted``
- ``com.palantir.atlasdb.sweep.SweepMetrics.cellTimestampPairsExamined.<table>``
- ``com.palantir.atlasdb.sweep.SweepMetrics.staleValuesDeleted.<table>``

**Meters**

- ``com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool.<host>.requestConnectionExceptions``
- ``com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool.<host>.requestExceptions``
- ``com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool.<host>.requests``
- ``com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool.requestConnectionExceptions``
- ``com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool.requestExceptions``
- ``com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool.requests``
- ``com.palantir.atlasdb.transaction.api.LockAwareTransactionManager.runTaskReadOnly.failures``
- ``com.palantir.atlasdb.transaction.api.LockAwareTransactionManager.runTaskWithRetry.failures``
- ``com.palantir.atlasdb.transaction.api.LockAwareTransactionManager.runTaskWithRetry.failures.<exception>``

Additional failure counts will be dynamically generated based on the returned exceptions, so you may see metrics like
the following:

- ``com.palantir.atlasdb.transaction.api.LockAwareTransactionManager.runTaskWithRetry.failures.java.lang.IllegalStateException``

**Timers**

- ``com.palantir.atlasdb.keyvalue.api.KeyValueService.<useragent>.addGarbageCollectionSentinelValues``
- ``com.palantir.atlasdb.keyvalue.api.KeyValueService.<useragent>.checkAndSet``
- ``com.palantir.atlasdb.keyvalue.api.KeyValueService.<useragent>.close``
- ``com.palantir.atlasdb.keyvalue.api.KeyValueService.<useragent>.compactInternally``
- ``com.palantir.atlasdb.keyvalue.api.KeyValueService.<useragent>.createTable``
- ``com.palantir.atlasdb.keyvalue.api.KeyValueService.<useragent>.createTables``
- ``com.palantir.atlasdb.keyvalue.api.KeyValueService.<useragent>.delete``
- ``com.palantir.atlasdb.keyvalue.api.KeyValueService.<useragent>.deleteRange``
- ``com.palantir.atlasdb.keyvalue.api.KeyValueService.<useragent>.dropTable``
- ``com.palantir.atlasdb.keyvalue.api.KeyValueService.<useragent>.dropTables``
- ``com.palantir.atlasdb.keyvalue.api.KeyValueService.<useragent>.get``
- ``com.palantir.atlasdb.keyvalue.api.KeyValueService.<useragent>.getAllTableNames``
- ``com.palantir.atlasdb.keyvalue.api.KeyValueService.<useragent>.getAllTimestamps``
- ``com.palantir.atlasdb.keyvalue.api.KeyValueService.<useragent>.getDelegates``
- ``com.palantir.atlasdb.keyvalue.api.KeyValueService.<useragent>.getFirstBatchForRanges``
- ``com.palantir.atlasdb.keyvalue.api.KeyValueService.<useragent>.getLatestTimestamps``
- ``com.palantir.atlasdb.keyvalue.api.KeyValueService.<useragent>.getMetadataForTable``
- ``com.palantir.atlasdb.keyvalue.api.KeyValueService.<useragent>.getMetadataForTables``
- ``com.palantir.atlasdb.keyvalue.api.KeyValueService.<useragent>.getRange``
- ``com.palantir.atlasdb.keyvalue.api.KeyValueService.<useragent>.getRangeOfTimestamps``
- ``com.palantir.atlasdb.keyvalue.api.KeyValueService.<useragent>.getRows``
- ``com.palantir.atlasdb.keyvalue.api.KeyValueService.<useragent>.getRowsColumnRange``
- ``com.palantir.atlasdb.keyvalue.api.KeyValueService.<useragent>.multiPut``
- ``com.palantir.atlasdb.keyvalue.api.KeyValueService.<useragent>.put``
- ``com.palantir.atlasdb.keyvalue.api.KeyValueService.<useragent>.putMetadataForTable``
- ``com.palantir.atlasdb.keyvalue.api.KeyValueService.<useragent>.putMetadataForTables``
- ``com.palantir.atlasdb.keyvalue.api.KeyValueService.<useragent>.putUnlessExists``
- ``com.palantir.atlasdb.keyvalue.api.KeyValueService.<useragent>.putWithTimestamps``
- ``com.palantir.atlasdb.keyvalue.api.KeyValueService.<useragent>.supportsCheckAndSet``
- ``com.palantir.atlasdb.keyvalue.api.KeyValueService.<useragent>.truncateTable``
- ``com.palantir.atlasdb.keyvalue.api.KeyValueService.<useragent>.truncateTables``
- ``com.palantir.atlasdb.transaction.impl.SnapshotTransaction.commitAcquireLocks``
- ``com.palantir.atlasdb.transaction.impl.SnapshotTransaction.commitCheckingForConflicts``
- ``com.palantir.atlasdb.transaction.impl.SnapshotTransaction.commitPutCommitTs``
- ``com.palantir.atlasdb.transaction.impl.SnapshotTransaction.commitTotalTimeSinceTxCreation``
- ``com.palantir.atlasdb.transaction.impl.SnapshotTransaction.commitWrite``
- ``com.palantir.atlasdb.transaction.impl.SnapshotTransaction.get``
- ``com.palantir.atlasdb.transaction.impl.SnapshotTransaction.getRows``
- ``com.palantir.atlasdb.transaction.impl.SnapshotTransaction.millisForPunch``
- ``com.palantir.atlasdb.transaction.impl.SnapshotTransaction.processedRangeMillis``
- ``com.palantir.atlasdb.transaction.impl.SnapshotTransaction.transactionMillis``
- ``com.palantir.atlasdb.transaction.impl.SnapshotTransaction.waitForCommitTsMillis``
