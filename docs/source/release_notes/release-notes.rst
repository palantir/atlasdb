.. _change-log:

*********
Changelog
*********

.. role:: changetype
.. role:: changetype-breaking
    :class: changetype changetype-breaking
.. role:: changetype-new
    :class: changetype changetype-new
.. role:: changetype-fixed
    :class: changetype changetype-fixed
.. role:: changetype-changed
    :class: changetype changetype-changed
.. role:: changetype-improved
    :class: changetype changetype-improved
.. role:: changetype-deprecated
    :class: changetype changetype-deprecated

.. |userbreak| replace:: :changetype-breaking:`USER BREAK`
.. |devbreak| replace:: :changetype-breaking:`DEV BREAK`
.. |new| replace:: :changetype-new:`NEW`
.. |fixed| replace:: :changetype-fixed:`FIXED`
.. |changed| replace:: :changetype-changed:`CHANGED`
.. |improved| replace:: :changetype-improved:`IMPROVED`
.. |deprecated| replace:: :changetype-deprecated:`DEPRECATED`

.. toctree::
  :hidden:

========
v0.61.12
========

.. replace this with the release date

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Fixed a bug in LockServiceImpl (caused by a bug in AbstractQueuedSynchronizer) where a race condition could cause a lock to become stuck indefinitely.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2799>`__)

=======
develop
=======

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |new|
         - Oracle SE will now automatically trigger shrinking table data post sweeping a table to recover space.
           You can disable the compaction by setting ``enableShrinkOnOracleStandardEdition`` to ``false`` in the Oracle DDL config.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2286>`__)

    *    - |improved|
         - Refactored ``AvailableTimestamps`` reducing overzealous synchronization. Giving out timestamps is no longer blocking on refreshing the timestamp bound if there are enough timestamps to give out with the current bound.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1783>`__)

    *    - |improved|
         - The lock server now logs to ``SlowLockLogger`` logger if a request takes more than a given time (10 seconds by default) to be processed.
           Specifically, the timelock server has a configuration parameter ``slowLockLogTriggerMillis`` which defaults to ``10000``.
           Setting this parameter to zero (or any negative number) will disable the new logger; slow locks will instead be logged at ``DEBUG``.
           If not using timelock, an application can modify the trigger value through ``LockServerOptions`` during initialization in ``TransactionManagers.create``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1791>`__)

    *    - |deprecated|
         - Deprecated ``InMemoryAtlasDbFactory#createInMemoryTransactionManager``, please instead use the supported ``TransactionManagers.createInMemory(...)`` for your testing.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1784>`__)

    *    - |fixed|
         - AtlasDB HTTP clients now parse ``Retry-After`` headers correctly.
           This manifests as clients failing over and trying other nodes when receiving a 503 with a ``Retry-After`` header from a remote (e.g. from a TimeLock non-leader).
           Previously, clients would immediately retry the connection on the node with a 503 two times (for a total of three attempts) before failing over.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1782>`__)

    *    - |improved|
         - Improved performance of getRange() on DbKvs. Range requests are now done with a single round trip to the database.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1805>`__)
    
    *    - |improved|
         - The lock server now will dump all held locks and outstanding lock requests in YAML file, when logging state requested, for easy readability and further processing.
           This will make debuging lock congestions easier. Lock descriptors are changed with places holders and can be decoded using descriptors file,
           which will be written in the folder. Information like requesting clients, requesting threads and other details can be found in the YAML.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1792>`__)

    *    - |new|
         - The ``atlasdb-config`` project now shadows the ``error-handling`` and ``jackson-support`` libraries from `http-remoting <https://github.com/palantir/http-remoting>`__.
           This will be used to handle exceptions in a future release, and was done in this way to avoid causing dependency issues in upstream products.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1796>`__)

    *    - |userbreak|
         - AtlasDB will refuse to start if backed by Postgres 9.5.0 or 9.5.1. These versions contain a known bug that causes incorrect results to be returned for certain queries.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1820>`__)

    *    - |new|
         - Atlas Console tables now have a join() method.  See ``help("join")`` in Atlas Console for more details.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1814>`__)

    *    - |improved|
         - Added logging for leadership election code,
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3294>`__)

    *    - |fixed|
         - The sweep CLI will no longer perform in-process compactions after sweeping a table.
           For DbKvs, this operation is handled by the background compaction thread; Cassandra performs its own compactions.
           Note that the sweep CLI itself has been deprecated in favour of using the sweep priority override configuration, possibly in conjunction with the thread count (:ref:`Docs<sweep_tunable_parameters>`).
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3369>`__)

=======
v0.38.0
=======

6 Apr 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - The default ``sweepBatchSize`` has been changed from 1000 to 100.
           This has empirically shown to be a better batch size because it puts less stress on the underlying KVS.
           For a full list of tunable sweep parameters and default settings, see :ref:`sweep tunable options <sweep_tunable_parameters>`.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1763>`__)

    *    - |fixed|
         - Reverted `#1524 <https://github.com/palantir/atlasdb/pull/1524>`__, which caused dependency issues in upstream products.
           Once we have resolved these issues, we will reintroduce the change, which was originally part of AtlasDB 0.37.0.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1775>`__)

    *    - |fixed|
         - Creating a postgres table with a long name now throws a ``RuntimeException`` if the truncated name (first sixty characters) is the same as that of a different existing table.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1729>`__)

    *    - |fixed|
         - Fixed a performance regression introduced in `#582 <https://github.com/palantir/atlasdb/pull/582>`__, which caused sub-optimal batching behaviour when getting large sets of rows in Cassandra.
           The benchmark, intentionally set up in `#1770 <https://github.com/palantir/atlasdb/pull/1770>`__ to highlight the break, shows a 10x performance improvement.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1764>`__)

    *    - |fixed|
         - Correctness issue fixed in the ``clean-transactions-range`` CLI. This CLI is responsible for deleting potentially inconsistent transactions in the KVS upon restore from backup.
           The CLI was not reading the entire ``_transactions`` table, and as a result missed deleting transactions that started before and committed after.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1759>`__)

    *    - |devbreak|
         - The ``atlasdb-remoting`` project was removed. We don't believe this was used anywhere, but if you encounter any problems due to the project having being removed, please contact AtlasDB support.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1750>`__)

    *    - |new|
         - ``InMemoryAtlasDbFactory`` now supports creating an in-memory transaction manager with multiple schemas.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1774>`__)

    *    - |improved|
         - Timelock users who start an embedded timestamp and lock service without :ref:`reverse-migrating <timelock-reverse-migration>` now encounter a more informative error message.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1755>`__)

=======
v0.37.0
=======

Removed 6 Apr 2017 due to dependency issues. Please use 0.38.0 instead.

Released 29 Mar 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Fixed an issue where a ``MultipleRunningTimestampServicesError`` would not be propagated from the asynchronous refresh job that increases the timestamp bound.
           This could result in a state where two timestamp services are simultaneously handing out timestamps until the older service's buffer of 1M timestamps is exhausted and fails.
           Now we immediately fail, alerting users much sooner that a ``MultipleRunningTimestampServicesError`` has occurred.
           Note that users would still see the error prior to the fix, we now just ensure it is discovered sooner.
           This failure does not affect the Timelock server.
           Furthermore, we improved the logic for increasing the timestamp bound when the allocation buffer is exhausted.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1718>`__)

    *    - |new|
         - Added :ref:`Dropwizard metrics <dropwizard-metrics>` for sweep, exposing aggregate and table-specific counts of cells examined and stale values deleted.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1695>`__)

    *    - |new|
         - Added a benchmark ``TimestampServiceBenchmarks`` for parallel requesting of fresh timestamps from the TimestampService.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1720>`__)

    *    - |fixed|
         - KVS migrations now maintain the guarantee of the timestamp service to hand out monotonically increasing timestamps.
           Previously, we would reset the timestamp service to 0 after a migration, but now we use the correct logical timestamp.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1199>`__)

    *    - |improved|
         - Improved performance of paging over dynamic columns on Oracle DBKVS: the time required to page through a large wide row is now linear rather than quadratic in the length of the row.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1702>`__)

    *    - |deprecated|
         - ``GenericStreamStore.loadStream`` has been deprecated.
           Use ``loadSingleStream``, which returns an ``Optional<InputStream>``, instead.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1265>`__)

    *    - |devbreak|
         - ``getAsyncRows`` and ``getAsyncRowsMultimap`` methods have been removed from generated code.
           They do not appear valuable to the API and use an unintuitive and custom ``AsyncProxy`` that was also removed.
           We believe they are unused by upstream applications, but if you do encounter breaks due to this removal please file a ticket with the dev team for immediate support.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1689>`__)

    *    - |fixed|
         - RemoteLockService clients will no longer silently retry on connection failures to the Timelock server.
           This is used to mitigate issues with frequent leadership changes owing to `#1680 <https://github.com/palantir/atlasdb/issues/1680>`__.
           Previously, because of Jetty's idle timeout and OkHttp's silent connection retrying, we would generate an endless stream of lock requests if using HTTP/2 and blocking for more than the Jetty idle timeout for a single lock.
           This would lead to starvation of other requests on the TimeLock server, since a lock request blocked on acquiring a lock consumes a server thread.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1727>`__)

    *    - |improved|
         - Cassandra dependencies have been bumped to newer versions.
           This should fix a bug (`#1654 <https://github.com/palantir/atlasdb/issues/1654>`__) that caused
           AtlasDB probing downed Cassandra nodes every few minutes to see if they were up and working yet to eventually take out the entire cluster by steadily
           building up leaked connections, due to a bug in the underlying driver.

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.36.0
=======

15 Mar 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Fixed DBKVS sweep OOM issue (`#982 <https://github.com/palantir/atlasdb/issues/982>`__) caused by very wide rows.
           ``DbKvs.getRangeOfTimestamps`` uses an adjustable cell batch size to avoid loading too many timestamps.
           One can set the batch size by calling ``DbKvs.setMaxRangeOfTimestampsBatchSize``.

           In case of a single row that is too wide, this may result in ``getRangeOfTimestamps`` returning multiple ``RowResult`` to include all timestamps.
           It is, however, guaranteed that each ``RowResult`` will contain all timestamps for each included column.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1678>`__)

    *    - |fixed|
         - Actions run by the ``ReadOnlyTransactionManager`` can no longer bypass necessary protections when using ``getRowsColumnRange()``.
           These protections disallow reads against ``THOROUGH`` swept tables as read only transactions do not acquire the appropriate locks to guarantee transactionality.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1521>`__)

    *    - |fixed|
         - Fixed an unnecessarily long-held connection in Oracle table name mapping code.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1593>`__)

    *    - |fixed|
         - Fixed an issue where we excessively log after successful transactions.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1687>`__)

    *    - |fixed|
         - Fixed an issue where the ``_persisted_locks`` table was unnecessarily logged as not having persisted metadata.
           The ``_persisted_locks`` table is a hidden table, and thus it does not need to have persisted metadata.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1696>`__)

    *    - |new|
         - AtlasDB now instruments services to expose aggregate response time and service call metrics for keyvalue, timestamp, and lock services.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1685>`__)

    *    - |devbreak| |improved|
         - ``TransactionManager`` now explicitly declares a ``close`` method that does not throw exceptions.
           This makes the ``TransactionManager`` significantly easier to develop against.
           Clients who have implemented a concrete ``TransactionManager`` throwing checked exceptions are encouraged to wrap said exceptions as unchecked exceptions.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1677>`__)

    *    - |new|
         - Added the following benchmarks for paging over columns of a very wide row:

             - ``TransactionGetRowsColumnRangeBenchmarks``
             - ``KvsGetRowsColumnRangeBenchmarks``

           (`Pull Request <https://github.com/palantir/atlasdb/pull/1684>`__)

    *    - |deprecated|
         - The public ``PaxosLeaderElectionService`` constructor is now deprecated to mitigate risks of users supplying parameters in the wrong order.
           ``PaxosLeaderElectionServiceBuilder`` should be used instead.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1681>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.35.0
=======

3 Mar 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - Timelock server now specifies minimum and maximum heap size of 512 MB.
           This should improve GC performance per the comments in `#1594 <https://github.com/palantir/atlasdb/pull/1594#discussion_r102255336>`__.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1647>`__)

    *    - |fixed|
         - The background sweeper now uses deleteRange instead of truncate when clearing the ``sweep.progress`` table.
           This allows users with Postgres to perform backups via the normal pg_dump command while running background sweep.
           Previous it was possible for a backup to fail if sweep were performing a truncate at the same time.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1616>`__)

    *    - |improved|
         - Cassandra now attempts to truncate when performing a ``deleteRange(RangeRequest.All())`` in an effort to build up less garbage.
           This is relevant for when sweep is operating on its own sweep tables.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1617>`__)

    *    - |new|
         - Users can now create a Docker image and run containers of the Timelock Server, by running ``./gradlew timelock-server:dockerTag``.
           This can be useful for quickly spinning up a Timelock instance (e.g. for testing purposes).
           Note that we are not yet publishing this image.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1661>`__)

    *    - |fixed|
         - AtlasDB :ref:`CLIs <clis>` run via the :ref:`Dropwizard bundle <dropwizard-bundle>` can now work with a Timelock block, and will contact the relevant Timelock server for timestamps or locks in this case.
           Previously, these CLIs would throw an error that a leader block was not specified.
           Note that CLIs will not perform automated migrations.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1661>`__)

    *    - |improved|
         - Cassandra truncates that are going to fail will do so faster.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1660>`__)

    *    - |devbreak|
         - The persistent lock endpoints now use ``PersistentLockId`` instead of ``LockEntry``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1665>`__)

    *    - |fixed|
         - The ``CheckAndSetException`` now gets mapped to the correct response for compatibility with `http-remoting <https://github.com/palantir/http-remoting>`__.
           Previously, any consumer using http-remoting would have to deal with deserialization errors.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1665>`__)

    *    - |devbreak|
         - The persistent lock release endpoint has now been renamed to ``releaseBackupLock`` since it is currently only supposed to be used for the backup lock.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1674>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.34.0
=======

23 Feb 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |new|
         - Timelock server now supports `HTTP/2 <https://http2.github.io/>`__, and the AtlasDB HTTP clients enable a required GCM cipher suite.
           This feature improves performance of the Timelock server.
           Any client that wishes to connect to the timelock server via HTTP/2 must add jetty_alpn_agent as a javaAgent JVM argument, otherwise connections will fall back to HTTP/1.1 and performance will be considerably slower.

           For an example of how to add this dependency, see our `timelock-server/build.gradle <https://github.com/palantir/atlasdb/pull/1594/files#diff-e7db4468f37a8004be3c399d791c323eR57>`__ file.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1594>`__)

    *    - |fixed|
         - AtlasDB :ref:`Perf CLI <perf-cli>` can now output KVS-agnostic benchmark data (such as ``HttpBenchmarks``) to a file.
           Previously running these benchmarks whilst attempting to write output to a file would fail.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1635>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.33.0
=======

22 Feb 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - AtlasDB HTTP clients are now compatible with OkHttp 3.3.0+, and no longer assume that header names are specified in Train-Case.
           This fix enables the Timelock server and AtlasDB clients to use HTTP/2.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1613>`__)

    *    - |fixed|
         - Canonicalised SQL strings will now have contiguous whitespace rendered as a single space as opposed to the first character of said whitespace.
           This is important for backwards compatibility with an internal product.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1603>`__)

    *    - |new|
         - Added the option to perform a dry run of sweep via the :ref:`Sweep CLI <atlasdb-sweep-cli>`.
           When ``--dry-run`` is set, sweep will tell you how many cells would have been deleted, but will not actually delete any cells.

           This feature was introduced to avoid accidentally generating more tombstones than the Cassandra tombstone threshold (default 100k) introduced in `CASSANDRA-6117 <https://issues.apache.org/jira/browse/CASSANDRA-6117>`__.
           If you delete more than 100k cells and thus cross the Cassandra threshold, then Cassandra may reject read requests until the tombstones have been compacted away.
           Customers wishing to run Sweep should first run with the ``--dry-run`` option and only continue if the number of cells to be deleted is fewer than 100k.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1598>`__)

    *    - |fixed|
         - Fixed atlasdb-commons Java 1.6 compatibility by removing tracing from ``InterruptibleProxy``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1599>`__)

    *    - |fixed|
         - Persisted locks table is now considered an Atomic Table.

           ``ATOMIC_TABLES`` are those that must always exist on KVSs that support check-and-set (CAS) operations.
           This is particularly relevant for AtlasDB clients that make use of the TableSplittingKVS and want to keep tables on different KVSs.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1610>`__)

    *    - |fixed|
         - Reverted PR #1577 in 0.32.0 because this change prevents AtlasDB clients from downgrading to earlier versions of AtlasDB.
           We will merge a fix for MRTSE once we have a solution that allows a seamless rollback process.
           This change is also reverted on 0.32.1.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1622>`__)

    *    - |improved|
         - Reduced contention on ``PersistentTimestampService.getFreshTimestamps`` to provide performance improvements to the Timestamp service under heavy request load.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1618>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.32.1
=======

21 Feb 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Reverted PR #1577 in 0.32.0 because this change prevents AtlasDB clients from downgrading to earlier versions of AtlasDB.
           We will merge a fix for MRTSE once we have a solution that allows a seamless rollback process.
           This change is also reverted on develop.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1622>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.32.0
=======

16 Feb 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Fixed erroneous occurrence of ``MultipleRunningTimestampServicesError`` (see `#1000 <https://github.com/palantir/atlasdb/issues/1000>`__) where the timestamp service was unaware of successfully writing the new timestamp limit to the DB.
           This fix only applies to Cassandra backed AtlasDB clients who are not using the external Timelock service.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1577>`__)

    *    - |improved|
         - AtlasDB HTTP clients will now have a user agent of ``<project.name>-atlasdb (project.version)`` as opposed to ``okhttp/2.5.0``.
           This should make distinguishing AtlasDB request logs from application request logs much easier.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1535>`__)

    *    - |new|
         - Sweep now takes out a lock to ensure data is not corrupted during online backups.

           Users performing :ref:`live backups <backup-restore>` should grab this lock before performing a backup of the underlying KVS, and then release the lock once the backup is complete.
           This enables the backup to safely run alongside either the :ref:`background sweeper <background-sweep>` or the :ref:`sweep CLI <atlasdb-sweep-cli>`.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1509>`__)

    *    - |new|
         - Initial support for tracing Key Value Services integrating with `http-remoting tracing <https://github.com/palantir/http-remoting#tracing>`__.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1385>`__)

    *    - |improved|
         - Improved heap usage during heavy DBKVS querying by reducing mallocs in ``SQLString.canonicalizeString()``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1560>`__)

    *    - |improved|
         - Removed an unused hamcrest import from the timestamp-impl project.
           This should reduce the size of our transitive dependencies, and therefore the size of product binaries.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1578>`__)

    *    - |fixed|
         - Fixed schema generation with Java 8 optionals.
           To use Java8 optionals, supply ``OptionalType.JAVA8`` as an additional constructor argument when creating your ``Schema`` object.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1501>`__)

    *    - |devbreak|
         - Modified the type signature of ``BatchingVisitableView#of`` to no longer accept ``final BatchingVisitable<? extends T> underlyingVisitable`` and instead accept ``final BatchingVisitable<T> underlyingVisitable``.
           This will resolve an issue where newer versions of Intellij fail to compile AtlasDB.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1582>`__)

    *    - |improved|
         - Reduced logging noise from large Cassandra gets and puts by removing ERROR messages and only providing stacktraces at DEBUG.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1590>`__)

    *    - |new|
         - Upon startup of an AtlasDB client with a ``timeblock`` :ref:`config block <timelock-client-configuration>`, the client will now automatically migrate its timestamp to the the :ref:`external Timelock cluster <external-timelock-service>`.

           The client will fast-forward the Timelock Server's timestamp bound to that of the embedded service.
           The client will now also *invalidate* the embedded service's bound, backing this up in a separate row in the timestamp table.

           Automated migration is only supported for Cassandra KVS at the moment.
           If using DBKVS or other key-value services, it remains the user's responsibility to ensure that they have performed the migration detailed in :ref:`Migration to External Timelock Services <timelock-migration>`.
           (`Pull Request 1 <https://github.com/palantir/atlasdb/pull/1569>`__,
           `Pull Request 2 <https://github.com/palantir/atlasdb/pull/1570>`__, and
           `Pull Request 3 <https://github.com/palantir/atlasdb/pull/1579>`__)

    *    - |fixed|
         - Fixed multiple scenarios where DBKVS can run into deadlocks due to unnecessary connections.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1566>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.31.0
=======

8 Feb 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved| |devbreak|
         - Improved Oracle performance on DBKVS by preventing excessive reads from the _namespace table when initializing SweepStrategyManager.
           Replaced ``mapToFullTableNames()`` with ``generateMapToFullTableNames()`` in ``com.palantir.atlasdb.keyvalue.TableMappingService``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1486>`__)

    *    - |devbreak|
         - Removed the unused ``TieredKeyValueService`` which offered the ability to spread tables across multiple KVSs that exist in a stacked hierarchy (primary & secondary).
           If you require this KVS please file a ticket to have it reinstated.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1553>`__)

    *    - |devbreak|
         - Fast forwarding a persistent timestamp service to ``Long.MIN_VALUE`` will now throw an exception, whereas previously it would be a no-op.
           Calling the ``fast-forward`` endpoint without specifying the fast-forward timestamp parameter will now default to submitting ``Long.MIN_VALUE``, and thus return a HTTP 400 response.

           We are introducing this break to prevent accidental corruption by forgetting to submit the fast-forward timestamp.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1538>`__)

    *    - |fixed|
         - Oracle queries now use the correct hints when generating the query plan.
           This will improve performance for Oracle on DB KVS.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1549>`__)

    *    - |userbreak|
         - Oracle table names can now have a maximum length of 27 characters instead of the previous limit of 30.
           This is to ensure consistency in naming the primary key constraint which adds a prefix of ``pk_`` to the table name.
           This will break any installation of Oracle with the ``useTableMapping`` flag set to ``true``.

           Since Oracle support is still in beta, we are not providing an automatic migration path from older versions of AtlasDB.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1552>`__)

    *    - |fixed|
         - Support for Oracle 12c batch responses.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1540>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.30.0
=======

27 Jan 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed| |devbreak|
         - Fixed schema generation with Java 8 optionals.
           To use Java8 optionals, supply ``OptionalType.JAVA8`` as an additional constructor argument when creating your ``Schema`` object.

           Additionally, this fix requires all AtlasDB clients to regenerate their schema, even if they do not use the Java 8 optionals.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1501>`__)

    *    - |fixed|
         - Prevent deadlocks in an edge case where we perform parallel reads with a small connection pool on DB KVS.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1468>`__)

    *    - |new|
         - Added support for benchmarking custom Key Value Stores.
           In the future this will enable performance regression testing for Oracle.

           See our :ref:`performance writing <performance-writing>` documentation for details.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1459>`__)

    *    - |improved|
         - Don't retry interrupted remote calls.

           This should have the effect of shutting down faster in situations where we receive a ``InterruptedException``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1488>`__)

    *    - |improved|
         - Added request and exception rates metrics in CassandraClientPool. This will provide access to 1-, 5-, and 15-minute moving averages.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1497>`__)

    *    - |improved|
         - More informative logging around retrying of transactions.
           If a transaction succeeds after being retried, we log the success (at the INFO level).
           If a transaction failed, but will be retried, we now also log the number of failures so far (at INFO).
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1376>`__)

    *    - |improved|
         - Updated our dependency on ``gradle-java-distribution`` from 1.2.0 to 1.3.0.
           See gradle-java-distribution `release notes <https://github.com/palantir/gradle-java-distribution/releases>`__ for details.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1500>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.29.0
=======

17 Jan 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |new|
         - Returned ``RemotingKeyValueService`` and associated remoting classes to the AtlasDB code base.
           These now live in ``atlasdb-remoting``.
           This KVS will pass remote calls to a local delegate KVS.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1304>`__)

    *    - |fixed|
         - Stream store compression, introduced in 0.27.0, no longer creates a transaction inside a transaction when streaming directly to a file.
           Additionally, a check was added to enforce the condition imposed in 0.28.0, namely that the caller of ``AbstractGenericStreamStore.loadStream`` should not call ``InputStream.read()`` within the transaction that was used to fetch the stream.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1460>`__)

    *    - |improved|
         - AtlasDB timestamp and lock HTTPS communication now use JVM optimized cipher suite CBC over the slower GCM.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1378>`__)

    *    - |new|
         - Added a new ``KeyValueService`` API method, ``checkAndSet``.
           This is to be used in upcoming backup lock changes, and is not intended for other usage. If you think your application would benefit from using this directly, please contact the AtlasDB dev team.
           This is supported for Cassandra, Postgres, and Oracle, but in the latter case support is only provided for tables which are not overflow tables.
           ``checkAndSet`` is **not** supported for RocksDB or JDBC.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1435>`__)

    *    - |fixed|
         - Reverted the ``devbreak`` in AtlasDB 0.28.0 by returning the ``DebugLogger`` to its original location.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1469>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.28.0
=======

13 Jan 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |devbreak|
         - The ``DebugLogger`` class was moved from package ``com.palantir.timestamp`` in project ``timestamp-impl`` to ``com.palantir.util`` in project ``atlasdb-commons``.
           This break is reverted in the next release (AtlasDB 0.29.0) and will not affect services who skip this release.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1445>`__)

    *    - |improved|
         - Increase default Cassandra pool size from minimum of 20 and maximum of 5x the minimum (100 if minimum not modified) connections to minimum of 30 and maximum of 100 connections.
           This has empirically shown better handling of bursts of requests that would otherwise require creating many new connections to Cassandra from the clients.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1402>`__)

    *    - |new|
         - Added metrics to SnapshotTransaction to monitor durations of various operations such as ``get``, ``getRows``, ``commit``, etc.
           AtlasDB users should use ``AtlasDbMetrics.setMetricRegistry`` to set a ``MetricRegistry``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1429>`__)

    *    - |new|
         - Added metrics in Cassandra clients to record connection pool statistics and exception rates.
           These metrics use the global ``AtlasDbRegistry`` metrics.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1380>`__)

    *    - |new|
         - There is now a ``TimestampMigrationService`` with the ``fast-forward`` method that can be used to migrate between timestamp services.
           You will simply need to fast-forward the new timestamp service using the latest timestamp from the old service.
           This can be done using the :ref:`timestamp forward cli <offline-clis>` when your AtlasDB services are offline.

           This capability was added so we can automate the migration to an external Timelock service in a future release.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1445>`__)

    *    - |fixed|
         - Allow tables declared with ``SweepStrategy.THOROUGH`` to be migrated during a KVS migration.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1410>`__)

    *    - |fixed|
         - Fix an issue with stream store where pre-loading the first block of an input stream caused us to create a transaction inside another transaction.
           To avoid this issue, it is now the caller's responsibility to ensure that ``InputStream.read()`` is not called within the transaction used to fetch the stream.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1447>`__)

    *    - |improved|
         - ``atlasdb-rocksdb`` is no longer required by ``atlasdb-cli`` and therefore will no longer be packaged with AtlasDB clients pulling in ``atlasdb-dropwizard-bundle``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1439>`__)

    *    - |fixed|
         - All SnapshotTransaction ``get`` methods are now safe for tables declared with SweepStrategy.THOROUGH.
           Previously, a validation check was omitted for ``getRowsColumnRange``, ``getRowsIgnoringLocalWrites``, and ``getIgnoringLocalWrites``, which in very rare cases could have resulted in deleted values being returned by a long-running read transaction.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1421>`__)

    *    - |userbreak|
         - Users must not create a client named ``leader``. AtlasDB Timelock Server will fail to start if this is found.
           Previously, using ``leader`` would have silently failed, since the JAXRS 3.7.2 algorithm does not include backtracking over
           root resource classes (so either leader election or timestamp requests would have failed).
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1442>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.27.2
=======

10 Jan 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Fixed an issue with ``StreamStore.loadStream``'s underlying ``BlockGetter`` where, for non-default block size and in-memory thresholds,
           we would incorrectly throw an exception instead of allowing the stream to be created.
           This caused an issue when the in-memory threshold was many times larger than the default (47MB for the default block size),
           or when the block size was many times smaller (7KB for the default in-memory threshold).
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1422>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.27.1
=======

6 Jan 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Fixed an edge case in stream stores where we throw an exception for using the exact maximum number of bytes in memory.
           This behavior was introduced in 0.27.0 and does not affect stream store usage pre-0.27.0.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1411>`__)

    *    - |improved|
         - Backoff when receiving a socket timeout to Cassandra to put back pressure on client and to spread out load incurred
           on remaining servers when a failover occurs.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1420>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.27.0
=======

6 Jan 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |new|
         - AtlasDB now supports stream store compression.
           Streams can be compressed client-side by adding the ``compressStreamInClient`` option to the stream definition.
           Reads from the stream store will transparently decompress the data.

           For information on using the stream store, see :ref:`schemas-streams`.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1357>`__)

    *    - |improved|
         - ``StreamStore.loadStream`` now actually streams data if it does not fit in memory.
           This means that getting the first byte of the stream now has constant-time performance, rather than
           linear in terms of stream length as it was previously.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1341>`__)

    *    - |improved|
         - Increased Cassandra connection pool idle timeout to 10 minutes, and reduced eviction check frequency to 20-30 seconds at 1/10 of connections.
           This should reduce bursts of stress on idle Cassandra clusters.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1336>`__)

    *    - |new|
         - There is a new configuration called ``maxConnectionBurstSize``, which configures how large the pool is able to grow when receiving a large burst of requests.
           Previously this was hard-coded to 5x the ``poolSize`` (which is now the default for the parameter).

           See :ref:`Cassandra KVS Config <cassandra-kvs-config>` for details on configuring AtlasDB with Cassandra.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1336>`__)

    *    - |improved|
         - Improved the performance of Oracle queries by making the table name cache global to the KVS level.
           Keeping the mapping in a cache saves one DB lookup per query, when the table has already been used.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1325>`__)

    *    - |fixed|
         - Oracle value style caching limited in scope to per-KVS, previously per-JVM, which could have in extremely rare cases caused issues for users in non-standard configurations.
           This would have caused issues for users doing a KVS migration to move from one Oracle DB to another.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1325>`__)

    *    - |new|
         - We now publish a runnable distribution of AtlasCli that is available for download directly from `Bintray <https://bintray.com/palantir/releases/atlasdb#files/com/palantir/atlasdb/atlasdb-cli-distribution>`__.
           (`Pull Request 1 <https://github.com/palantir/atlasdb/pull/1318>`__) and
           (`Pull Request 2 <https://github.com/palantir/atlasdb/pull/1345>`__)

    *    - |improved|
         - Enabled garbage collection logging for CircleCI builds.
           This may be useful for investigating pre-merge build failures.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1398>`__)

    *    - |Improved|
         - Updated our dependency on ``gradle-java-distribution`` from 1.0.1 to 1.2.0.
           See gradle-java-distribution `release notes <https://github.com/palantir/gradle-java-distribution/releases>`__ for details.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1361>`__)

    *     - |new|
          - Add KeyValueStore.deleteRange(); makes large swathes of row deletions faster,
            like transaction sweeping. Also can be used as a fallback option for people
            having issues with their backup solutions not allowing truncate() during a backup
            (`Pull Request <https://github.com/palantir/atlasdb/pull/1391>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.26.0
=======

5 Dec 2016

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - Added Javadocs to ``CassandraKeyValueService.java``, `documented <http://palantir.github.io/atlasdb/html/configuration/multinode_cassandra.html>`__ the behaviour of ``CassandraKeyValueService`` when one or more nodes in the Cassandra cluster are down.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1329>`__)

    *    - |improved|
         - Substantially improved performance of the DBKVS implementation of the single-iterator version of getRowsColumnRange.
           Two new performance benchmarks were added as part of this PR:

              - ``KvsGetRowsColumnRangeBenchmarks.getAllColumnsAligned``
              - ``KvsGetRowsColumnRangeBenchmarks.getAllColumnsUnaligned``

           These benchmarks show a 2x improvement on Postgres, and an AtlasDB client has observed an order of magnitude improvement experimentally.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1132>`__)

    *    - |improved|
         - OkHttpClient connection pool configured to have 100 idle connections with 10 minute keep-alive, reducing the number of connections that need to be created when a large number of transactions begin.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1294>`__)

    *    - |improved|
         - Commit timestamp lookups are now cached across transactions.
           This provided a near 2x improvement in our performance benchmark testing.
           See comments on the pull request for details.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1238>`__)

    *    - |improved|
         - ``LockAwareTransactionManager.runTaskWithLocksWithRetry`` now fails faster if given lock tokens that time out in a way that cannot be recovered from.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1322>`__)

    *    - |improved|
         - When we hit the ``MultipleRunningTimestampServicesError`` issue, we now automatically log thread dumps to a separate file (file path specified in service logs).
           The full file path of the ``atlas-timestamps-log`` file will be outputted to the service logs.
           (`Pull Request 1 <https://github.com/palantir/atlasdb/pull/1275>`__, `Pull Request 2 <https://github.com/palantir/atlasdb/pull/1332>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.25.0
=======

25 Nov 2016

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - ``--config-root`` and other global parameters can now be passed into dropwizard CLIs.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1268>`__)

    *    - |userbreak|
         - The migration ``--config-root`` shorthand (``-r``) can no longer be used as it conflicted with the timestamp command ``--row``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1268>`__)

    *    - |new|
         - Dbkvs: ConnectionSupplier consumers can now choose to receive a brand new unshared connection.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1258>`__)

    *    - |new|
         - AtlasDB now supports Cassandra 3.7 as well as Cassandra 2.2.8.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1206>`__)

    *    - |improved|
         - Oracle perf improvement; table names now cached, resulting in fewer round trips to the database.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1215>`__)

    *    - |improved|
         - ``SweepStatsKeyValueService`` will no longer flush a final batch of statistics during shutdown. This avoids
           potentially long pauses that could previously occur when closing a ``Cleaner``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1232>`__)

    *    - |improved|
         - Better support for AtlasDB clients running behind load balancers. In particular, if an AtlasDB client falls down and
           its load balancer responds with "503: Service Unavailable", the request will be attempted on other clients rather than aborting.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1175>`__)

    *    - |fixed|
         - Oracle will not drop a table that already exists on  ``createTable`` calls when multiple AtlasDB clients make the call to create the same table.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1243>`__)

    *    - |fixed|
         - Certain Oracle KVS calls no longer attempt to leak connections created internally.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1215>`__)

    *    - |fixed|
         - OracleKVS: ``TableSizeCache`` now invalidates the cache on table delete.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1272>`__)

    *    - |devbreak|
         - Our Jackson version has been updated from 2.5.1 to 2.6.7 and Dropwizard version from 0.8.2 to 0.9.3.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1209>`__)

    *    - |improved|
         - Additional debugging available for those receiving 'name must be no longer than 1500 bytes' errors.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1117>`__)

    *    - |devbreak|
         - ``Cell.validateNameValid`` is now private; consider ``Cell.isNameValid`` instead.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1117>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.24.0
=======

15 Nov 2016

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |userbreak|
         - All Oracle table names will be truncated and be of the form: ``<prefix>_<2-letter-namespace>__<table-name>_<5-digit-int>``.
           Previously we only truncated names that exceeded the character limit for Oracle table names.
           This should improve legibility as all table names for a particular application will have identical formatting.

           Oracle is in beta, and thus we have not built a migration path from old table names to new table names.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1187>`__)

    *    - |fixed|
         - The fetch timestamp CLI correctly handles ``--file`` inputs containing non-existent directories by creating any missing intermediate directories.
           Previously, the CLI would throw an exception and fail in such cases.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1151>`__)

    *    - |fixed|
         - When using DBKVS with Oracle, ``TableRemappingKeyValueService`` does not throw a RuntimeException when performing ``getMetaData`` and ``dropTable`` operations on a non-existent table.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1186>`__)

    *    - |fixed|
         - The KVS migration CLI will now decrypt encrypted values in your KVS configuration.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1171>`__)

    *    - |improved|
         - If using the Dropwizard command to run a KVS migration, the Dropwizard config will be used as the ``--migrateConfig`` config if none is specified.
           Running the KVS migration command as a deployable CLI still requires ``--migrateConfig``.

           See the :ref:`documentation <clis-migrate>` for details on how to use the KVS migration command.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1171>`__)

    *    - |fixed|
         - The timestamp bound store now works with Oracle as a relational backing store.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1224>`__)

    *    - |improved|
         - CLIs now output to standard out, standard error, and the service logs, rather than only printing to the service logs.
           This should greatly improve usability for service admins using the CLIs.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1177>`__)

    *    - |improved|
         - Remove usage of ``createUnsafe`` in generated Schema code. You can regenerate your schema to get rid of the deprecation warnings.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1194>`__)

    *    - |improved|
         - ``atlasdb-cassandra`` now depends on ``cassandra-thrift`` instead of ``cassandra-all``.
           Applications that support :ref:`CassandraKVS <cassandra-configuration>` will see a 20MB (10%) decrease in their Cassandra dependency footprint.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1222>`__)

    *    - |new|
         - Add support for generating schemas with Java8 Optionals instead of Guava Optionals.
           To use Java8 optionals, supply ``OptionalType.JAVA8`` as an additional constructor argument when creating your ``Schema`` object.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1162>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.23.0
=======

8 Nov 2016

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |devbreak|
         - All KVSs now as a guarantee throw a RuntimeException on attempts to truncate a non-existing table, so services should check the existence of a table before attempting to truncate.
           Previously we would only throw exceptions for the Cassandra KVS.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1114>`__)

    *    - |fixed|
         - The KVS :ref:`migration <clis-migrate>` command now supports the ``--offline`` flag and can be run as an offline CLI.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1149>`__)

    *    - |deprecated|
         - ``TableReference.createUnsafe`` is now deprecated to prevent mishandling of table names.
           ``createWithEmptyNamespace`` or ``createFromFullyQualifiedName`` should be used instead.

           Schema generated code still contains use of ``TableReference.createUnsafe`` and is being tracked for removal on `#1172 <https://github.com/palantir/atlasdb/issues/1172>`__.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1121>`__)

    *    - |new|
         - We now provide Oracle support (beta) for all valid schemas.
           Oracle table names exceeding 30 characters are now mapped to shorter names by truncating and appending a sequence number.
           Support for Oracle is currently in beta and services wishing to deploy against Oracle should contact the AtlasDB team.

           See :ref:`oracle_table_mapping` for details on how table names are mapped.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1076>`__)

    *    - |changed|
         - We now test against Cassandra 2.2.8, rather than Cassandra 2.2.7.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1112>`__)

    *    - |improved|
         - Added a significant amount of logging aimed at tracking down the ``MultipleRunningTimestampServicesError``.
           If clients are hitting this error, then they should add TRACE logging for ``com.palantir.timestamp``.
           These logs can also be directed to a separate file, see the :ref:`documentation <logging-configuration>` for more details.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1098>`__)

    *    - |improved|
         - Retrying a Cassandra operation now retries against distinct hosts.
           Previously, this would independently select hosts randomly, meaning that we might unintentionally try the same operation on the same servers.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1139>`__)

    *    - |fixed|
         - AtlasDB clients can start when a single Cassandra node is unreachable.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1045>`__).

    *    - |improved|
         - Removed spurious error logging during first-time startup against a brand new Cassandra cluster.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1033>`__)

    *    - |improved|
         - Improved the reliability of starting up against a degraded Cassandra cluster.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1033>`__)

    *    - |fixed|
         - No longer publish a spurious junit dependency in atlasdb-client compile.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1176>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.22.0
=======

28 Oct 2016

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - The ``clean-cass-locks-state`` CLI clears the schema mutation lock by setting it to a special "cleared" value in the same way that normal lockholders clear the lock.
           Previously the CLI would would drop the whole ``_locks`` table to clear the schema mutation lock.

           See :ref:`schema-mutation-lock` for details on how the schema mutation lock works.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1056>`__)

    *    - |fixed|
         - Fixed an issue where some locks were not being tracked for continuous refreshing due to one of the lock methods not being overridden by the ``LockRefreshingLockService``.
           This resulted in locks that appeared to be refreshed properly, but then would mysteriously time out at the end of a long-running operation.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1134>`__)

    *    - |improved|
         - Sweep no longer immediately falls back to a ``sweepBatchSize`` of 1 after receiving an error.

           See :ref:`sweep tuning <sweep_tunable_parameters>` documentation for more information on sweep tuning parameters.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1093>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.21.1
=======

24 Oct 2016

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Fixed a regression with Cassandra KVS where you could no longer create a table if it has the same name as another table in a different namespace.

           To illustrate the issue, assume you have namespace ``namespace1`` and the table ``table1``, and you would like to add a column to ``table1`` and `version` the table by using the new namespace ``namespace2``.
           On disk you already have the Cassandra table ``namespace1__table1``, and now you are trying to create ``namespace2__table1``.
           Creating ``namespace2__table1`` would fail because Cassandra KVS believes that the table already exists.
           This is relevant if you use multiple namespaces when performing schema migrations.

           Note that namespace is an application level abstraction defined as part of a AtlasDB schema and is not the same as Cassandra keyspace.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1110>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.21.0
=======

21 Oct 2016

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |new|
         - Sweep now supports batching on a per-cell level via the ``sweepCellBatchSize`` parameter in your AtlasDB config.
           This can decrease Sweep memory consumption on the client side if your tables have large cells or many columns (i.e. wide rows).
           For information on how to configure Sweep batching, see the :ref:`sweep documentation <atlasdb-sweep-cli>`.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1068>`__)

    *    - |fixed|
         - If ``hashFirstRowComponent()`` is used in a table or index definition, we no longer throw ``IllegalStateException`` when generating schema code.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1091>`__)


.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.20.0
=======

19 Oct 2016

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |devbreak|
         - Hotspotting warnings, previously logged at ERROR, will now throw ``IllegalStateException`` when generating your schema code.
           Products who hit this warning will need to add ``ignoreHotspottingChecks()`` to the relevant tables of their schema, or modify their schemas such that the first row component is not a VAR_STRING, a VAR_LONG, a VAR_SIGNED_LONG, or a SIZED_BLOB.

           See documentation on :ref:`primitive value types <primitive-valuetypes>` and :ref:`partitioners <tables-and-indices-partitioners>` for information on how to address your schemas.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/947>`__)

    *    - |fixed|
         - The AtlasDB Console included in the Dropwizard bundle can startup in an "online" mode, i.e. it can connect to a running cluster.

           See :ref:`AtlasDB Console <console>` for information on how to use AtlasDB console.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1063>`__)

    *    - |fixed|
         - The ``atlasdb-dagger`` project now publishes a shadowed version so we do not rely on the version of dagger on the classpath.
           This fixes the issue where running the CLIs would cause a ``ClassNotFoundException`` if your application also makes use of dagger.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1065>`__)

    *    - |new|
         - Oracle is supported via DBKVS if you have runtime dependency on an Oracle driver that resolves the JsonType "jdbcHandler".
           Due to an Oracle limitation, all table names in the schema must be less than 30 characters long.

           See :ref:`Oracle KVS Configuration <oracle-configuration>` for details on how to configure your service to use Oracle.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/985>`__)

    *    - |fixed|
         - The DBKVS config now enforces that the namespace must always be empty for ``metadataTable`` in the ``ddl`` block.
           The ``metadataTable`` parameter defaults to an empty name space, and if this was configured to be anything else previously, DBKVS would not start.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/985>`__)

    *    - |fixed|
         - We have changed the default ``tablePrefix`` for ``OracleDdlConfig`` to be ``a_``.
           Previously this would default to be empty and so user-defined tables could have a leading underscore, which is an invalid table name for Oracle.
           This change is specific to Oracle and does not affect DBKVS on Postgres.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/985>`__)

    *    - |fixed|
         - The ``metadataTableName`` for Oracle is now ``atlasdb_metadata`` instead of ``_metadata``.
           This is due to Oracle's restriction of not allowing table names with a leading underscore.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/985>`__)


.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.19.0
=======

11 Oct 2016

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change
    *    - |devbreak|
         - Removed KeyValueService ``initializeFromFreshInstance``, ``tearDown``, and ``getRangeWithHistory``.
           It is likely all callers of tearDown just want to call close, and getRangeWithHistory has been replaced with ``getRangeOfTimestamps``.
           Also removed Partitioning and Remoting KVSs, which were unused and had many unimplemented methods.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1041>`__)

    *    - |fixed|
         - In Cassandra KVS, we now no longer take out the schema mutation lock in calls to ``createTables`` if tables already exist.
           This fixes the issue that prevented the ``clean-cass-locks-state`` CLI from running correctly.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/991>`__)

    *    - |fixed|
         - Added a wait period before declaring someone dead based on lack of heartbeat.
           This will ensure we handle delayed heartbeats in high load situations (eg. on circleci).
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1035>`__)

    *    - |devbreak|
         - Removed the following classes and interfaces that appeared to be unused:
              - ``AbstractStringCollector``
              - ``BatchRowVisitor``
              - ``ChunkedRowVisitor``
              - ``CloseShieldedKeyValueService``
              - ``DBMgrConfigurationException``
              - ``IdGenerator``
              - ``ManyHostPoolingContainer``
              - ``MapCollector``
              - ``PalantirSequenceEnabledSqlConnection``
              - ``PalantirSqlConnectionRunner``
              - ``PaxosLearnerPersistence``
              - ``PaxosPingablePersistence``
              - ``PaxosProtos``
              - ``PostgresBlobs``
              - ``RowWrapper``
              - ``SqlConnectionImpl``
              - ``SqlStackLogWrapper``
              - ``StringCollector``
              - ``TLongQueue``

           Please reach out to us if you are adversely affected by these removals.
           (`Pull Request 1 <https://github.com/palantir/atlasdb/pull/1027>`__ and `Pull Request 2 <https://github.com/palantir/atlasdb/pull/1027>`__)

    *   - |changed|
        - The SQL connection manager will no longer temporarily increase the pool size by `eleven <https://github.com/palantir/atlasdb/pull/971/files#diff-f0027e21eb0fc2a30cf8b011cc0a1adbL358>`__ connections when the pool is exhausted.
          (`Pull Request <https://github.com/palantir/atlasdb/pull/971>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.18.0
=======

3 Oct 2016

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Fixed a bug introduced in 0.17.0, where products upgraded to 0.17.0 would see a "dead heartbeat" error on first start-up, requiring users to manually truncate the ``_locks`` table.
           Upgrading to AtlasDB 0.18.0 from any previous version will work correctly without requiring manual intervention.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1016>`__)

    *    - |fixed|
         - Dropping a table and then creating it again no longer adds an additional row to the ``_metadata`` table.
           Historical versions of the metadata entry before the most recent one are **not** deleted, so if you routinely drop and recreate the same table, you might consider :ref:`sweeping <sweep>` the ``_metadata`` table.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/946>`__)

    *    - |improved|
         - Users of DBKVS can now set arbitrary connection parameters.
           This is useful if, for example, you wish to boost performance by adjusting the default batch size for fetching rows from the underlying database.
           See the :ref:`documentation <postgres-configuration>` for how to set these parameters, and `the JDBC docs <https://jdbc.postgresql.org/documentation/head/connect.html>`__ for a full list.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1001>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.17.0
=======

28 Sept 2016

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - The schema mutation lock holder now writes a "heartbeat" to the database to indicate that it is still responsive.
           Other processes that are waiting for the schema mutation lock will now be able to see this heartbeat, infer that the lock holder is still working, and wait for longer.
           This should reduce the need to manually truncate the locks table.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/934>`__)

    *    - |new|
         - ``hashFirstRowComponent`` can now be used on index definitions to prevent hotspotting when creating schemas.
           For more information on using ``hashFirstRowComponent``, see the :ref:`Partitioners <tables-and-indices-partitioners>` documentation.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/986>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.16.0
=======

26 Sept 2016

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |devbreak|
         - Removed ``TransactionManager`` implementations ``ShellAwareReadOnlyTransactionManager`` and ``AtlasDbBackendDebugTransactionManager``.
           These are no longer supported by AtlasDB and products are not expected to use them.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/939>`__)

    *    - |improved|
         - ``TransactionMangers.create()`` now accepts ``LockServerOptions`` which can be used to apply configurations to the embedded LockServer instance running in the product.
           The other ``create()`` methods will continue to use ``LockServerOptions.DEFAULT``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/984>`__)

    *    - |fixed|
         - :ref:`Column paging Sweep <cassandra-sweep-config>` (in beta) correctly handles cases where table names have both upper and lowercase characters and cases where sweep is run multiple times on the same table.
           If you are using the regular implementation of Sweep (i.e. you do not specify ``timestampsGetterBatchSize`` in your AtlasDB config), then you are not affected.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/951>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.15.0
=======

14 Sept 2016

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - We have removed references to temp tables and no longer attempt to drop temp tables when aborting transactions.

           Temp tables are not currently being used by any KVSs, yet we were still calling ``dropTempTables()`` when we abort transactions.
           Since dropping tables is a schema mutation, this has the side effect of increasing the likelihood that we lose the schema mutation lock when there are many concurrent transactions.
           Removing temp tables entirely should reduce the need to manually truncate the locks table.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/916>`__)

    *    - |devbreak|
         - All TransactionManagers are now AutoCloseable and implement a close method that will free up the underlying resources.

           If your service implements a ``TransactionManager`` and does not extend ``AbstractTransactionManager``, you now have to add a close method to the implementation.
           No operations can be performed using the TransactionManager once it is closed.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/907>`__)

    *    - |new|
         - :ref:`AtlasDB Sweep <physical-cleanup-sweep>` now uses :ref:`column paging <cassandra-sweep-config>` via the ``timestampsGetterBatchSize`` parameter to better handle sweeping cells with many historical versions.

           By paging over historical versions of cells during sweeping, we can avoid out of memory exceptions in Cassandra when we have particularly large cells or many historical versions of cells.
           This feature is only implemented for Cassandra KVS and is disabled by default; please reach out to the AtlasDB dev team if you would like to enable it.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/834>`__)

    *    - |new|
         - Added a second implementation of ``getRowsColumnRange`` method which allows you to page through dynamic columns in a single iterator.
           This is expected to perform better than the previous ``getRowsColumnRange``, which allows you to page through columns per row with certain KVS stores (e.g. DB KVS).
           The new method should be preferred unless it is necessary to page through the results for different rows separately.

           Products or clients using wide rows should consider using ``getRowsColumnRange`` instead of ``getRows`` in ``KeyValueService``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/724>`__)

    *    - |new|
         - Added an :ref:`offline CLI <offline-clis>` called ``clean-cass-locks-state`` to truncate the locks table when the schema mutation lock has been lost.

           This is useful on Cassandra KVS if an AtlasDB client goes down during a schema mutation and does not release the schema mutation lock, preventing other clients from continuing.
           Previously an error message would direct users to manually truncate this table with CQL, but now this error message references the CLI.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/888>`__)

    *    - |changed|
         - Reverted our Dagger dependency from 2.4 to 2.0.2 and shadowed it so that it won't conflict with internal products.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/926>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.14.0
=======

8 Sept 2016

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |userbreak|
         - ``TransactionManagers.create()`` no longer takes in an argument of ``Optional<SSLSocketFactory> sslSocketFactory``.
           Instead, security settings between AtlasDB clients are now specified directly in configuration via the new optional parameter ``sslConfiguration`` located in the ``leader``, ``timestamp``, and ``lock`` blocks.
           Details can be found in the :ref:`Leader Configuration <leader-config>` documentation.

           To assist with back compatibility, we have introduced a helper method ``AtlasDbConfigs.addFallbackSslConfigurationToAtlasDbConfig``, which will add the provided ``sslConfiguration`` to ``config`` if the SSL configuration is not specified directly in the ``leader``, ``timestamp``, or ``lock`` blocks.
           (`Pull Request 1 <https://github.com/palantir/atlasdb/pull/873>`__ and `Pull Request 2 <https://github.com/palantir/atlasdb/pull/906>`__)

    *    - |fixed|
         - AtlasDB could startup with a leader configuration that is nonsensical, such as specifying both a ``leader`` block as well as a remote ``timestamp`` and ``lock`` blocks.
           AtlasDB will now fail to start if your configuration is invalid with a sensible message, per `#790 <https://github.com/palantir/atlasdb/issues/790>`__, rather than potentially breaking in unexpected ways.
           Please refer to :ref:`Example Leader Configurations <leader-config-examples>` for guidance on valid configurations.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/854>`__)

    *    - |fixed|
         - Fixed and standardized serialization and deserialization of AtlasDBConfig.
           This prevented CLIs deployed via the :ref:`Dropwizard bundle <dropwizard-bundle>` from loading configuration properly.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/875>`__)

    *    - |devbreak|
         - Updated our Dagger dependency from 2.0.2 to 2.4, so that our generated code matches with that of internal products.
           This also bumps our Guava dependency from 18.0 to 19.0 to accommodate a Dagger compile dependency.
           We plan on shading Dagger in the next release of AtlasDB, but products can force a Guava 18.0 runtime dependency to workaround the issue in the meantime.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/878>`__)


.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.13.0
=======

30 Aug 2016

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |devbreak|
         - ``AtlasDbServer`` has been renamed to ``AtlasDbServiceServer``.
           Any products that are using this should switch to using the standard AtlasDB java API instead.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/801>`__)

    *    - |fixed|
         - The method ``updateManyUnregisteredQuery(String sql)`` has been removed from the ``SqlConnection`` interface, as it was broken, unused, and unnecessary.
           Use ``updateManyUnregisteredQuery(String sql, Iterable<Object[] list>)`` instead.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/796>`__)

    *    - |improved|
         - Improved logging for schema mutation lock timeouts and added logging for obtaining and releasing locks.
           Removed the advice to restart the client, as it will not help in this scenario.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/805>`__)

    *    - |fixed|
         - Connections to Cassandra can be established over arbitrary ports.
           Previously AtlasDB clients would assume the default Cassandra port of 9160 despite what is specified in the :ref:`Cassandra keyValueService configuration <cassandra-kvs-config>`.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/771>`__)

    *    - |fixed|
         - Fixed an issue when starting an AtlasDB client using the Cassandra KVS where we always grab the schema mutation lock, even if we are not making schema mutations.
           This reduces the likelihood of clients losing the schema mutation lock and having to manually truncate the _locks table.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/771>`__)

    *    - |improved|
         - Performance and reliability enhancements to the in-beta CQL KVS.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/771>`__)


.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.12.0
=======

22 Aug 2016

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |userbreak|
         - AtlasDB will always try to register timestamp and lock endpoints for your application, whereas previously this only occurred if you specify a :ref:`leader-config`.
           This ensures that CLIs will be able to run against your service even in the single node case.
           For Dropwizard applications, this is only a breaking change if you try to initialize your KeyValueService after having initialized the Dropwizard application.
           Note: If you are initializing the KVS post-Dropwizard initialization, then your application will already fail when starting multiple AtlasDB clients.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/708>`__)

    *    - |new|
         - There is now a Dropwizard bundle which can be added to Dropwizard applications.
           This will add startup commands to launch the AtlasDB console and :ref:`CLIs <clis>` suchs as ``sweep`` and ``timestamp``, which is needed to perform :ref:`live backups <backup-restore>`.
           These commands will only work if the server is started with a leader block in its configuration.
           (`Pull Request 1 <https://github.com/palantir/atlasdb/pull/629>`__ and `Pull Request 2 <https://github.com/palantir/atlasdb/pull/696>`__)

    *    - |fixed|
         - DB passwords are no longer output as part of the connection configuration ``toString()`` methods.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/755>`__)

    *    - |new|
         - All KVSs now come wrapped with ProfilingKeyValueService, which at the TRACE level provides timing information per KVS operation performed by AtlasDB.
           See :ref:`logging-configuration` for more details.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/798>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.11.4
=======

29 Jul 2016

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Correctly checks the Cassandra client version that determines if Cassandra supports Check And Set operations.
           This is a critical bug fix that ensures we actually use our implementation from `#436 <https://github.com/palantir/atlasdb/pull/436>`__, which prevents data loss due to the Cassandra concurrent table creation bug described in `#431 <https://github.com/palantir/atlasdb/issues/431>`__.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/751>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.11.2
=======

29 Jul 2016

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |userbreak|
         - Reverting behavior introduced in AtlasDB 0.11.0 so the ``ssl`` property continues to take precedence over the ``sslConfiguration`` block to allow back-compatibility when using SSL with CassandraKVS.
           This means that products can add default truststore and keystore configuration to their AtlasDB config without overriding previously made SSL decisions (setting ``ssl: false`` should cause SSL to not be used).

           This only affects end users who have deployed products with AtlasDB 0.11.0 or 0.11.1; users upgrading from earlier versions will not see changed behavior.
           See :ref:`Communicating Over SSL <cass-config-ssl>` for details on how to configure CassandraKVS with SSL.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/745>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.11.1
=======

28 Jul 2016

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Removed a check enforcing a leader block config when one was not required.
           This prevents AtlasDB 0.11.0 clients from starting if a leader configuration is not specified (i.e. single node clusters).
           (`Pull Request <https://github.com/palantir/atlasdb/pull/741>`__)

    *    - |improved|
         - Updated schema table generation to optimize reads with no ColumnSelection specified against tables with fixed columns.
           To benefit from this improvement you will need to re-generate your schemas.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/713>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.11.0
=======

27 Jul 2016

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - Clarified the logging when multiple timestamp servers are running to state that CLIs could be causing the issue.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/719>`__)

    *    - |changed|
         - Updated cassandra client from 2.2.1 to 2.2.7 and cassandra docker testing version from 2.2.6 to 2.2.7.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/699>`__)

    *    - |fixed|
         - The leader config now contains a new ``lockCreator`` option, which specifies the single node that creates the locks table when starting your cluster for the very first time.
           This configuration prevents an extremely unlikely race condition where multiple clients can create the locks table simultaneously.
           Full details on the failure scenario can be found on `#444 <https://github.com/palantir/atlasdb/issues/444#issuecomment-221612886>`__.

           If left blank, ``lockCreator`` will default to the first host in the ``leaders`` list, but we recommend setting this explicitly to ensure that the lockCreater is the same value across all your clients for a specific service.
           This configuration is only relevant for new clusters and does not affect existing AtlasDB clusters.

           Full details for configuring the leader block, see `cassandra configuration <https://palantir.github.io/atlasdb/html/configuration/cassandra_KVS_configuration.html>`__.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/594>`__)

    *    - |fixed|
         - A utility method was removed in the previous release, breaking an internal product that relied on it.
           This method has now been added back.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/661>`__)

    *    - |fixed|
         - Removed unnecessary error message for missing _timestamp metadata table.
           _timestamp is a hidden table, and it is expected that _timestamp metadata should not be retrievable from public API.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/716>`__)

    *    - |improved|
         - Trace logging is more informative and will log all failed calls.
           To enable trace logging, see `Enabling Cassandra Tracing <https://palantir.github.io/atlasdb/html/configuration/enabling_cassandra_tracing.html#enabling-cassandra-tracing>`__.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/700>`__)

    *    - |new|
         - The Cassandra KVS now supports specifying SSL options via the new ``sslConfiguration`` block, which takes precedence over the now deprecated ``ssl`` property.
           The ``ssl`` property will be removed in a future release, and consumers leveraging the Cassandra KVS are encouraged to use the ``sslConfiguration`` block instead.
           See the `Cassandra SSL Configuration <https://palantir.github.io/atlasdb/html/configuration/cassandra_KVS_configuration.html#communicating-over-ssl>`__ documentation for more details.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/638>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.10.0
=======

13 Jul 2016

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |changed|
         - Updated HikariCP dependency from 2.4.3 to 2.4.7 to comply with updates in internal products.
           Details of the HikariCP changes can be found `here <https://github.com/brettwooldridge/HikariCP/blob/dev/CHANGES>`__.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/662>`__)

    *    - |new|
         - AtlasDB currently allows you to create dynamic columns (wide rows), but you can only retrieve entire rows or specific columns.
           Typically with dynamic columns, you do not know all the columns you have in advance, and this features allows you to page through dynamic columns per row, reducing pressure on the underlying KVS.
           Products or clients (such as AtlasDB Sweep) making use of wide rows should consider using ``getRowsColumnRange`` instead of ``getRows`` in ``KeyValueService``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/582>`__)

           Note: This is considered a beta feature and is not yet being used by AtlasDB Sweep.

    *    - |fixed|
         - We properly check that cells are not set to empty (zero-byte) or null.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/663>`__)

    *    - |improved|
         - Cassandra client connection pooling will now evict idle connections over a longer period of time and has improved logic for deciding whether or not a node should be blacklisted.
           This should result in less connection churn and therefore lower latency.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/667>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

======
v0.9.0
======

11 Jul 2016

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |devbreak|
         - Inserting an empty (size = 0) value into a ``Cell`` will now throw an ``IllegalArgumentException``. (`#156 <https://github.com/palantir/atlasdb/issues/156>`__) Likely empty
           values include empty strings and empty protobufs.

           AtlasDB cannot currently distinguish between empty and deleted cells. In previous versions of AtlasDB, inserting
           an empty value into a ``Cell`` would delete that cell. Thus, in this snippet,

           .. code-block:: java

               Transaction.put(table, ImmutableMap.of(myCell, new byte[0]))
               Transaction.get(table, ImmutableSet.of(myCell)).get(myCell)

           the second line will return ``null`` instead of a zero-length byte array.

           To minimize confusion, we explicitly disallow inserting an empty value into a cell by throwing an
           ``IllegalArgumentException``.

           In particular, this change will break calls to ``Transaction.put(TableReference tableRef, Map<Cell, byte[]> values)``,
           as well as generated code which uses this method, if any entry in ``values`` contains a zero-byte array. If your
           product does not need to distinguish between empty and non-existent values, simply make sure all the ``values``
           entries have positive length. If the distinction is necessary, you will need to explicitly differentiate the
           two cases (for example, by introducing a sentinel value for empty cells).

           If any code deletes cells by calling ``Transaction.put(...)`` with an empty array, use
           ``Transaction.delete(...)`` instead.

           *Note*: Existing cells with empty values will be interpreted as deleted cells, and will not lead to Exceptions when read.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/524>`__)

    *    - |improved|
         - The warning emitted when an attempted leadership election fails is now more descriptive.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/630>`__)

    *    - |fixed|
         - Code generation for the ``hashCode`` of ``*IdxColumn`` classes now uses ``deepHashCode`` for its arrays such that it returns
           consistent hash codes for use with hash-based collections (HashMap, HashSet, HashTable).
           This issue will only appear if you are instantiating columns in multiple places and storing columns in hash collections.

           If you are using `Indices <https://palantir.github.io/atlasdb/html/schemas/tables_and_indices.html#indices>`__ we recommend you upgrade as a precaution and ensure you are not relying on logic related to the ``hashCode`` of auto-generated ``*IdxColumn`` classes.
           You will need to regenerate your schema code in order to see this fix.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/600>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

======
v0.8.0
======

5 Jul 2016

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Some logging was missing important information due to use of the wrong substitution placeholder. This version should be taken in preference to 0.7.0 to ensure logging is correct.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/642>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

======
v0.7.0
======

4 Jul 2016

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |new|
         - AtlasDB can now be backed by Postgres via DB KVS. This is a very early release for this feature, so please contact us if you
           plan on using it. Please see :ref:`the documentation <postgres-configuration>` for more details.

    *    - |fixed|
         - The In Memory Key Value Service now makes defensive copies of any data stored or retrieved. This may lead to a slight performance degradation to users of In Memory Key Value Service.
           In Memory Key Value Service is recommended for testing environments only and production instances should use DB KVS or Cassandra KVS for data that needs to be persisted.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/552>`__)

    *    - |fixed|
         - AtlasDB will no longer log incorrect errors stating "Couldn't grab new token ranges for token aware cassandra mapping" when running against a single node and single token Cassandra cluster.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/634>`__)

    *    - |improved|
         - Read heavy workflows with Cassandra KVS will now use substantially less heap. In worst-case testing this change resulted in a 10-100x reduction in client side heap size.
           However, this is very dependent on the particular scenario AtlasDB is being used in and most consumers should not expect a difference of this size.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/568>`__)

.. <<<<------------------------------------------------------------------------------------------------------------>>>>

======
v0.6.0
======

26 May 2016

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *   - Type
        - Change

    *   - |fixed|
        - A potential race condition could cause timestamp allocation to never complete on a particular node (#462).

    *   - |fixed|
        - An innocuous error was logged once for each TransactionManager about not being able to allocate enough timestamps.
          The error has been downgraded to INFO and made less scary.

    *   - |fixed|
        - Serializable Transactions that read a column selection could consistently report conflicts when there were none.

    *   - |fixed|
        - An excessively long Cassandra related logline was sometimes printed (#501).

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

======
v0.5.0
======

16 May 2016

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *   - Type
        - Change

    *   - |changed|
        - Only bumping double minor version in artifacts for long-term stability fixes.

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

======
v0.4.1
======

17 May 2016

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *   - Type
        - Change

    *   - |fixed|
        - Prevent _metadata tables from triggering the Cassandra 2.x schema mutation bug `431 <https://github.com/palantir/atlasdb/issues/431>`_ (`444 <https://github.com/palantir/atlasdb/issues/444>`_ not yet fixed).

    *   - |fixed|
        - Required projects are now Java 6 compliant.


.. <<<<------------------------------------------------------------------------------------------------------------->>>>
