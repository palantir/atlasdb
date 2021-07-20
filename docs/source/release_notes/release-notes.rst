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
.. role:: changetype-logs
    :class: changetype changetype-logs
.. role:: changetype-metrics
    :class: changetype changetype-metrics
.. role:: strike
    :class: strike

.. |userbreak| replace:: :changetype-breaking:`USER BREAK`
.. |devbreak| replace:: :changetype-breaking:`DEV BREAK`
.. |new| replace:: :changetype-new:`NEW`
.. |fixed| replace:: :changetype-fixed:`FIXED`
.. |changed| replace:: :changetype-changed:`CHANGED`
.. |improved| replace:: :changetype-improved:`IMPROVED`
.. |deprecated| replace:: :changetype-deprecated:`DEPRECATED`
.. |logs| replace:: :changetype-logs:`LOGS`
.. |metrics| replace:: :changetype-metrics:`METRICS`

.. toctree::
  :hidden:

.. note::

   For versions of AtlasDB above v0.151.1, please refer to GitHub's
   `releases page <https://github.com/palantir/atlasdb/releases>`__ for release notes. This page is kept around as a
   document of changes up to v0.151.1 inclusive.

========
v0.151.1
========

2 Jul 2019

Identical to v0.151.0. This version was released owing to deployment infrastructure issues with v0.151.0.

========
v0.151.0
========

28 Jun 2019

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - Upgraded gradle-baseline to improve compile time static analysis checks.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3974>`__)

    *    - |devbreak|
         - ``KeyValueService`` implementations now have a new endpoint ``deleteRows()`` that allows row-level deletes to be performed.
           Users who extend ``KeyValueService`` will need to implement this method, but can refer to ``AbstractKeyValueService`` for a functional implementation (as this method can be written in terms of ``deleteRange()``).
           (`Pull Request <https://github.com/palantir/atlasdb/pull/4094>`__)

    *    - |improved|
         - Targeted Sweep now supports reading write information and dedicated row information from multiple partitions at a time; the same overall sweep batch limit is still respected.
           This is expected to be particularly relevant at installations where write information is sparsely distributed over many partitions (e.g. transactions2 deployments, especially such deployments with thorough tables).
           (`Pull Request <https://github.com/palantir/atlasdb/pull/4097>`__)

========
v0.150.0
========

20 Jun 2019

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - |new|
         - Added ability for timelock to rate limit targeted sweep lock requests to 2 per second. This is to reduce load on timelock for bad atlas clients.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/4096>`__)

    *    - |improved|
         - Relaxed concurrency model of ``MetricsManager`` allowing for more concurrency.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/4098>`__)


========
v0.149.0
========

11 Jun 2019

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - Added runtime configurable behavior to targeted sweep to allow multiple consecutive iterations on the same targeted sweep shard, all while holding the relevant lock. This option
           should improve targeted sweep throughput without adding additional load on Timelock for places where the downtime between targeted sweep iterations is a bottleneck.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/4086>`__)

========
v0.148.0
========

11 Jun 2019

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - The default value for the length of pause between targeted sweep iterations, ``pauseMillis``, has been changed back to 500ms.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/4084>`__)

========
v0.147.0
========

10 Jun 2019

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |devbreak|
         - Cassandra clients are now required to supply credentials in the KVS config.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/4076>`__)

========
v0.146.0
========

7 Jun 2019

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - Async initialization callbacks are run with an instrumented TransactionManager.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/4081>`__)

    *    - |improved|
         - If using DbKVS or JDBC KVS, we no longer spin up a background thread attempting to install new transaction schema versions.
           Furthermore, we now log at WARN if configuration indicates that a new schema version should be installed in these cases where it won't be respected.
           Previously, we would always read to and write from transactions1, even though the installer might actually have installed a non-1 schema version (and logged that this happened!).
           (`Pull Request <https://github.com/palantir/atlasdb/pull/4070>`__)


========
v0.145.0
========

4 Jun 2019

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Fixed a bug causing connection leaks to timelock.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/4052>`__)

    *    - |improved|
         - The default value for the length of pause between targeted sweep iterations, ``pauseMillis``, has been reduced to 50ms.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/4067>`__)

    *    - |improved|
         - Changed the default values in ``PaxosRuntimeConfiguration`` as (`#3943 <https://github.com/palantir/atlasdb/pull/3943>`__) changed it on test configs only.
           ``leader-ping-response-wait-in-ms`` was reduced to 2000 ms from 5000 ms.
           ``maximum-wait-before-proposal-in-ms`` was reduced to 300 ms from 1000 ms.
           ``ping-rate-in-ms`` was reduced to 50 ms from 5000 ms.
           These settings have empirically improved the performance of timelock when the leader node goes down without negatively affecting stability.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/4055>`__)

    *    - |improved|
         - Clients now log at most once every 10 seconds when a ``TimelockService`` level request is slow.
           Please see the JavaDocs of ``ProfilingTimelockService`` for more information.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/4QQQ>`__)

========
v0.144.0
========

20 May 2019

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |devbreak|
         - ``AutoDelegate`` only works on interfaces now.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/4045>`__)

    *    - |fixed|
         - Fixed a bug in ``TransactionManagers`` introduced by a recently added caching layer, causing NPE's.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/4044>`__)

    *    - |improved|
         - The pause time between iterations of targeted sweep for each background thread is now configurable by the targeted sweep runtime configuration ``pauseMillis``.
           The default value has also changed from 5000 milliseconds to 500.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/4046>`__)

========
v0.143.1
========

16 May 2019

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - ``InsufficientConsistencyException`` and ``NoSuchElementException`` will now not cause nodes to be blacklisted from the Cassandra client pool.
           Previously this could happen - even though these exceptions are not reflective of the individual node in question being unable to service requests.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/4038>`__)

    *    - |fixed|
         - Removed the DB username from the Hikari connection pool name.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3949>`__)

    *    - |fixed|
         - Fixed incorrect delegation that took place in AutoDelegate transaction managers.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/4041>`__)

========
v0.143.0
========

16 May 2019

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved| |deprecated|
         - Replaced all usages of Guava Supplier by Java Supplier. Please note that while Guava Supplier endpoints may still exist, they will be removed in a future release.
           (`Pull Request 1 <https://github.com/palantir/atlasdb/pull/3978>`__,
           `Pull Request 2 <https://github.com/palantir/atlasdb/pull/3995>`__ and
           `Pull Request 3 <https://github.com/palantir/atlasdb/pull/4034>`__)

    *    - |fixed|
         - Coordination service metrics no longer throw ``NullPointerException`` when attempting to read the metric value before reading anything from the coordination store.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/4031>`__)

    *    - |fixed|
         - AtlasDB now maintains a finite length for the delete executor's work queue, to avoid OOMs on services with high conflict rates for transactions.
           In the event the queue length is reached, we will not proactively schedule cleanup of values written by a transaction that was rolled back.
           Note that it is not essential that these deletes are carried out immediately, as targeted sweep will eventually clear them out.
           Previously, this queue was unbounded, meaning that service nodes could end up using lots of memory.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/4037>`__)

    *    - |improved|
         - The coordination store now retries when reading a value at a given sequence number that no longer exists (as opposed to throwing).
           This is necessary for supporting cleanup of the coordination store.
           Note that if one is performing rolling upgrades to a version that sweeps the coordination store, one MUST upgrade from at least this version.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3990>`__)

========
v0.142.2
========

14 May 2019

This version is equivalent to v0.142.0, but was re-tagged because of publishing issues.

========
v0.142.1
========

14 May 2019

This version is equivalent to v0.142.0, but was re-tagged because of publishing issues.

========
v0.142.0
========

14 May 2019

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - The default configuration for the number of targeted sweep shards has been increased to 8.
           This enables us to increase the speed of targeted sweep if processing the queue starts falling behind.
           Previously, we could only increase the speed of processing future entries, as we cannot sweep entries with higher parallelism than the number of shards active when the writes were made.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3997>`__)

    *    - |improved|
         - AtlasDB now throws an ``IllegalArgumentException`` when attempting to create a column range selection that is invalid (has end before start).
           Previously, exceptions were thrown from the underlying KVS, but these were implementation-dependent.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3993>`__)

    *    - |devbreak|
         - ``AtlasDbHttpClients.createProxyWithFailover()`` now requires ``UserAgent`` parameter.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3996>`__)

========
v0.141.0
========

9 May 2019

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |new|
         - ``TransactionManagers`` now has a new builder option ``lockImmutableTsOnReadOnlyTransactions()``.
           If it is set to ``true`` all transactions (including read-only ones) will grab immutable ts lock, enabling migrating to thorough sweep without downtime.
           Please contact the AtlasDB team before using this feature.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3987>`__)

    *    - |improved|
         - The Timelock Availability Health check should not timeout if we can't reach other nodes. This should stop
           the health check firing erroneously.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3988>`__)

    *    - |new|
         - Setting ``lockImmutableTsOnReadOnlyTransactions()`` to ``true`` disables background sweep. This aims to prevent Cassandra load caused by Conservative to Thorough sweep migration.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3992>`__)

========
v0.140.0
========

8 May 2019

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |metrics| |improved|
         - Client side tombstone filtering is now instrumented more exhaustively.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3977>`__)

    *    - |devbreak|
         - ``TimelockDeprecatedConfig`` and ``TimeLockServerConfiguration`` have been removed.
           Note that these configurations were only used for the dropwizard timelock server, which should only be used in tests.
           Now, the dropwizard server launcher uses a similar setup to the use in production, forcing the use of client request limits and lock time limiter.
           Note that this requires converting your existing ``TimeLockServerConfiguration`` to a ``CombinedTimeLockServerConfiguration``.
           For an example of this conversion, refer to the PR below.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3971>`__)

    *    - |improved|
         - Changed the default values in ``PaxosConfiguration``.
           ``leader-ping-response-wait-in-ms`` was reduced to 2000 ms from 5000 ms.
           ``maximum-wait-before-proposal-in-ms`` was reduced to 300 ms from 1000 ms.
           ``ping-rate-in-ms`` was reduced to 50 ms from 5000 ms.
           These settings have empirically improved the performance of timelock when the leader node goes down without negatively affecting stability.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3943>`__)

========
v0.139.0
========

30 Apr 2019

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |metrics| |changed|
         - All instrumentation AtlasDB metrics now use a ``SlidingTimeWindowArrayReservoir``.
           Previously, they used an exponentially decaying reservoir.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3937>`__)

========
v0.138.0
========

25 Apr 2019

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |userbreak|
         - AtlasDB Cassandra KVS now depends on rescue 4.4.0 (was previously 3.22.0).
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3954>`__)

========
v0.137.0
========

25 Apr 2019

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Coordination service now checks for semantic equality of ``VersionedInternalSchemaMetadata`` payloads as opposed to byte equality when deciding whether to reuse an existing value agreed on.
           Previously, using byte equality meant that multi-node clusters could end up spuriously writing the stored value many times, causing unnecessarily wide rows.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3954>`__)

========
v0.136.1
========

23 Apr 2019

This release is equivalent to v0.136.0 but was re-tagged due to a publishing issue.

========
v0.136.0
========

23 Apr 2019

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved| |devbreak|
         - Usage metrics for the coordination store have been added.
           Users should provide a MetricsRegistry when creating their coordination services.
           Also, ``CoordinationService.createDefault()`` now handles instrumentation of both the coordination service and store.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3894>`__)

    *    - |fixed|
         - lock-api now declares a minimum dependency on timelock-server 0.59.0.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3894>`__)

========
v0.135.0
========

19 Apr 2019

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - Coordination service now only initiates one request to perpetuate the bound forward at a time.
           This should avoid unnecessarily many CAS operations taking place when we need to do this.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3953>`__)

========
v0.134.0
========

18 Apr 2019

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - We now close Cassandra clients properly when verifying that one's Cassandra configuration makes sense.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3944>`__)

========
v0.133.0
========

16 Apr 2019

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - AtlasDB now logs diagnostic information about usage of classes that utilise smart batching (e.g. when starting transactions, verifying leadership, _transactions2 put-unless-exists, etc.).
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3924>`__)

========
v0.132.0
========

11 Apr 2019

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed| |devbreak|
         - Stop memoizing the ``Supplier`` of ``TimestampService``, as we **must** get a fresh instance on each ``Supplier.get()`` call to ensure correctness after leadership elections.
           Without it, there is a possibility of data corruption if you are running atlas with a leader block in a multi-node configuration.
           Services using External Timelock, Embedded or Leader with 1 node will not be affected.
           Dev break to force ``AtlasDbFactory`` and ``ServiceDiscoveringAtlasSupplier`` to return ``ManagedTimestampService`` which unifies ``TimestampService`` and ``TimestampManagementService``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3911>`__)

    *    - |improved|
         - Removed unnecessary memory allocations in the lock refresher, and in several other classes, by using Lists.partition(...) instead of Iterables.partition(...).
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3918>`__)

========
v0.131.0
========

9 Apr 2019

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Cassandra client input and output transports are now properly closed.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3892>`__)

========
v0.130.0
========

4 Apr 2019

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |new|
         - AtlasDB now supports _transactions2 if backed by Cassandra KVS or In-Memory KVS.
           This is expected to improve transaction performance by making ``putUnlessExists`` faster, and increase stability by avoiding hotspotting of the transactions table in Cassandra.
           This is a beta feature; please contact the AtlasDB team if you are interested to use _transactions2.
           (Many PRs; key PRs include `Pull Request 1 <https://github.com/palantir/atlasdb/pull/3706>`__,
           `Pull Request 2 <https://github.com/palantir/atlasdb/pull/3707>`__,
           `Pull Request 3 <https://github.com/palantir/atlasdb/pull/3726>`__,
           `Pull Request 4 <https://github.com/palantir/atlasdb/pull/3732>`__)

    *    - |fixed|
         - ``putUnlessExists`` in Cassandra KVS now produces correct cell names when failing with a ``KeyAlreadyExistsException``.
           Previously, Cassandra KVS used to produce incorrect cell names (that were the concatenation of the correct cell name and an encoding of the AtlasDB timestamp).
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3882>`__)

    *    - |new|
         - A new configuration option ``lockImmutableTsOnReadOnlyTransactions`` is added under ``atlas-runtime.transaction``. Default value for this flag is ``false``, and setting it to ``true``
           enables running read-only transactions on thorough sweep tables; but introduces a perf overhead to read-only transactions on conservative sweep tables. This is an experimental feature,
           please do not change the default value for this flag without talking to AtlasDB team.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3888>`__)

    *    - |fixed|
         - ``WriteBatchingTransactionService`` now tries requests for duplicated timestamps in subsequent batches.
           Previously, we would immediately throw a ``SafeIllegalStateException`` when seeing a duplicate in a single batch, which was causing unnecessary failures in transactions which could handle the ``KeyAlreadyExistsException`` safely.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3879>`__)

    *    - |fixed|
         - Fixed a rare situation in which interrupting a thread could possibly leave dangling locks.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3805>`__)

    *    - |fixed|
         - Coordination services now only perpetuate an existing value on value-preserving transformations if the existing bound is invalid at a fresh sequence number.
           Previously, we would perpetuate the bound regardless, meaning that when the bound is crossed in a multi-threaded environment, each in-flight transaction that tries to determine its transaction schema version will independently attempt to perpetuate the bound.
           This may lead to multiple unnecessary updates to the coordinated value in a short space of time.
           Note that updates that do change the value will be applied regardless, and could potentially still race if applied in parallel.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3889>`__)

    *    - |improved|
         - ``LockRefresher`` now logs at INFO when locks cannot be refreshed in that the server does not indicate that they were refreshed, along with a sample of the lock tokens involved.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3866>`__)

    *    - |improved|
         - Reduced dependency footprint by replacing dependency on groovy-all with dependencies on groovy, groovy-groovysh, and groovy-json.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3886>`__)


========
v0.129.0
========

28 Mar 2019

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Oracle KVS now deletes old entries correctly if using targeted sweep.
           Previously, there were situations where it would not delete values that could safely be deleted.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3870>`__)

    *    - |improved|
         - The Cassandra KVS ``CellLoader`` now supports cross-column batching for requests which query a variety of columns for a few rows.
           Previously, we would make separate requests for each of these columns in parallel, creating additional load on Cassandra.
           Internal benchmarks reflect a 4-5x improvement in read p99s for such workflows (e.g. small numbers of rows with static columns, or rows with dynamic columns when the column key is varied and known in advance).
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3860>`__)

    *    - |improved|
         - Concurrent calls to `TimelockService.startIdentifiedAtlasDbTransaction()` now coalesced into a single Timelock rpc to reduce load on Timelock.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3844>`__)

    *    - |devbreak|
         - `RemoteTimelockServiceAdapter` is now closeable. Users of this class should invoke `close()` before termination to avoid thread leaks.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3844>`__)

    *    - |userbreak| |fixed|
         - AtlasDB Cassandra KVS now depends on sls-cassandra 3.31.0 (was 3.31.0-rc3).
           We do not want to stay on an RC version now that a full release is available.
           Note that this means that you must use this version of the sls-cassandra server if you want to use Cassandra KVS.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3872>`__)



========
v0.128.0
========

27 Mar 2019

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |userbreak|
         - AtlasDB Cassandra KVS now depends on sls-cassandra 3.31.0-rc3 (was 3.27.0).
           This version of Cassandra KVS supports a ``multiget_multislice`` operation which retrieves different columns across different rows in a single query.
           Note that this means that you must use this version of the sls-cassandra server if you want to use Cassandra KVS.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3849>`__)

    *    - |fixed| |devbreak|
         - Callbacks specified in TransactionManagers will no longer be run synchronously when ``initializeAsync`` is set to true, even if initialization succeeds in the first, synchronous attempt.
           Previously, we would attempt to run the callbacks synchronously when synchronous initialization succeeds, but this prevented use cases where the callback must block until an external resource is available.
           Consequently, even if the initialization of a transaction manager created with asynchronous initialization succeeds synchronously, readiness of the returned object must be checked because transaction managers are not ready to be used until callbacks successfully run.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3865>`__)

    *    - |changed|
         - Postgres 9.5.2+ requirement temporarily rescinded.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3862>`__)

    *    - |logs|
         - Added extra debug/trace logging to log the state of the Cassandra pool / application when running into cassandra pool exhaustion errors.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3863>`__)

========
v0.127.0
========

25 Mar 2019

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Fixed an issue where the ``transformAgreedValue`` of the ``KeyValueServiceCoordinationStore`` would throw an NPE when check and set fails on KVSs that do not support detail on CAS failure (DbKvs).
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3848>`__)

    *    - |fixed| |userbreak|
         - Background Sweep will now continue to prioritise tables accordingly, if writes to the sweep queue are enabled but targeted sweep is disabled on startup.
           Previously, Background Sweep would not prioritise new writes for sweeping if writes to the sweep queue were enabled.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3824>`__)

    *    - |changed| |improved|
         - We've rolled back the change from 0.117.0 that introduces an extra delay after leader election as we are no longer pursuing leadership leases.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3836>`__)

    *    - |improved| |devbreak|
         - `AtlasDbHttpClients`, `FeignOkHttpClients` and `AtlasDbFeignTargetFactory` are refactored to get rid of deprecated methods and overused overloads.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3837>`__)

========
v0.126.0
========

18 Mar 2019

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |changed| |userbreak|
         - Removed functionality for marking tables as deprecated as part of the schema definition and automatically dropping deprecated tables on startup.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3829>`__)

    *    - |improved|
         - Improved the startup check that verifies the correctness of the timestamp source to impose tighter constraints. Now uses a recent value from the puncher store
           rather than the unreadable timestamp.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3825>`__)

    *    - |fixed|
         - ``KeyValueService`` and ``CassandraKeyValueService`` in particular now has tighter consistency guarantees in the presence of failures.
           Previously, inconsistent deletes to thoroughly swept tables could result in readers serving stale versions of cells.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3807>`__)

    *    - |devbreak|
         - The contract of ``deleteAllTimestamps`` has been strengthened, and the default implementation has been removed.
           Please contact the AtlasDB team if you think this affects your workflows.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3807>`__)

    *    - |fixed|
         - Fixed a bug in ``PaxosQuorumChecker`` causing a new timelock leader to block for 5 seconds before being able to serve requests if another node was unreachable.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3811>`__)

========
v0.125.0
========

07 Mar 2019

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - ``CassandraKeyValueService`` now exposes a lightweight method for obtaining row keys.
           If you believe you need to use this method, you should reach out to the AtlasDB team first to assess your options.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3757>`__)

    *    - |changed| |userbreak|
         - The minimum Postgres version is now 9.5.2
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3786>`__)

    *    - |fixed|
         - Some race conditions in ``TableRemappingKeyValueService`` and ``KvTableMappingService`` have been fixed.
           Previously, it was possible to run into unexpected instances of ``NullPointerException`` and ``IllegalStateException`` when reading from tables, even when other (completely disjoint) sets of tables were created or dropped.
           It is likely that there remain more bugs here, though we have fixed several more egregious ones.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3803>`__)

========
v0.122.0
========

22 Feb 2019

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved| |devbreak|
         - Clients talking to Timelock will now throw instead of making a request with a payload larger than 50MB.
           This addresses several internal issues concerning Timelock stability.
           This is a devbreak in several AtlasDB utility classes used to create clients, where an additional boolean parameter has been added controlling whether its requests should be limited.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3772>`__)

    *    - |improved|
         - Timelock clients now use leased lock tokens to reduce number of RPC's to Timelock server, and improve transaction performance.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3760>`__)

    *    - |devbreak|
         - `startIdentifiedAtlasDbTransaction()` and `lockImmutableTimestamp()` now being called without an `IdentifiedTimeLockRequest` parameter.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3769>`__)

========
v0.121.0
========

21 Feb 2019

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - We now use jetty-alpn-agent 2.0.9
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3763>`__)

    *    - |improved| |devbreak|
         - All usage of remoting-api and remoting3 have been replaced by their equivalents in `com.palantir.tracing`, `com.palantir.conjure.java.api`, and `com.palantir.conjure.java.runtime`.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3764>`__)

========
v0.120.0
========

19 Feb 2019

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |devbreak|
         - The deprecated `startAtlasDbTransaction()` method is removed from `TimelockService`.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3758>`__)

    *    - |devbreak|
         - `startIdentifiedAtlasDbTransaction` now accepts `IdentifiedTimeLockRequest` as a parameter rather than `StartIdentifiedAtlasDbTransactionRequest`. Moving the requestorId
           information to TimelockClient from the caller.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3758>`__)

    *    - |fixed|
         - ``FailoverFeignTarget`` now retries correctly if calls to individual nodes take a long time and eventually fail with an exception.
           Previously, we could fail out without having tried all nodes under certain circumstances, even when there existed a node that could legitimately service a request.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3752>`__)

    *    - |fixed|
         - Fixed cases where column range scans could result in NullPointerExceptions when there were concurrent writes to the same range.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3756>`__)

========
v0.119.0
========

13 Feb 2019

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |changed| |improved|
         - TimeLock will now no longer create its high level paxos directory at configuration de-serialization time.
           Instead it waits until creating each individual learner or acceptor log directory, allowing timelock to rely more accurately on directory existence as a proxy for said timelock node being new or not.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3745>`__)

========
v0.118.0
========

8 Feb 2019

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved| |devbreak|
         - AtlasDB Cassandra KVS now depends on ``com.palantir.cassandra`` instead of ``org.apache.cassandra``.
           This version of Cassandra thrift client supports a ``put_unless_exists`` operation that can update multiple columns in the same row simultaneously.
           The Cassandra KVS putUnlessExists method has been updated to use the above call.
           Note that this means that you must use sls-cassandra server 3.27.0 if you want to use Cassandra KVS.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3726>`__)

    *    - |devbreak| |improved|
         - The `TableMetadata` class has been refactored to use Immutables.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3624>`__)

    *    - |new| |metrics|
         - Transaction services now expose timer metrics indicating how long committing or getting values takes.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3733>`__)

    *    - |fixed|
         - Entries with the same value in adjacent ranges in a timestamp partitioning map will now be properly coalesced, and for the purposes of coordination will not be written as new values.
           Previously, these were stored as separate entries, meaning that unnecessary values may have been written to the coordination store; this does not affect correctness, but is unperformant.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3733>`__)

    *    - |improved|
         - AtlasDB now allows you to enable a new transaction retry strategy with exponential backoff via configs.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3749>`__)

========
v0.117.0
========

28 Jan 2019

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |changed|
         - Timelock service no longer supports synchronous lock endpoints. Users who explicitly stated timelock to use synchronous resources by setting `install.asyncLock.useAsyncLockService` to `false` (default is `true`) should migrate to `AsyncLockService` before taking this upgrade.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3718>`__)

    *    - |devbreak|
         - Key value services now require their ``CheckAndSetCompatibility`` to be specified.
           Refer to the contract of ``KeyValueService#getCheckAndSetCompatibility`` and the ``CheckAndSetCompatibility`` enum class to guide this decision.
           Please be very careful if you are explicitly setting this to ``CheckAndSetCompatibility.SUPPORTED_DETAIL_ON_FAILURE``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3713>`__)

    *    - |improved|
         - AtlasDB now has an extra delay after leader elections; this lays the groundwork for leadership leases.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3701>`__)

    *    - |improved|
         - We now correctly handle host restart in the clock skew monitor.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3716>`__)

========
v0.116.1
========

20 Dec 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - The completion service in the Paxos leader election service should be more resilient to individual nodes being slow.
           Previously, if one individual node had a full thread pool, the service would throw a ``RejectedExecutionException`` even if other nodes were able to service the request.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3687>`__)

========
v0.116.0
========

14 Dec 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |new|
         - AtlasDB now writes to the _coordination table, a new table which is used to coordinate changes to schema metadata internal to AtlasDB across a multi-node cluster.
           Services which want to adopt _transactions2 will need to go through this version, to ensure that nodes are able to reach a consensus on when to switch the transaction schema version forwards.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3686>`__)

    *    - |fixed|
         - AtlasDB transaction services no longer throw exceptions when performing Thorough Sweep on tables with sentinels.
           Previously, the services would throw when trying to delete the sentinel, meaning that Background and Targeted Sweep would become stuck if sweeping thorough tables that used to be conservative, or tables that had undergone hard delete via the scrubber.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3704>`__)

    *    - |changed| |devbreak|
         - AtlasDB transaction services now no longer support negative timestamps.
           Users are unlikely to be affected, since using transaction services with negative timestamps was already broken in the past owing to the use of negative numbers for special values (like sentinels or a marker meaning that a transaction was rolled back).
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3709>`__)

    *    - |devbreak|
         - With the introduction of _coordination, creation of ``TransactionService`` now requires a ``CoordinationService<InternalSchemaMetadata>``.
           Users may create a ``CoordinationService`` via the ``CoordinationServices`` factory, if needed, or retrieve it from the relevant ``TransactionManager``.
           Generally speaking, ``TransactionService`` should not be directly used by standard AtlasDB consumers; abusing it can result in **SEVERE DATA CORRUPTION**.
           (`Pull Request 1 <https://github.com/palantir/atlasdb/pull/3686>`__ and `Pull Request 2 <https://github.com/palantir/atlasdb/pull/3689>`__)

    *    - |devbreak|
         - Transaction Managers now expose a ``getTransactionService()`` method.
           Users with custom subclasses of ``TransactionManager`` will need to implement this.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3689>`__)

    *    - |new| |metrics|
         - With the introduction of _coordination, we expose new metrics indicating the point (in logical timestamps) till which the coordination service knows what has been agreed, as well as the transactions schema version that will eventually be applied.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3691>`__)

    *    - |fixed| |userbreak|
         - Cassandra KVS `getMetadataForTables` method now returns a map where table reference keys have capitalisation matching the table names in Cassandra.
           Previously there was no strict guarantee on the keys' capitalisation, but it was in most cases all lowercase.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3649>`__)

    *    - |fixed|
         - Cassandra KVS `getMetadataForTables` method now does not contain entries for tables that do not exist in Cassandra.
           Previously, when a table was dropped, an empty byte array would be written into the _metadata table to mark it as deleted.
           Now, we delete all rows of the _metadata table containing entries pertaining to the dropped table.
           Note that this involves a range scan over a part of the _metadata table.
           While it is not expected that this significantly affects performance of table dropping, please contact the AtlasDB team if this causes issues.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3649>`__)

========
v0.115.0
========

07 Dec 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Cassandra KVS now correctly decommissions servers from the client pool that do not appear in the current token range if autoRefreshNodes is set to true (default value).
           Previously, refresh would only add discovered new servers, but never remove decommissioned hosts.
           The new behaviour enables live decommissioning of Cassandra nodes, without having to update the configuration and restart of AtlasDB to stop trying to talk to that server.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3661>`__)

    *    - |fixed|
         - The @AutoDelegate annotation now works correctly for interfaces which have static methods, and for simple cases of generics.
           Previously, the annotation processor would generate code that wouldn't compile.
           Note that some cases (e.g. sub-interfaces of generics that refine type parameters) are still not supported correctly.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3670>`__)

    *    - |improved|
         - TimeLock Server now logs that a new client has been registered the first time a service makes a request (for each lifetime of each server).
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3676>`__)

    *    - |improved|
         - Adds com.palantir.common.collect.IterableView#stream method for simplified conversion to Java Stream API usage.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3703>`__)

========
v0.114.0
========

03 Dec 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |userbreak|
         - As part of preparatory work to migrate to a new transactions table, this version of AtlasDB and all versions going forward expect to be using a version of TimeLock that supports the ``startIdentifiedAtlasDbTransaction`` endpoint.
           Support for previous versions of TimeLock has been dropped; please update your TimeLock server.
           Products should depend on TimeLock 0.51.0 or higher, or ignore this dependency altogether if they do not expect to use TimeLock.
           Note that new versions of the TimeLock server still expose the old endpoints, so old clients may still safely use a new TimeLock server.
           Also note that some momentary issues may be faced if one is performing a rolling upgrade of embedded services, though once the upgrades settle services should work normally.
           Note that for or across this version, blue-green deployment of embedded services is not supported.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3597>`__)

========
v0.113.0
========

03 Dec 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - KVS Migration CLI no longer migrates the checkpoint table if it exists on the source KVS.
           Previously, existence of an old checkpoint table on the source KVS could cause a migration to silently skip migrating data.
           Furthermore, in the cleanup stage of migration, the checkpoint table is now dropped instead of truncated.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3587>`__)

    *    - |improved|
         - Read transactions on thoroughly swept tables requires one less RPC to timelock now.
           This improves the read performance and reduces load on timelock.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3651>`__)

    *    - |fixed|
         - Fix warning in stream-store generated code.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3667>`__)

========
v0.112.1
========

26 Nov 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Wrap shutdown callback running in try-catch.
           This guards against any shutdown hooks throwing unchecked exceptions,
           which would cause other hooks to not run.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3654>`__)

========
v0.112.0
========

26 Nov 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Remove a memory leak due to usages of Runtime#addShutdownHook to cleanup resources.
           This only applies where multiple `TransactionManager` s might exist in a single VM
           and they are created an shutdown repeatedly.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3653>`__)

========
v0.111.0
========

20 Nov 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Fixed a bug where lock and timestamp services were not closed when transaction managers were closed.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3644>`__)

========
v0.110.0
========

20 Nov 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - Numerous small internal improvements that did not include release notes.

========
v0.109.0
========

14 Nov 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |devbreak|
         - PaxosQuorumChecker now takes an ExecutorService as opposed to an Executor.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3596>`__)

    *    - |fixed|
         - Re-introduced the distinct bounded thread pools to PaxosLeaderElectionService for communication with other PaxosLearners and PingableLeaders.
           Previously, a single unbounded thread pool was used, which could cause starvation and OOMs under high load if any learners or leaders in the cluster were slow to fulfil requests.
           This change also improves visibility as to which specific communication workflows may be suffering from issues.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3596>`__)

    *    - |fixed|
         - Targeted sweep now handles table truncations with conservative sweeps correctly.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3610>`__)

    *    - |Improved|
         - No longer calls deprecated OkHttpClient.Builder().sslSocketFactory() method, now passes in X509TrustManager.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3627>`__)

    *    - |improved|
         - Sha256Hash now caches its Java hashCode method.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3633>`__)

    *    - |improved|
         - The version of javapoet had previously been bumped to 1.11.1 from 1.9.0. However this was not done consistently across the repository. The atlasdb-client and atlasdb-processors subprojects now also use the newer version.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3631>`__)

========
v0.108.0
========

7 Nov 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Cassandra KVS no longer uses the schema mutation lock and instead creates tables using an id deterministically generated from the Cassandra keyspace and the table name.
           As part of this change, table deletion now truncates the table before dropping it in Cassandra, therefore requiring all Cassandra nodes to be available to drop tables.
           This fixes a bug where it was possible to create two instances of the same table on two different Cassandra nodes, resulting in schema version inconsistency that required manual intervention.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3620>`__)

    *    - |improved|
         - Introduced runtime checks on the client side for timestamps retrieved from timelock. This aims to prevent data corruption if timestamps go back in time, possibly caused by a misconducted timelock migration. This is a best effort for catching abnormalities on timestamps at runtime, and does not provide absolute protection.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3568>`__)

    *    - |userbreak|
         - Qos Service: The experimental QosService for rate-limiting clients has been removed.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3586>`__)

    *    - |fixed|
         - Fixed a bug in the ``AsyncInitializer.cancelInitialization`` method that caused asynchronously initialized ``CassandraKeyValueServiceImpl`` and ``CassandraClientPoolImpl`` objects unable to be closed and shut down, respectively.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3578>`__)

    *    - |fixed|
         - Targeted sweep now deletes certain sweep queue rows faster than before, which should
           reduce table bloat (particularly on space constrained systems).
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3581>`__)

    *    - |improved| |fixed|
         - Schema mutations against the Cassandra KVS are now HA.
           Previously, Cassandra KVS required that after some schema mutations all cassandra nodes must agree on the schema version.
           Now, all reachable nodes must agree and at least a quorum of nodes must be reachable, instead.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3480>`__)

    *    - |devbreak|
         - The AutoDelegate annotation no longer supports a typeToExtend parameter.
           Users should instead annotate the desired class or interface directly.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3579>`__)

    *    - |fixed|
         - Targeted sweep does better with missing tables, and also with the empty namespace.
           Previously, it would just cycle on the error and never sweep. A highly undesirable condition.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3577>`__)

    *    - |fixed|
         - ``KeyValueServicePuncherStore``s ``getMillisForTimestamp`` method now does a much more efficient ``_punch`` table lookup.
           This affects the performance of calculating the ``millisSinceLastSweptTs`` metric for targeted sweep.
           Also, the above mentioned metric will now consistently report falling behind if no new entries are being punhed into the punch table.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3570>`__)

    *    - |improved|
         - The HikariConnectionClientPool now allows specification of a use-case.
           If specified, threads created will have the use-case in their name, and log messages about pool statistics will be prefaced by the use-case as well.
           This may be useful for debugging when users run multiple such pools.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3588>`__)

    *    - |new|
         - Old deprecated tables can now be added to a schema to be cleaned up on startup.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3569>`__)

    *    - |fixed|
         - Fixed a bug where ``AwaitingLeadershipProxy`` stops trying to gain leadership, causing client calls to leader to throw ``NotCurrentLeaderException``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3582>`__)

    *    - |new|
         - TimeLock now exposes a ``startIdentifiedAtlasDbTransaction`` endpoint.
           This may be used by AtlasDB clients for some key value services to achieve better data distribution and performance as far as the transactions table is concerned.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3529>`__)

    *    - |devbreak|
         - The schema metadata service has been removed, as the AtlasDB team does not intend to pursue extracting sweep to its own separate service in the short to medium term, and it was causing support issues.
           If you were consuming this service, please contact the AtlasDB team.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3605>`__)

    *    - |improved|
         - On Oracle backed DbKvs, schema changes that would require the addition of an overflow column will now throw upon application.
           Previously, puts would instead fail at runtime when the column did not exist.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3604>`__)

    *    - |improved|
         - The index cleanup task for stream stores now only fetches the first column for each stream ID when determining whether the stream is still in use.
           Previously, we would fetch the entire row which is unnecessary and causes read pressure on the key-value-service for highly referenced streams.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3423>`__)

    *    - |fixed|
         - Live-reloading HTTP proxies and HTTP proxies with failover now refresh themselves after encountering a large number of cumulative requests or consecutive exceptions.
           This was previously implemented to work around several issues with our usage of OkHttp, but was not implemented for the proxies with failover (which includes proxies to TimeLock).
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3629>`__)

========
v0.107.0
========

10 Oct 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - Targeted sweep now stores even less data in the sweepable cells table due to dictionary encoding table references instead of storing them as strings.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3533>`__)

    *    - |improved|
         - The legacy lock service's lock state logger now logs additional information about the lock service's internal synchronization state.
           This includes details of queueing threads on each underlying sync object, as well as information on the progress of inflight requests.
           (`Pull Request 1 <https://github.com/palantir/atlasdb/pull/3554>`__ and
           `Pull Request 2 <https://github.com/palantir/atlasdb/pull/3565>`__)

========
v0.106.0
========

2 Oct 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed| |devbreak|
         - Reverted the PR #3505, which was modifying PaxosLeaderElectionService to utilise distinct bounded thread pools, as this PR uncovered some resiliency issues with PaxosLeaderElectionService.
           It will be re-merged after fixing those issues.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3536>`__)

    *    - |fixed|
         - Targeted sweep now stores much less data in the sweepable cells table due to more efficient encoding.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3516>`__)

    *    - |new|
         - ``TransactionManager``s now expose a ``TimestampManagementService``, allowing clients to fast-forward timestamps when necessary.
           This functionality is intended for libraries that extend AtlasDB functionality; it is unlikely that users should directly require the TimestampManagementService.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3524>`__)

    *    - |fixed|
         - Targeted sweep no longer chokes if a table in the queue no longer exists, and was deleted by a different host while this host was online and sweeping.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3516>`__)

    *    - |improved|
         - Add versionId to SimpleTokenInfo to improve logging for troubleshooting.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3521>`__)

    *    - |improved|
         - Increase maximum allowed rescue dependency version to 4.X.X.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3525>`__)

    *    - |logs| |changed|
         - Changed the origin for logs when queries were slow from `kvs-slow-log` to `kvs-slow-log-2`.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3549>`__)

========
v0.105.0
========

20 Sep 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Improved threading for MetricsManager's metricsRegistry
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3497>`__)

    *    - |logs| |metrics|
         - Improved visibility into sources of high DB load.
           We log when a query returns a high number of timestamps that need to be looked up in the database, and tag some additional metrics with the tablename we were querying.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3488>`__)
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3504>`__)

    *    - |changed|
         - Upgrade http-remoting 3.41.1 -> 3.43.0 to make tracing delegate nicely.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3494>`__)

    *    - |improved|
         - Users may now provide their own executors to instances of ``BasicSQL`` and to ``BasicSQLUtils.runUninterruptably``.
           Previously users were forced to use a default executor which had an unbounded thread-pool and fixed keep-alive timeouts.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3487>`__)

    *    - |fixed|
         - TargetedSweepMetrics#millisSinceLastSweptTs updates periodically, even if targeted sweep is failing to successfully run.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3507>`__)

    *    - |fixed|
         - Targeted sweep no longer chokes if a table in the queue no longer exists.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3508>`__)

    *    - |fixed|
         - Targeted sweep threads will no longer die if Timelock unlock calls fail.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3510>`__)

    *    - |improved|
         - PaxosLeaderElectionService now utilises distinct bounded thread pools for communication with other PaxosLearners and PingableLeaders.
           Previously, a single unbounded thread pool was used, which could cause starvation and OOMs under high load if any learners or leaders in the cluster were slow to fulfil requests.
           This change also improves visibility as to which specific communication workflows may be suffering from issues.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3505>`__)

    *    - |fixed|
         - Fixed an issue in timelock where followers were publishing metrics with ``isCurrentSuspectedLeader`` tag set to ``true``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3509>`__)

    *    - |fixed|
         - Background sweep will now choose between priority tables uniform randomly if there are multiple priority tables.
           Previously, if multiple priority tables were specified, background sweep would repeatedly pick the same table to be swept, meaning that the other priority tables would all never be swept.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3512>`__)

    *    - |improved|
         - A few timelock ops edge cases have been removed. Timelock users must now indicate whether they are booting their
           servers for the first time or subsequent times, to avoid the situation where a timelock node becomes newly
           misconfigured and thinks it is booting up for the first time again. Additionally, timestamps no longer overflow
           when they hit Long.MAX_VALUE; this would only happen due to a bug, but at least now the DB will become read only
           and not corrupt.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3498>`__)

    *    - |devbreak|
         - PaxosQuorumChecker now takes an ExecutorService as opposed to an Executor.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3505>`__)

========
v0.104.0
========

4 Sep 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - The Jepsen tests no longer assume that users have installed Python or DateUtil, and will install these itself if needed.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3461>`__)

    *    - |changed|
         - Bumps com.palantir.remoting3 dependency to 3.41.1 from 3.22.0.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3482>`__)

========
v0.103.0
========

30 Aug 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - Targeted sweep queue now hard fails if it is unable to read table metadata to determine sweep strategy.
           Previously, we assumed the strategy was conservative, which could result in sweeping tables that should never be swept.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3477>`__)

    *    - |fixed|
         - Fixed an issue where targeted sweep would fail to increase the number of shards and error out if the default number of shards was ever persisted into the progress table.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3473>`__)

    *    - |fixed|
         - Several exceptions (such as when creating cells with overly long names or executors in illegal configurations) now contain numerical parameters correctly.
           Previously, the exceptions thrown would erroneously contain ``{}`` values.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3468>`__)

    *    - |fixed|
         - Cassandra Key Value Service now no longer logs spurious ERROR warning messages when failing to read new-format table metadata.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3478>`__)

    *    - |improved|
         - Throw more specific CommittedTransactionException when operating on a committed transaction.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3460>`__)

========
v0.102.0
========

24 Aug 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - CQL queries are now logged correctly (with safe and unsafe arguments respected).
           Previously, these versions would log all arguments as part of the format string as it eagerly did the string substitution.
           AtlasDB versions 0.100.0 through 0.101.0 (inclusive both ends) are affected.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3469>`__)

    *    - |devbreak| |improved|
         - CqlQuery is now an abstract class and must now be created through its builder.
           This makes the intention that the query string provided is safe considerably more explicit.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3469>`__)

    *    - |improved|
         - DbKvs now implements its own version of ``deleteAllTimestamps`` instead of using the default AbstractKvs implementation.
           This facilitates better performance of targeted sweep on DbKvs.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3379>`__)

    *    - |fixed|
         - LockRefreshingLockService now batches calls to refresh locks in batches of 650K.
           Previously, trying to refresh a larger number of locks could trigger the 50MB limit in payload size.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3450>`__)

    *    - |logs|
         - Reduce logging level for locks not being refreshed.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3458>`__)

========
v0.101.0
========

16 Aug 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |changed|
         - Targeted Sweep is now enabled by default.
           Products using atlasdb-cassandra library need to declare a dependency on Rescue 3 or ignore that dependency altogether.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3451>`__)

    *    - |fixed|
         - Fixed a bug that when filtering the row results for ``getRows`` in ``SnapshotTransaction`` could cause an exception due to duplicate keys in a map builder.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3445>`__)

    *    - |improved|
         - AtlasDB now correctly closes the targeted sweeper on shutdown, and logs less by default.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3447>`__)

    *    - |improved| |devbreak|
         - The atlasdb-commons package has had its dependency tree greatly pruned of unused cruft.
           This may introduce a devbreak to users transitively relying on these old dependencies.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3439>`__)

    *    - |changed|
         - ``CassandraRequestExceptionHandler`` is set to use ``Conservative`` exception handler by default. Main differences are:

            - Conservative exception handler backs off for larger subset of exceptions
            - Backoff period is exponentially increasing (but cannot go beyond ``MAX_BACKOFF``)
            - Retries are executed on a different host rather than the same host for a larger subset of exceptions

           (`Pull Request <https://github.com/palantir/atlasdb/pull/3444>`__)

    *    - |improved| |logs|
         - CassandraKVS's ``ExecutorService`` is now instrumented.
           This ExecutorService is responsible for submitting queries to the underlying DB. It being throttled will increase the latency of queries and transactions.
           The following metrics are available:

              - ``com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService.executorService.submitted``
              - ``com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService.executorService.running``
              - ``com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService.executorService.completed``
              - ``com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService.executorService.duration``

           (`Pull Request <https://github.com/palantir/atlasdb/pull/3416>`__)

    *    - |new| |devbreak|
         - ``TransactionManagers`` has a new builder option named ``validateLocksOnReads()``; set to ``true`` by default. This option is passed to ``TransactionManager``'s constructor, to be used in initialization of ``Transaction``.
           A transaction will validate pre-commit conditions and immutable ts lock after every read operation if underlying table is thoroughly swept (Default behavior). Setting ``validateLocksOnReads`` to ``false`` will stop transaction to do the mentioned validation on read operations; causing validations to take place only at commit time for the sake of reducing number of round-trips to improve overall transaction perf.
           This change will cause a devbreak if you are constructing a ``TransactionManager`` outside of ``TransactionManagers``. This can be resolved by adding an additional boolean parameter to the constructor (``true`` if you would like to keep previous behaviour)
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3414>`__)

========
v0.100.0
========

2 Aug 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Cassandra KVS now correctly accepts check-and-set operations if one is working with multiple columns in the relevant row.
           Previously, if there were multiple columns in the row where one was trying to do a CAS, the CAS would be rejected even if the column value matched the cell.
           Similarly, for put-unless-exists, the PUE would be rejected if there were any other cells in the relevant row (even if they had a different column name).
           We now perform the operations correctly only considering the value (or absence of value) in the relevant cell.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3388>`__)

    *    - |improved| |devbreak|
         - We have removed the ``sleepForBackoff(int)`` method from ``AbstractTransactionManager`` as there were no known users and its presence led to user confusion.
           AtlasDB does not actually backoff between attempts of running a user's transaction task.
           If your service overrides this method, please contact the AtlasDB team.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3432>`__)

    *    - |improved|
         - Sequential sweep now sleeps longer between iterations if there was nothing to sweep.
           Previously we would sleep for 2 minutes between runs, but it is unlikely that anything has changed dramatically in 2 minutes so we sleep for longer to prevent scanning the sweep priority table too often.  Going forward the most likely explanation for there being nothing to sweep is that we have switched to targeted sweep.
           We don't stop completely or sleep for too long just in case configuration changes and a table is eligible to sweep again.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3429>`__)

    *    - |improved|
         - TimeLockAgent now exposes the number of active clients and the configured maximum.
           This makes it easier for a service to expose these via a health check.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3431>`__)

=======
v0.99.0
=======

25 July 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Fixed an issue where a failure to punch a value into the _punch table would suppress any future attempts to punch.
           Previously, if the asynchronous job that punches a timestamp every minute ever threw an exception, the unreadable timestamp would be stuck until the service is restarted.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3427>`__)

    *    - |improved|
         - TimeLock by default now has a client limit of 500.
           Previously, this used to be 100 - however we have run into issues internally where stacks legitimately reach this threshold.
           Note that we still need to maintain the client limit to avoid a possible DOS attack with users creating arbitrarily many clients.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3413>`__)

    *    - |new| |metrics|
         - Added metrics for the number of active clients and maximum number of clients in TimeLock Server.
           These are useful to identify stacks that may be in danger of breaching their maxima.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3413>`__)

=======
v0.98.0
=======

25 July 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |new| |metrics|
         - Targeted sweep now exposes tagged metrics for the outcome of each iteration, analogous to the legacy sweep outcome metrics.
           The reported outcomes for targeted sweep are: ``SUCCESS``, ``NOTHING_TO_SWEEP``, ``DISABLED``, ``NOT_ENOUGH_DB_NODES_ONLINE``, and ``ERROR``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3399>`__)

    *    - |improved|
         - Changed the range scan behavior for the sweep priority table so that reads scan less data in Cassandra.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3410>`__)

=======
v0.97.0
=======

20 July 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - TimeLock Server now exposes a ``startAtlasDbTransaction`` endpoint which locks an immutable timestamp and then gets a fresh timestamp (in a single round-trip call); new TimeLock clients call this endpoint.
           This saves an estimated one TimeLock round-trip of latency when starting a transaction.
           Note that the old endpoints are still exposed (so TimeLock remains compatible with older Atlas clients), and there is an automated adapter for new TimeLock clients to talk to old TimeLock servers that don't have this endpoint.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3319>`__)

    *    - |improved| |logs|
         - Reduced the logging level of various log messages.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3101>`__)

    *    - |changed| |metrics|
         - CassandraClientPoolingContainer metrics are tagged by pool name. Previosly pool name was embedded in metric name.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3375>`__)

    *    - |improved|
         - Added the ``CallbackInitializable`` interface to simplify asynchronous initialization of resources using transaction manager callbacks.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3360>`__)

    *    - |improved|
         - The timestamp cache size is now actually live reloaded, and uses Caffeine instead of Guava for better performance. The read only transaction manager
           (almost unused) now no longer constructs a thread pool.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3351>`__)

    *    - |improved| |devbreak|
         - Transactions now have meters recording their outcomes (e.g. successful commits, lock expiry, being rolled back, read-write conflicts, etc.)
           In the cases of write-write and read-write conflicts, the first table on which a conflict occurred will be tagged on to the conflict meter if it is safe for logging.
           Note that some metric names have changed; in particular, ``SerializableTransaction.SerializableTransactionConflict`` and ``SnapshotTransaction.SnapshotTransactionConflict`` are now tracked as ``readWriteConflicts`` and ``writeWriteConflicts`` respectively under ``TransactionOutcomeMetrics``.
           This is also an improvement in terms of clarity, as serializable transactions that experienced write-write conflicts were previously marked as snapshot transaction conflicts.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3358>`__)

=======
v0.96.0
=======

11 July 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Targeted sweep metrics will no longer range scan the punch table if the last swept timestamp was issued more than one week ago.
           Previously, we would range scan the table even if the last swept timestamp was -1, which would force a range scan of the entire table.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3364>`__)

    *    - |fixed| |deprecated|
         - Atlas clients using Cassandra can specify type of kvs as `cassandra` rather then `CassandraKeyValueServiceRuntimeConfig` in runtime configuration.
           The `CassandraKeyValueServiceRuntimeConfig` type is now deprecated.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3359>`__)

    *    - |improved|
         - Startup and schema change performance improved for Cassandra users with large numbers of tables.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3278>`__)

=======
v0.95.0
=======

9 July 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - The atlas console metadata query now returns more table metadata, such as sweep strategy and conflict handler information.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3161>`__)

    *    - |devbreak|
         - The ``putUnlessExists`` API has been removed from AtlasDB tables, as it was misleading (it only did the put if the given row, column and value triple were already present, as opposed to the more intuitive condition of the row and column value pair being present).
           Please replace any uses of the table-level ``putUnlessExists`` with a get, check and put if appropriate - these will still be transactional because of the AtlasDB transaction protocol.
           Note that this is not the same as the KVS ``putUnlessExists`` API, which is still used by the transaction protocol.
           This API has already been deprecated since August 2017 (11 months from time of writing).
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3340>`__)

    *    - |improved|
         - We will no longer continue to update ``sweep.priority`` if writes are persisted to the targeted sweep queue.
           This means that assuming ``targetedSweep.enableSweepQueueWrites`` remains on, the background sweeper will eventually run out of things to sweep without further intervention.
           At this point, the background sweeper will start reporting ``NOTHING_TO_SWEEP``, and the background sweeper may safely be disabled.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3349>`__)

    *    - |fixed|
         - Writes to the targeted sweep queue are now done using the start timestamp of the transaction that makes the call.
           Previously, the writes were done at timestamp 0, which was interfering with Cassandra compactions.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3342>`__)

    *    - |fixed|
         - The sweep CLI will no longer perform in-process compactions after sweeping a table.
           For DbKvs, this operation is handled by the background compaction thread; Cassandra performs its own compactions.
           Note that the sweep CLI itself has been deprecated in favour of using the sweep priority override configuration, possibly in conjunction with the thread count (:ref:`Docs<sweep_tunable_parameters>`).
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3338>`__)

    *    - |new|
         - Three new conflict handlers ``SERIALIZABLE_CELL``, ``SERIALIZABLE_INDEX`` and ``SERIALIZABLE_LOCK_LEVEL_MIGRATION`` are added.
           ``SERIALIZABLE_CELL`` conflict handler is same as ``SERIALIZABLE``, but checks for conflicts by locking cells during commit instead of locking rows. Cell locks are more fine-grained, so this will produce less contention at the expense of requiring more locks to be acquired.
           ``SERIALIZABLE_INDEX`` conflict handler is designed to be used by index tables. As any write/write conflict on an index table will necessarily also be a write/write conflict on base table, this conflict handler does not check write/write conflicts. Read/write conflicts should still need to be checked, since we do not need to read the index table with the main table. This conflict handler also locks at cell level.
           If your schema already has a table with ``SERIALIZABLE`` conflict handler, and you would like to migrate it to ``SERIALIZABLE_CELL`` or ``SERIALIZABLE_INDEX`` with a rolling upgrade (without a shutdown); then you should first migrate it to ``SERIALIZABLE_LOCK_LEVEL_MIGRATION`` conflict handler to avoid data corruption.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3267>`__)

    *    - |devbreak|
         - Removed the token range skewness logger from the Cassandra KVS. We've not been relying on it to catch issues and it produces a very large output that is cumbersome.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3352>`__)

=======
v0.94.0
=======

28 June 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - Snapshot transaction ``getRowsColumnRange`` performance has been improved by using an ``ImmutableSortedMap.Builder`` and constructing the map at the end.
           We previously used a ``SortedSet`` which would incur overhead in rebalancing the underlying red-black tree as the data was already mostly sorted.
           We have seen a 7 percent speedup for reading all columns from a wide row (50,000 columns).
           We have also seen a 6 percent speedup for reading 50,000 columns from a wide row, where a random 2 percent of these rows are from uncommitted transactions.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3319>`__)

    *    - |devbreak|
         - Snapshot transactions now return immutable maps when calling ``getRows`` and ``getRowsColumnRange``.
           These used to return mutable maps - please make a copy of the map if you need it to be mutable.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3319>`__)

    *    - |new|
         - Multiple ``BackgroundSweeper`` threads can now run simultaneously.
           To enable this, set the runtime option ``sweep/sweepThreads`` to the desired number of threads and restart any Atlas client.
           If running multiple clients, these threads will be randomly split across them.
           Due to the load it may place on Cassandra, this option is not recommended for long-term use for Cassandra-backed installations.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3257>`__)

    *    - |improved|
         - Sweep progress is now stored per-table, meaning that if background sweep of a table is interrupted
           (for example, because sweep priority config changed), next time the background sweeper selects that table, it will pick up where it left off.
           Previously, the table would be swept from the start, potentially leading to several days of work being redone.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3257>`__)

    *    - |devbreak|
         - The ``BackgroundSweeper`` is no longer a ``Runnable``. Its job is now to manage ``BackgroundSweeperThread`` instances, which are ``Runnable``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3257>`__)

    *    - |improved|
         - Targeted sweep now stops reading from the sweep queue immediately if it encounters an entry known to be committed after the sweep timestamp.
           Previously, we would read an entire batch before checking commit timestamps so that lookups can be batched, but this is not necessary if the commit timestamp is cached from a previous iteration.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3325>`__)

    *    - |improved|
         - Write transactions now unlock their row locks and immutable timestamp locks asynchronously after committing.
           This saves an estimated two TimeLock round-trips of latency when committing a transaction.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3303>`__)

    *    - |new|
         - AtlasDB clients now batch calls to unlock row locks and immutable timestamp locks across transactions.
           This should reduce request volumes on TimeLock Server.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3303>`__)

    *    - |fixed|
         - Snapshot transactions now write detailed profiling logs of the form ``Committed {} bytes with locks...`` only once every 5 seconds per ``TransactionManager`` used.
           Previously, they were written on every transaction.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3326>`__)

    *    - |fixed|
         - AtlasDB Benchmarks, CLIs and Console now shutdown properly under certain read patterns.
           Previously, if these tools needed to delete a value that a failed transaction had written, the delete executor was never closed, thereby preventing an orderly JVM shutdown.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3328>`__)

    *    - |fixed|
         - Fixed a bug in C* retry logic where number of retries over all the hosts were used as number of retries on a single host, which may cause unexpected blacklisting behaviour.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3323>`__)

=======
v0.93.0
=======

25 June 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved| |metrics|
         - Snapshot Transaction metrics now track the post-commit step of unlocking the transaction row locks.
           Also, the ``nonPutOverhead`` and ``nonPutOverheadMillionths`` metrics now account for this step as well.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3307>`__)

    *    - |improved|
         - Targeted sweep now uses timelock locks to synchronize background threads on multiple hosts.
           This avoids multiple hosts doing the same sweeps.
           Targeted sweep also no longer forcibly sets the number of shards to at least the number of threads.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3310>`__)

    *    - |fixed|
         - Cassandra deleteRows now avoids reading any information in the case that we delete the whole row.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3312>`__)

    *    - |userbreak|
         - The ``scyllaDb`` option in Cassandra KVS config has been removed.
           Please contact the AtlasDB team if you deploy AtlasDB with scyllaDb (this was never supported).
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3313>`__)

    *    - |fixed| |logs|
         - Fixed a bug where Cassandra client pool was erroneously logging host removal from denylist, even the host was not blacklisted in the first place.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3314>`__)

=======
v0.92.2
=======

22 June 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - With targeted sweep, we now only call timelock once per set of range tombstones we leave, rather than once per cell.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3305>`__)

=======
v0.92.1
=======

21 June 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - We now consider only one row at a time when getting rows from the KVS with sweepable cells.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3302>`__)

    *    - |fixed|
         - Cassandra retry messages now log bounds on attempts correctly.
           Previously, they would log the supplier of these bounds (instead of the actual bounds, which users are more likely to be interested in).
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3291>`__)

=======
v0.92.0
=======

20 June 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved| |metrics|
         - We now publish metrics for more individual stages of the commit stage in a SnapshotTransaction.
           We also now publish metrics for the total non-KVS overhead - both the absolute time involved as well as a ratio of this to the total time spent in the commit stage.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3273>`__)

    *    - |new| |logs|
         - Snapshot transactions now, up to once every 5 real-time seconds, log an overview of how long each step in the commit phase took.
           These logs will help the Atlas team better understand which parts of committing transactions may be slow, so that we can improve on it.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3273>`__)

    *    - |metrics| |improved|
         - The ``millisSinceLastSweptTs`` metric for targeted sweep now updates at the same frequency as the ``lastSweptTimestamp`` metric.
           This will result in a much smoother graph for the former metric instead of the current sawtooth graph.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3265>`__)

    *    - |fixed|
         - We now page with a smaller batch size when looking at the sweepable cells.
           We also batch targeted sweep deletes in smaller batches.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3283>`__)

    *    - |fixed|
         - Fixed an issue in targeted sweep where reading from the sweep queue when there are more than the specified batch size entries can cause some entries to be skipped.
           This is unlikely to have affected anyone because the default batch size used was very large.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3284>`__)

    *    - |improved| |metrics|
         - AtlasDB now publishes timers tracking time taken to setup a transaction task before it is run, and time taken to tear down the task after it is done before runTaskWith* returns.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3281>`__)

    *    - |improved| |logs|
         - Added logging for leadership election code.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3275>`__)

=======
v0.91.0
=======

18 June 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |devbreak|
         - AtlasDB metrics are no longer a static singleton, and are now created upon construction of relevant classes.
           This allows internal users to construct multiple AtlasDBs and get meaningful metrics. Many constructors have
           been broken due to this change.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3237>`__)

    *    - |devbreak|
         - Refactored the TransactionManager inheritance tree to consolidate all relevant methods into a single interface.
           Functionally, any TransactionManager created using TransactionManagers will provide the serializable and snapshot
           isolation guarantees provided by a SerializableTransactionManager. Constructing TransactionManagers via this class
           should result in only a minor dev break as a result of this change. This will make it easier to transparently wrap
           TransactionManagers to extend their functionality.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3188>`__)

    *    - |fixed|
         - The delete executor now uses daemon threads, so is less likely to cause failure to shutdown.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3271>`__)

    *    - |fixed|
         - Fixed an issue where starting an HA Oracle-backed client may fail due to constraint violation.
           The issue occurred when multiple nodes attempted to insert the same metadata.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3269>`__)

    *    - |changed| |metrics|
         - Sweep metrics have been reworked based on their observed usefulness in the field.
           ``tableBeingSwept`` is removed, as it is observed that it is not ingested as expected. Users can use service logs to track the table being swept.
           ``cellTimestampPairsExamined``, ``staleValuesDeleted`` and ``sweepTimeSweeping`` are being tracked by counters, instead of meters now. This change is done as it is observed that periodically sampled gauge readings are not useful if the frequency is lower than gauge update frequency. Now, these values will be accumulating over time. Users can take the difference of values of two successive points to track the process.
           Sweep now exposes the following metrics with the common prefix ``com.palantir.atlasdb.sweep.metrics.LegacySweepMetrics.`` (To be better distinguished from ``TargetedSweepMetrics``):

              - ``cellTimestampPairsExamined``
              - ``staleValuesDeleted``
              - ``sweepTimeSweeping``
              - ``sweepTimeElapsedSinceStart``
              - ``sweepError``

           (`Pull Request <https://github.com/palantir/atlasdb/pull/3244>`__)

=======
v0.90.0
=======

11 June 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - When writing to Cassandra, the internal write timestamp for writes of sweep sentinels, range tombstones and deletes to regular tables are now approximately fresh timestamps from the timestamp service, as opposed to being an arbitrary hardcoded value or related to the transaction's start timestamp.
           This should improve Cassandra's ability to purge droppable tombstones at compaction time, particularly in tables that see heavy volumes of overwrites and sweeping.

           Note that this only applies if you have created your Transaction Manager through the ``TransactionManagers`` factory.
           If you are creating your transaction manager elsewhere, you should supply a suitable ``freshTimestampProvider`` in initialization.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3224>`__)

    *    - |new| |improved|
         - Targeted sweep now also sweeps stream stores.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3240>`__)

           Note that targeted sweep is considered a beta feature as it is not fully functional yet.
           Consult with the AtlasDB team if you wish to use targeted sweep in addition to, or instead of, standard sweep.

    *    - |fixed|
         - Targeted sweep will no longer sweep cells from transactions that were committed after the sweep timestamp.
           Instead, targeted sweep will not proceed for that shard and strategy until the sweep timestamp progresses far enough.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3253>`__)

    *    - |fixed|
         - Fixed an issue where ``getRowsColumnRange`` would return no results if the number of rows was more than the batch hint.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3249>`__)

    *    - |devbreak|
         - Dropwizard transitive dependencies have been removed from the ``atlasdb-config`` subproject.
           Usages of ``AtlasDbConfigs`` for config parsing still support discovering subtypes of config, as we ship AtlasDB with an implementation of Dropwizard's `DiscoverableSubtypeResolver <https://github.com/dropwizard/dropwizard/blob/master/dropwizard-jackson/src/main/java/io/dropwizard/jackson/DiscoverableSubtypeResolver.java>`__.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3218>`__)

    *    - |devbreak|
         - ``AtlasDbFactory`` now takes an additional ``LongSupplier`` parameter when creating a key-value-service that is intended to be a source of fresh timestamps from the timestamp service.
           Please contact the AtlasDB team if you are uncertain what should be passed here.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3224>`__)

    *    - |improved|
         - The unbounded ``CommitTsLoader`` has been renamed to ``CommitTsCache`` and now has an eviction policy to prevent memory leaks.
           Background sweep now reuses this cache for iterations of sweep instead of recreating it every iteration.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3256>`__)

    *    - |fixed|
         - Some users of AtlasDB rely on being able to abort transactions which are in progress. Until the last release of AtlasDB, this worked successfully, however this was only the case because before
           an assert could throw an AssertionError, an NPE was thrown by different code. Now, the assertion error is not thrown.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3254>`__)

=======
v0.89.0
=======

6 June 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - When determining if large sets of candidate cells were part of committed transactions, Background and Targeted Sweep will now read smaller batches of timestamps from the transaction service in serial.
           Previously, though these reads were re-partitioned into smaller batches, the batch requests were made in parallel which could monopolise Atlas client-side as well as KVS-side resources.
           There may be a small performance regression here, though this change promotes better stability for the underlying key-value-service especially in the presence of wide rows.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3245>`__)

    *    - |userbreak|
         - The size of batches that are used when the ``CommitTsLoader`` loads timestamps as part of sweep is now set to be a non-configurable 50,000.
           This used to be configured via the ``fetchBatchSize`` parameter in Cassandra config.
           Other workflows that use this parameter continue to respect it.
           If you have a use case for configuring this specifically, please contact the AtlasDB team.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3245>`__)

    *    - |fixed| |devbreak|
         - The ``Transaction.getRowsColumnRange`` method that returns an iterator now throws for ``SERIALIZABLE`` conflict handlers.
           This functionality was never implemented correctly and never offered the serializable guarantee.
           The method now throws an ``UnsupportedOperationException`` in this case.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3200>`__)

    *    - |devbreak|
         - Due to lack of use, we have deleted the AtlasDB Dropwizard bundle.
           Users who need Atlas Console and CLI functionality are encouraged to use the respective distributions.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3231>`__)

    *    - |new| |metrics|
         - Added a new tagged metric for targeted sweep showing approximate time in milliseconds since the last swept timestamp has been issued.
           This metric can be used to estimate how far targeted sweep is lagging behind the current moment in time.
           The metric is ``com.palantir.atlasdb.sweep.metrics.TargetedSweepMetrics.millisSinceLastSweptTs`` and is tagged with the sweep strategy used.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3217>`__)

    *    - |fixed|
         - Atlas no longer throws if you read the same column range twice in a serializable transaction.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3239>`__)

    *    - |fixed|
         - We no longer treat CAS failure in Cassandra as a Cassandra level issue, meaning that we won't denylist connections due to a failed CAS.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3215>`__)

    *    - |improved|
         - ``SnapshotTransaction`` now asynchronously deletes values for transactions that get rolled back.
           This restores the behaviour from before the previous `fix <https://github.com/palantir/atlasdb/pull/3199>`__, except that the parent transaction no longer waits for the delete to finish.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3219>`__)

    *    - |fixed|
         - Fixed an issue occurring during transaction commits, where a failure to putUnlessExists a commit timestamp caused an NPE, leading to a confusing error message.
           Previously, the method determining whether the transaction had committed successfully or been aborted would hit a code path that would always result in an NPE.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3205>`__)

    *    - |improved|
         - Increased PTExecutors default thread timeout from 100 milliseconds to 5 seconds to avoid recreating threads unnecessarily.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3208>`__)

=======
v0.88.0
=======

30 May 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |devbreak| |new|
         - KVS method ``deleteAllTimestamps`` now also takes a boolean argument specifying if garbage deletion sentinels should also be deleted.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3212>`__)

    *    - |new|
         - AtlasDB now implements targeted sweep using a sweep queue.
           As long as the ``enableSweepQueueWrites`` property of the ``targetedSweep`` configuration is set to true, information about each transactional write and delete will be persisted into the sweep queue.
           If ``targetedSweep`` is enabled in AtlasDB runtime configurations, background threads will read the persisted information from the sweep queue and delete stale data from the kvs.
           For more details on targeted sweep, please refer to the :ref:`targeted sweep docs<targeted-sweep>`.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3094>`__)

           Note that targeted sweep is considered a beta feature as it is not fully functional yet.
           Consult with the AtlasDB team if you wish to use targeted sweep in addition to, or instead of, standard sweep.

    *    - |new| |metrics|
         - Added tagged targeted sweep metrics for conservative and thorough sweep.
           The metrics show the cumulative number of enqueued writes, entries read, tombstones put, and aborted cells deleted.
           Additionally, there are metrics for the sweep timestamp of the last sweep iteration and for the lowest last swept timestamp across all shards.
           The metrics, tagged with the sweep strategy used, are as follws (with the common prefix ``com.palantir.atlasdb.sweep.metrics.TargetedSweepMetrics.``):

              - ``enqueuedWrites``
              - ``entriesRead``
              - ``tombstonesPut``
              - ``abortedWritesDeleted``
              - ``sweepTimestamp``
              - ``lastSweptTimestamp``

           (`Pull Request <https://github.com/palantir/atlasdb/pull/3202>`__)

    *    - |improved| |logs|
         - Added logging of the values used to determine which table to sweep, provides more insight into why tables are being swept and others aren't.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2829>`__)

    *    - |improved|
         - http-remoting has been upgraded to 3.22.0 (was 3.14.0).
           This release fixes several issues with communication between Atlas servers and a QoS service, if configured (especially in HA configurations).
           Note that this change does not affect communication between timelock nodes, or between an Atlas client and timelock, as these do not currently use remoting.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3196>`__)

=======
v0.87.0
=======

25 May 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - ``SnapshotTransaction`` will no longer attempt to delete values for transactions that get rolled back.
           The deletes were (necessarily) run at consistency ``ALL``, meaning that if aborted data was present, read
           transactions had significantly impaired performance if a database node was down.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3199>`__)

=======
v0.86.0
=======

23 May 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - The Cassandra key value service is now guaranteed to return getRowsColumnRange results in the correct order.
           Previously while paging over row dynamic columns, the first batchHint results are ordered lexicographically,
           whilst the remainder are hashmap ordered in chunks of batchHint. In practice, when paging this can lead to
           entirely incorrect, duplicate results being returned. Now, they are returned in order.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3184>`__)

    *    - |fixed|
         - Fixed a race condition where requests to a node can fail with NotCurrentLeaderException,
           even though that node just gained leadership.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3183>`__)

=======
v0.85.0
=======

18 May 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Snapshot transaction is now guaranteed to return getRowsColumnRange results in the correct order.
           Previously while paging over row dynamic columns, if uncommitted or aborted transaction data was
           seen, it would be placed at the end of the list, instead of at the start, meaning that the results
           are mostly (but not entirely) in sorted order. In practice, this leads to duplicate results in paging,
           and on serializable tables, transactions that paradoxically conflict with themselves.
           Now, they are guaranteed to be returned in order, which removes this issue.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3174>`__)

=======
v0.84.0
=======

16 May 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - Timelock will now have more debugging info if the paxos directories fail to be created on startup.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3156>`__)

    *    - |improved|
         - Move a complicated and elsewhere overridden method from AbstractKeyValueService into DbKvs
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3159>`__)

    *    - |fixed|
         - The (Thrift-backed) ``CassandraKeyValueService`` now returns correctly for CQL queries that return null.
           Previously, they would throw an exception when we attempted to log information about the response.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3158>`__)

=======
v0.83.0
=======

10 May 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - If we make a successful request to a Cassandra client, we now remove it from the overall Cassandra service's denylist.
           Previously, removal from the denylist would only occur after a background thread successfully refreshed the pool, meaning that requests may become stuck if Cassandra was rolling restarted.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3145>`__)

    *    - |fixed|
         - The Cassandra client pool now respects the ``maxRetriesOnHost`` config option, and will not try a single operation beyond that many times on the same node.
           Previously, under certain kinds of exceptions (such as ``TTransportException``), we would repeatedly retry the operation on the same node up to ``maxTriesTotal`` times.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3145>`__)

    *    - |fixed| |devbreak|
         - Any ongoing Cassandra schema mutations are now given two minutes to complete upon closing a transaction manager, decreasing the chance that the schema mutation lock is lost.
           Some exceptions thrown due to schema mutation failures now have type ``UncheckedExecutionException``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3152>`__)

=======
v0.82.2
=======

4 May 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - ``SerializableTransaction`` now initialises internal state correctly.
           Previously, we would throw an exception if multiple equivalent column range selections for different rows needed to be checked in the same transaction.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3147>`__)

=======
v0.82.1
=======

1 May 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Specifying tables in configuration for sweep priority overrides now works properly.
           Previously, attempting to deserialize configurations with these overrides would cause errors.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3136>`__)

=======
v0.82.0
=======

1 May 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - AtlasDB now partitions versions of cells to be swept into batches more robustly and more efficiently.
           Previously, this could cause stack overflows when sweeping a very wide row, because the partitioning algorithm attempted to traverse a recursive hierarchy of sublists.
           Also, previously, partitioning would require time quadratic in the number of versions present in the row; it now takes linear time.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3095>`__)

    *    - |new|
         - Users can now explicitly specify specific tables for the background sweeper to (1) prioritise above other tables, or (2) denylist.
           This is done as part of live-reloadable configuration, though note that background sweep will conclude its current iteration before switching to a priority table / away from a blacklisted table, as appropriate.
           Please see :ref:`Sweep Priority Overrides <sweep-priority-overrides>` for more details.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3090>`__)

    *    - |fixed|
         - Transaction managers now shut down threads associated with the QoS client and TimeLock lock refresher when they are closed.
           Previously, these threads would continue running and needlessly using resources.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3096>`__)

    *    - |fixed|
         - The ``_locks`` table is now created with a deterministic column family ID.
           This means that multi-node installations will no longer create multiple locks tables on first start-up.
           Note that new installations using versions of Cassandra prior to 2.1.13, 2.2.5, 3.0.3 or 3.2 will fail to create this table, as we rely on syntax introduced by the fix to `CASSANDRA-9179 <https://issues.apache.org/jira/browse/CASSANDRA-9179>`__.
           Existing installations will be unaffected.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3088>`__)

    *    - |fixed|
         - OkHttp is not handling ``Thread.interrupt()`` well, and calling interrupts repeatedly may cause corrupted http clients.
           This would cause TimeLock clients to appear silent (requests would not be accepted or logged), but would not have affected data integrity.
           To avoid this issue, our Feign client is now wrapped with an ``ExceptionCountingRefreshingClient``, which will detect and refresh corrupted clients.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3121>`__)

    *    - |fixed| |devbreak|
         - ``LoggingArgs::isSafeForLogging(TableReference, Cell)`` was removed, as it behaved unexpectedly and could leak information.
           Previously, it returned true only if the cell's row matched the name of a row component which was declared as safe.
           However, knowledge of the existence of such a cell may not actually be safe.
           There currently isn't an API for declaring specific row or dynamic column components as safe; please contact the AtlasDB team if you have such a use case.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3093>`__)

    *    - |improved| |logs|
         - Expired lock refreshes now tell you which locks expired, instead of just their refreshing token id.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3125>`__)

    *    - |improved|
         - The strategy for choosing the table to compact was adjusted to avoid the case when the same table is chosen multiple times in a row, even if it was not swept between compactions.
           Previously, the strategy to choose the table to compact was:

             1. if possible choose a table that was swept but not compacted
             2. otherwise choose a table for which the time passed between last compact and last swept was longer

           When all tables are swept and afterward compacted, the last point above could choose to compact the same table because ``lastSweptTime - lastCompactTime`` is negative and largest among all tables.

           The new strategy is:

             1. if possible choose a table that was swept but not compacted
             2. if there is no uncompacted table then choose a table swept further after it was compacted
             3. otherwise, randomly choose a table after filtering out the ones compacted in the past hour

           (`Pull Request <https://github.com/palantir/atlasdb/pull/3100>`__)

    *    - |improved| |logs|
         - ``kvs-slow-log`` messages now also include start time of the operation for easier debugging.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3117>`__)

    *    - |improved| |logs|
         - AtlasDB internal tables will no longer produce warning messages about hotspotting.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3126>`__)

=======
v0.81.0
=======

19 April 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved| |metrics|
         - Async TimeLock Service metric timers are now tagged with (1) the relevant clients, and (2) whether the current node is the leader or not.
           This allows for easier analysis and consumption of these metrics.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3075>`__)

    *    - |improved|
         - Common annotations can now be imported via the commons-annotations library, instead of needing to pull in atlasdb-commons.
           Existing code that uses atlasdb-commons for the annotations will still be able to resolve them.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3089>`__)

    *    - |fixed|
         - Logs in ``CassandraRequestExceptionHandler`` are logged using a logger named after that class instead of ``CassandraClientPool``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3092>`__)

    *    - |improved| |devbreak|
         - Bumped several libraries to get past known security vulns:

           - Cassandra Thrift and CQL libs
           - Jackson
           - Logback
           - Netty (indirectly via cassandra lib bump)

           (`Pull Request <https://github.com/palantir/atlasdb/pull/3084>`__)

=======
v0.80.0
=======

04 April 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed| |devbreak|
         - Centralize how ``PersistentLockManager`` is created in a dagger context.
           Also, removed the old constructor for ``CellsSweeper``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3047>`__)

    *    - |improved| |logs|
         - Downgraded "Tried to connect to Cassandra {} times" logs from ``ERROR`` to ``WARN``, and stopped printing the stack trace.
           An exception is thrown to the service who made the request; this service has the opportunity to log at a higher level if desired.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3069>`__)

    *    - |new|
         - AtlasDB now supports runtime configuration of throttling for stream stores when streams are written block by block in a non-transactional fashion.
           Previously, such streams would be written using a separate transaction for each block, though in cases where data volume is high this may still cause load on the key-value-service Atlas is using.
           Please note that if you wish to use this feature, you will need to regenerate your Atlas schemas and suitably inject the stream persistence configuration into your stream stores.
           However, if you do not intend to use this feature, no action is required, and your stream stores' behaviour will not be changed.
           Note that enabling throttling may make nontransactional ``storeStream`` operations take longer, though the length of constituent transactions should not be affected.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3073>`__)

    *    - |new|
         - AtlasDB now schedules KVS compactions on a background thread, as opposed to triggering a compaction after each table was swept.
           This allows for better control over KVS load arising from compactions.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3058>`__)

    *    - |new|
         - AtlasDB now supports configuration of a maintenance mode for compactions.
           If compactions are run in maintenance mode, AtlasDB may perform more costly operations which may be able to recover more space.
           For example, for Oracle KVS, ``SHRINK SPACE`` (which acquires locks on the entire table) will only be run if compactions are carried out in maintenance mode.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3058>`__)

    *    - |fixed|
         - Fixed a bug that causes Cassandra clients to return to the pool even if they have thrown blacklisted exceptions.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3063>`__)

    *    - |fixed|
         - Fix NPE if PaxosLeaderElectionServiceBuilder's new field onlyLogOnQuorumFailure is never set.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3074>`__)

    *    - |new|
         - If using TimeLock, AtlasDB now checks the value of a fresh timestamp against the unreadable timestamp on startup, failing if the fresh timestamp is smaller.
           That implies clocks went backwards; doing this mitigates the damage that a bogus TimeLock migration or other corruption of TimeLock can do.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3018>`__)

    *    - |improved|
         - Applications can now easily determine whether their Timelock cluster is healthy by querying ``TransactionManager.getTimelockServiceStatus().isHealthy()``.
           This returns true only if a healthy connection to timelock service is established.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3030>`__)

=======
v0.79.0
=======

20 March 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved| |devbreak|
         - Guava has been updated from 21.0 to 23.6-jre.
           This unblocks users using libraries which have dependencies on more recent versions of Guava, owing to API changes in ``SimpleTimeLimiter``, among other classes.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3038>`__)

    *    - |improved| |metrics|
         - Sweep metrics are now updated to the result value of the last run iteration of sweep instead of the cumulative values for the run of sweep on the table.
           This has been done in order to improve the granularity of the metrics, since cumulative results can be several orders of magnitude larger, thus obfuscating the delta.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3055>`__)

    *    - |new|
         - Added a new parameter ``addressTranslation`` to ``CassandraKeyValueServiceConfig``.
           This parameter is a static map specifying how internal Cassandra endpoints should be translated to InetSocketAddresses.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3040>`__)

    *    - |fixed|
         - The Cassandra client pool is now cleaned up in the event of a failure to construct the Cassandra KVS (e.g. because we lost our connection to Cassandra midway).
           Previously, the client pool was not shut down, leading to a thread leak.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3006>`__)

    *    - |improved| |logs|
         - Log an ERROR in the case of failure to create a Cell due to a key greater than 1500 bytes. Previously we logged at DEBUG.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3034>`__)

    *    - |fixed|
         - ``clean-cass-locks-state`` command is now using Atlas namespace as Cassandra keyspace if provided.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3035>`__)

    *    - |improved| |logs|
         - Logging exceptions in the case of quorum is runtime configurable now, using ``only-log-on-quorum-failure`` flag, for external timelock services. Previously it was set to true by default.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3057>`__)

=======
v0.78.0
=======

2 March 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |new|
         - The ``TransactionManagers`` builder now optionally accepts a ``Callback`` object.
           If ``initializeAsync`` is set to true, then this callback will be run after all the initialization prerequisites for the TransactionManager have been met, and the TransactionManager will start returning true on calls to its ``isInitialized()`` method only once the callback has returned.
           If ``initializeAsync`` is set to false, then this callback will be run just before the TransactionManager is returned, blocking until it is done.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3011>`__)

    *    - |fixed|
         - SerializableTransactionManager can now be closed even if it is not initialized yet.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3011>`__)

    *    - |new| |changed| |metrics|
         - Sweep metrics have been reworked based on their observed usefulness in the field.
           In particular, histograms and most of the meters were replaced with gauges that expose last known values of tracked sweep results.
           Tagged metrics have been removed as well, and were replaced by a gauge ``tableBeingSwept`` that exposes the name of the table being swept, if it is safe for logging.
           Sweep metrics ``cellTimestampPairsExamined`` and ``staleValuesDeleted`` are now updated after every batch of deletes instead of waiting until all of the batches are processed.
           Sweep now exposes the following metrics with the common prefix ``com.palantir.atlasdb.sweep.metrics.SweepMetric.``:

              - ``tableBeingSwept``
              - ``cellTimestampPairsExamined``
              - ``staleValuesDeleted``
              - ``sweepTimeSweeping``
              - ``sweepTimeElapsedSinceStart``
              - ``sweepError``

           (`Pull Request <https://github.com/palantir/atlasdb/pull/2951>`__)

    *    - |fixed|
         - LoggingArgs no longer throws when it tries to hydrate invalid table metadata.
           This fixes an issue that prevented AtlasDB to start after performing a KVS migration.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3006>`__)

    *    - |changed|
         - Changes the default scrubber behavior to aggressive scrub (synchronous with scrub request).
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3009>`__)

    *    - |fixed|
         - Fixed a bug that can causes the background sweep thread to fail to shut down cleanly, hanging the application.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3023>`__)

    *    - |improved|
         - Remove a round trip from read only transactions
           not involving thoroughly swept tables.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3020>`__)

=======
v0.77.0
=======

16 February 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |changed|
         - AtlasDB migration CLI no longer drops the temporary table used during migration and instead truncates it.
           This avoids an issue where AtlasDB would refuse to start after a migration because it would try to hydrate empty table metadata for the above table.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3000>`__)

    *    - |changed|
         - Upgraded Postgres jdbc driver to 42.2.1 (from 9.4.1209).
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2985>`__)

    *    - |fixed|
         - Fix NPE when warming conflict detection cache if table is being created.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2993>`__)

    *    - |improved| |devbreak|
         - Introduced configurable ``writeThreshold`` and ``writeSizeThreshold`` parameters for when to write stats for the Sweep prioritization.
           Also reduce the defaults to flush write stats on 32MB overall write size and 2k cells.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2998>`__)

    *    - |fixed|
         - Fix ``SnapshotTransaction#getRows`` to apply ``ColumnSelection`` when there are local writes.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/3008>`__)

    *    - |fixed|
         - CassandraKVS sstable size in MB was not being correctly set.
           This resulted in requirements on the entire cluster being up during startup of certain stacks.
           CF metadata mismatch messages are also now correctly safety marked for logging.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2989>`__)

=======
v0.76.0
=======

12 February 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Fixed a bug which would make sweep deletes not be compacted by Cassandra.
           Over time this would lead to tombstones being accumulated in the DB and disk space not being reclaimed.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2968>`__)

    *    - |fixed|
         - When TransactionManagers doesn't return successfully, we leaked resources depending on which step of the initialization failed.
           Now resources are properly closed and freed.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2964>`__)

    *    - |fixed|
         - Fixed a bug where Cassandra clients' input buffers were left in an invalid state before returning the client to the pool, manifesting in NPEs in the Thrift layer.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2971>`__)

    *    - |improved| |new|
         - Added a new parameter ``conservativeRequestExceptionHandler`` to ``CassandraKeyValueServiceRuntimeConfig``.
           Setting this parameter to true will enable more conservative retrying logic for requests, including longer backoffs and not retrying on the same host when encoutering an exception that is indicative of high Cassandra load, e.g., TimeoutExceptions.
           This parameter is live-reloadable, and reloading it will affect in-flight requests, with the caveat that once a request gives up on a node, it will not retry that node again even if we disable conservative retrying.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2960>`__)

    *    - |improved|
         - AtlasDB CLIs now allow a runtime config to be passed in.
           This allows the CLIs to be used with products that are configured to use timelock and have the timelock block in the runtime config.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2937>`__)

    *    - |improved| |devbreak|
         - AtlasDbConfigs now supports parsing of both install and runtime configuration.
           As part of these changes, ``load``, ``loadFromString`` and other methods in ``AtlasDbConfigs`` now take a type parameter.
           To fix existing usage, please pass in ``AtlasDbConfig.class`` as the type parameter to these functions.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2937>`__)

    *    - |fixed|
         - Fixed a bug where the CleanCassLocksState CLI would not start because the Cassandra locks were in a bad state.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2948>`__)

    *    - |fixed|
         - Close AsyncInitializer executors. This should reduce memory pressure of clients after startup.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2945>`__)

    *    - |improved|
         - Added a TimeLock healthcheck for signalling that no leader election has been triggered.
           This will allow TimeLock itself to broadcast a HEALTHY status even without a leader.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2939>`__)

    *    - |improved|
         - Index tables can now be marked as safe for logging.
           If you use indexes, please add ``allSafeForLogging()`` on their definition (where reasonable).
           This makes all AtlasDB tables able to be marked as safe for logging.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2940>`__)

    *    - |improved|
         - Make some values of ``CassandraKeyValueServiceConfig`` live-reloadable.
           To check which parameters are live-reloadable, check the ``CassandraKeyValueServiceRuntimeConfig`` class.
           Docs about this config can be found :ref:`here <atlas-config>` and :ref:`here <cassandra-configuration>`.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2896>`__)

    *    - |devbreak|
         - Renamed the method used to create LockAndTimestampServices by the CLI commands and AtlasConsole.
           Please update usages of ``createLockAndTimestampServices`` to ``createLockAndTimestampServicesForCli``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2896>`__)

    *    - |improved| |logs|
         - Sweep now logs the number of cells it is deleting when performing a single batch of deletes.
           This is useful for visibility of Sweep progress; previously, Sweep would only log when a top-level batch was complete, meaning that for highly versioned rows Sweep would only log after deleting all stale versions of said row.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2958>`__)

    *    - |improved|
         - The sweep-table endpoint now returns HTTP status 400 instead of 500, when asked to sweep a non-existent table.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2936>`__)

    *    - |improved| |metrics|
         - Atlas now records the number of cells written over time, if you are using Cassandra KVS.
           This metric is reported under ``com.palantir.atlasdb.keyvalue.cassandra.CassandraClient.cellsWritten``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2967>`__)
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2974>`__)

    *    - |improved|
         - ``ExecutorInheritableThreadLocal`` from ``commons-executors`` has been split out into a ``commons-executors-api`` dependent project with no dependencies.
           This allows api projects outside of atlasdb to use ``ExecutorInheritableThreadLocal`` without pulling in the dependencies of ``commons-executors``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2961>`__)

=======
v0.75.0
=======

29 January 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed| |userbreak|
         - AtlasDB will now *fail to start* if a TimeLock block is included in the initial runtime configuration, but the install configuration is set up with a leader block or with remote timestamp and lock blocks.
           Previously, AtlasDB would start successfully under these conditions, but the TimeLock block in the runtime configuration would be silently ignored.
           Note that the decision on whether to use TimeLock or another source of timestamps and locks is made at install-time.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2850>`__)

    *    - |improved| |userbreak|
         - AtlasDB users can now specify the usage of TimeLock Server purely by including a TimeLock block in the initial runtime configuration.
           Previously, AtlasDB users would need to specify that they were using TimeLock in the install configuration, possibly with an empty object (``timelock: {}``).
           This is a change from previous behaviour in cases where users specified an embedded install configuration but a TimeLock block in the runtime configuration; previously, the embedded configuration would have been selected, while now the TimeLock configuration will be selected.

           Also, users with scripts that depend on supplying a default runtime configuration may need to be careful to ensure that TimeLock configuration is preserved when such scripts are run.
           That said, AtlasDB will fail to start if trying to access a key-value service where TimeLock has been used as a source of timestamps without going through TimeLock, so we don't think there is a risk of data corruption.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2850>`__)

    *    - |fixed| |metrics|
         - Fixed metric re-registration log spam in ``TokenRangeWriteLogger``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2913>`__)

    *    - |fixed| |devbreak|
         - AtlasDB clients will receive a ``QosException.Throttle`` for requests that are throttled and http-remoting
           should handle them appropriately based on the backOff strategy provided by the application. Note that this is
           an experimental feature and we do not expect it to be enabled anywhere. This is a dev break as the exception type
           has changed from ``RateLimitExceededException`` to ``QosException.Throttle``.
           (`Throttle <https://github.com/palantir/http-remoting/blob/a14a0894c2f5d1a415c5ee2727e9c79d73255b7b/okhttp-clients/src/main/java/com/palantir/remoting3/okhttp/RemotingOkHttpCall.java#L221-L233>`__)
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2926>`__)

    *    - |improved| |metrics|
         - Use tags in sweep outcome metrics instead of using each name per outcome.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2927>`__)

    *    - |improved| |logs|
         - Log message for leaked sweep/backup lock is now WARN rather than INFO.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2912>`__)

    *    - |improved| |logs| |metrics|
         - ``TokenRangeWrite`` metrics are calculated every 1000 writes so we can chart metrics for smaller tables.  Logging now happens every 6 hours regardless of number of writes (although there must be at least 1).
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2910>`__)

    *    - |improved| |logs|
         - ``CassandraClient`` kvs-slow-logs have been improved. They now contain the duration of the call and information
           about the results from the KVS.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2924>`__)

    *    - |changed|
         - Updated our Guava dependency from 18.0 to 20.0. This should unblock downstream products from upgrading to Guava 22.0.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2926>`__)

    *    - |changed|
         - Updated our http-remoting dependency from 3.5.1 to 3.14.0.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2926>`__)


=======
v0.74.0
=======

23 January 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved| |logs|
         - AtlasDB internal table names are now safe for logging.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2903>`__)

    *    - |improved| |metrics|
         - ``BackgroundSweeperImpl`` now logs if there's an uncaught exception.  Added 2 new outcomes for normal and abnormal shutdown to allow closer monitoring.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2884>`__)

    *    - |improved|
         - The ``LockAwareTransactionManager`` pre-commit checks that verify that locks are still held have been generalized to support arbitrary pre-commit checks.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2846>`__)

    *    - |devbreak|
         - ``AtlasDbConstants.GENERIC_TABLE_METADATA`` is now safe for logging, if you are using this as the metadata to
           create table names that shouldn't be logged in the internal logging framework, do not use this metadata.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2903>`__)

    *    - |devbreak| |improved|
         - The ``partitionStrategy`` parameter in AtlasDB table metadata has been removed; products that explicitly specify partition strategies in their schemas will need to remove them.
           The value of this parameter was never actually read; behaviour would have been identical regardless of what this was specified to (if at all).
           This change was made to simplify the API and also remove any illusion that specifying the ``partitionStrategy`` would have done anything.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2864>`__)

    *    - |devbreak|
         - The protobuf library has been upgraded to 3.5.1. Dependent projects will need to update their dependencies.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2887>`__)

    *    - |fixed|
         - V2 Schemas which use ``ValueType.BLOB`` will now compile.
           Previously, compilation failed with an ``IllegalArgumentException`` from Java Poet, as we assumed Java versions of ``ValueType`` were always associated with object types.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2899>`__)

    *    - |fixed| |metrics|
         - ``TokenRangeWriteLogger`` now registers different metric names per table even if all are unsafe.  We instead tag with an obfuscated version of the name which is safe for logging.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2889>`__)

    *    - |fixed|
         - Stop sweeping when the sweep thread is interrupted.
           Previously, when services were shutting down, the background sweeper thread continuously logged warnings
           due to a closed ``TransactionManager``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2900>`__)

    *    - |devbreak|
         - Removed ``CassandraKeyValueServiceConfigManager``. If you're affected by this, please contact the AtlasDB team.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2886>`__)

=======
v0.73.1
=======

16 January 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Fix a NPE in that could happen in the Sweep background thread. In this scenario, sweep would get stuck and not be able to proceed.
           The regression was introduced with (`#2860 <https://github.com/palantir/atlasdb/pull/2860>`__), in version 0.73.0.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2883>`__)

    *    - |fixed|
         - Qos clients will query the service every 2 seconds instead of every client request. This should prevent too many requests to the service.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2872>`__)

    *    - |fixed|
         - All Atlas executor services now run tasks wrapped in http-remoting utilities to preserve trace logging.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2868>`__)
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2874>`__)

=======
v0.73.0
=======

16 January 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - On Cassandra KVS, sweep reads data from Cassandra in parallel, resulting in improved performance.
           The parallelism can be changed by adjusting ``sweepReadThreads`` in Cassandra KVS config (default 16).
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2860>`__)

    *    - |improved|
         - AtlasDB now throws an error during schema code generation stage if index table name length exceeds KVS table name length limits.
           To override this, please specify ``ignoreTableNameLengthChecks()`` on your schema.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2862>`__)

===========
v0.73.0-rc2
===========

12 January 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |new|
         - Qos Service: AtlasDB now supports a QosService which can rate-limit clients.
           Please note that this feature is currently experimental; if you wish to use it, please contact the AtlasDB team.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2700>`__)

    *    - |new|
         - The JDBC URL for Oracle can now be overridden in the configuration.
           The parameter path is ``keyValueService/connection/url``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2837>`__)

===========
v0.73.0-rc1
===========

11 January 2018

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved| |logs| |metrics|
         - Allow StreamStore table names to be marked as safe. This will make StreamStore tables appear correctly on our logs and metrics.
           When building a StreamStore, please use ``.tableNameLogSafety(TableMetadataPersistence.LogSafety.SAFE)`` to mark the table name as safe.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2835>`__)

    *    - |improved|
         - Sweep stats are updated more often when large writes are being made.
           ``SweepStatsKVS`` now tracks the size of modifications being made to the underlying KVS and will write when a threshold is passed.
           Previously, sweep stats were updated every 65536 writes, but this could be a significant amount of data if written to the stream store.
           We now also track the size of the writes and if this is greater than 1GB, we flush the stats.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2792>`__)

    *    - |improved|
         - Improvements to how sweep prioritises which tables to sweep; should allow better reclaiming of space from stream stores.
           Stream store value tables are now more likely to be chosen because they contain lots of data per write.
           We ensure we sweep index tables before value tables, and allow a gap after sweeping index tables and before sweeping value tables.
           We wait 3 days between sweeps of a value table to prevent unnecessary work, allow other tables to be swept and tombstones to be compacted away.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2793>`__)

    *    - |fixed|
         - ``SweepResults.getCellTsPairsExamined`` now returns the correct result when sweep is run over multiple batches.
           Previously, the result would only count cell-ts pairs examined in the last batch.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2830>`__)

    *    - |fixed|
         - Further reduced memory pressure on sweep for Cassandra KVS, by rewriting one of the CQL queries.
           This removes a significant cause of occurrences of Cassandra OOMs that have been seen in the field recently.
           However, performance is significantly degraded on tables with few columns and few overwrites (fixed in 0.73.0).
           (`Pull Request 1 <https://github.com/palantir/atlasdb/pull/2826>`__ and `Pull Request 2 <https://github.com/palantir/atlasdb/pull/2826>`__)

    *    - |fixed| |logs|
         - Safe and Unsafe table name logging args are now different, fixed unreleased bug where tables names were logged as Safe
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2838>`__)

    *    - |logs|
         - Messages to the ``slow-lock-log`` now log at ``WARN`` rather than ``INFO``, these messages can indicate a problem so we should be sure they are visible.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2828>`__)

    *    - |devbreak|
         - For clarity, we renamed ``ForwardingLockService`` to ``SimplifyingLockService``, since this class also overwrote some of its parent's methods.
           Also, its ``delegate`` method is now public.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2729>`__)

    *    - |improved|
         - Tritium was upgraded to 0.9.0 (from 0.8.4), which provides functionality for de-registration of tagged metrics.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2823>`__)

=======
v0.72.0
=======

13 December 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |new| |improved| |metrics| |logs|
         - Sweep metrics were reworked. Sweep now exposes metrics indicating the total number of cells examined, cells deleted, time spent sweeping, and time elapsed since sweep started on the current table that are updated after each iteration of sweep and separate metrics that are updated after each table is fully swept.
           Additionally, sweep now exposes metrics tagged with table names that expose the total number of cells examined, cells deleted, time spent sweeping per iteration for each table separately.
           Logs will also include the new timing information.
           Sweep now exposes the following metrics with the common prefix ``com.palantir.atlasdb.sweep.metrics.SweepMetric.``:

              - ``cellTimestampPairsExamined.meter.currentIteration``
              - ``cellTimestampPairsExamined.histogram.currentTable``
              - ``cellTimestampPairsExamined.histogram.currentIteration`` (tagged)
              - ``staleValuesDeleted.meter.currentIteration``
              - ``staleValuesDeleted.histogram.currentTable``
              - ``staleValuesDeleted.histogram.currentIteration`` (tagged)
              - ``sweepTimeSweeping.meter.currentIteration``
              - ``sweepTimeSweeping.histogram.currentTable``
              - ``sweepTimeSweeping.histogram.currentIteration`` (tagged)
              - ``sweepTimeElapsedSinceStart.currentValue.currentIteration``
              - ``sweepTimeElapsedSinceStart.histogram.currentTable``

           (`Pull Request <https://github.com/palantir/atlasdb/pull/2672>`__)

    *    - |improved|
         - AtlasDB now provides a configurable ``compactInterval`` (0 by default) option for Postgres, in the Postgres DDL Config.
           A vacuum will be kicked off an a table only if there hasn't been one on the same table in the last ``compactInterval``.
           This will prevent increasing load on Postgres due to queued up vacuums. We would suggest a value of 1-2 days (e.g. ``2d`` or ``2 days``) for this config option
           and would encourage users to test this out and report the results back. We will modify the defaults once this has been field tested.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2767>`__)

    *    - |fixed|
         - The ``LeaderPingHealthCheck`` supplied by ``PaxosLeadershipCreator`` now correctly reports the leadership state of nodes that believe themselves to be the leader.
           Previously, the health check would ping every *other* node in the cluster, resulting in leader nodes reporting that there are no leaders.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2805>`__)

    *    - |fixed|
         - Fixed a bug in LockServiceImpl (caused by a bug in AbstractQueuedSynchronizer) where a race condition could cause a lock to become stuck indefinitely.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2799>`__)

    *    - |devbreak|
         - Deleted the TTL duration field from the ``Cell`` class.
           The interface ``ExpiringKeyValueService`` and implementations ``CassandraExpiringKeyValueService`` and ``CqlExpiringKeyValueService`` have also been removed.
           Additionally, ``StreamTableDefinitionBuilder.expirationStrategy`` has been removed.
           We don't believe that any of these fields or classes were used.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2599>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.71.1
=======

8 December 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Removed an unused dependency from ``atlasdb-api``, fixing a dependency clash in a downstream product.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2798>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.71.0
=======

7 December 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |new|
         - **AtlasDB QoS**: AtlasDB now allows clients to live-reloadably configure read and write limits (in terms of bytes) to rate-limit requests to Cassandra.
           AtlasDB clients will receive a ``RateLimitExceededException`` for requests that are throttled and should handle them appropriately.
           We provide an exception mapper ``RateLimitExceededExceptionMapper`` to map the throttling exceptions to ``429``, but it is upto the application to register the exception mapper.
           Note that this is an experimental feature and applications should generally not enable it by default yet, unless the application has hard read-write limits.
           This should allow us to throttle dynamically in situations where the load on Cassandra is high.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2629>`__)

    *    - |improved|
         - AtlasDB publish of new releases is now done through the internal circleCI build instead of external circleCI.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2783>`__)

    *    - |improved|
         - AtlasDB no longer logs Cassandra retries at level WARN, thus reducing the volume of WARN logs by a significant factor. These logs are now available at INFO.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2776>`__)

    *    - |fixed|
         - Sweep can now make progress after a restore and after the clean transactions CLI is run.
           Earlier, it would fail throwing a ``NullPointerException`` due to failure to read the commit ts.
           This would cause sweep to keep retrying without realising that it will never proceed forward.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2778>`__)

    *    - |fixed|
         - Sweep will no longer run during KVS Migrations.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2784>`__)

    *    - |new| |logs|
         - Cassandra KVS now records how many writes have been made into each token range for each table.
           That information is logged at info every time a table is written to more than a threshold of times (currently 100 000 writes).
           These logs will be invaluable in more easily identifying hotspotting and for using targeted sweep.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2718>`__)

    *    - |new| |metrics|
         - New metric added which reports the probability that a table is being written to unevenly.  ``com.palantir.atlasdb.keyvalue.cassandra.TokenRangeWritesLogger.probabilityDistributionIsNotUniform`` is tagged with the table reference (where safe) and is a probability from 0.0 to 1.0 that the token ranges are being written to unevenly.  Cassandra KVS only.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2764>`__)

    *    - |new|
         - ``TimeLockAgent`` exposes a new method, ``getStatus()``, to be used by the internal TimeLock instance in order to provide a health check.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2730>`__)

    *    - |devbreak|
         - Removed several utility methods that are used by AtlasDB code. ``MathUtils`` has been moved to our large internal product, which was the only place to use it.

              - ``MathUtils`` (entire class)
              - ``IterableUtils`` (``getFirst(it, defaultValue)``, ``mergeIterators``, ``partitionByHash``, ``prepend``, ``transformIterator``)
              - ``IteratorUtils.iteratorDifference``

           (`Pull Request <https://github.com/palantir/atlasdb/pull/2782>`__)

    *    - |new|
         - Added a CLI to read the punch table. The CLI receives an epoch time, in millis, and returns an approximation of the AtlasDB timestamp strictly before the given timestamp.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2775>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.70.1
=======

30 November 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |devbreak| |improved|
         - The ``TransactionManagers`` builder now hooks up the metric registries passed in so that AtlasDB metrics are registered on the specified metric registries.
           Applications no longer should use the ``AtlasDbMetrics.setMetricRegistry`` method to specify a metric registry for AtlasDB.

            .. code:: java

                TransactionManagers.builder()
                    .config(config)
                    .userAgent("ete test")
                    .globalMetricRegistry(new MetricRegistry())
                    .globalTaggedMetricRegistry(DefaultTaggedMetricRegistry.getDefault())
                    .registrar(environment.jersey()::register)
                    .addAllSchemas(ETE_SCHEMAS)
                    .build()
                    .serializable()

           (`Pull Request <https://github.com/palantir/atlasdb/pull/2760>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.70.0
=======

30 November 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - When BackgroundSweeper decides to sweep a StreamStore VALUE table, first sweep the respective StreamStore
           INDEX table.
           Before we just swept the VALUE table, which ended up not deleting any values in the backing store.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2756>`__)

    *    - |devbreak| |metrics|
         - The method ``AtlasDbMetrics.setMetricsRegistries`` was added, to register both the MetricRegistry and the TaggedMetricRegistry.
           Please use it instead of the old ``AtlasDbMetrics.setMetricsRegistry``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2720>`__)

    *    - |improved| |logs|
         - All logging in ``SnapshotTransaction`` now marks its placeholder log arguments as either safe or unsafe.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2755>`__)

    *    - |devbreak| |improved|
         - The previously deprecated ``TransactionManagers.create()`` methods have been removed.
           To create a ``SerializableTransactionManager`` please use the ``TransactionManagers.builder()`` to create a ``TransactionManagers`` object and then call its ``serializable()`` method.
           Furthermore, this builder now requires a ``taggedMetricRegistry`` argument, and is a staged builder, requiring all mandatory parameters to be specified in the following order:
           ``TransactionManagers.config().userAgent().metricRegistry().taggedMetricRegistry()``.
           This avoid runtime errors due to failure to specify all required arguments.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2720>`__)

    *    - |fixed|
         - Fixed a bug where setting ``compressBlocksInDb`` for stream store definitions would result in a much bigger than intended block size.
           This option is also deprecated, as we recommend ``compressStreamsInClient`` instead.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2752>`__)

    *    - |fixed|
         - Fixed an edge case where sweep would loop infinitely on tables that contained only tombstones.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2744>`__)

    *    - |fixed| |metrics|
         - ``MetricsManager`` no longer outputs stack traces to WARN when a metric is registered for a second time.
           The stack trace can still be accessed by turning on TRACE logging for ``com.palantir.atlasdb.util.MetricsManager``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2753>`__)

    *    - |improved| |devbreak|
         - AtlasDB now wraps ``NotCurrentLeaderException`` in ``AtlasDbDependencyException`` when this exception is thrown by TimeLock.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2716>`__)

    *    - |improved|
         - Sweep no longer fetches any values from Cassandra in CONSERVATIVE mode. This results in significantly less data being transferred from Cassandra to the client when sweeping tables with large values, such as stream store tables.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2754>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.69.0
=======

23 November 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Fixed the deletion of values from the StreamStore when configured to hash rowComponents.
           Previously, due to a deserialization bug, we wouldn't delete the streamed data.
           If you think you're affected by this bug, please contact the AtlasDB team to migrate away from this behavior.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2736>`__)

    *    - |fixed|
         - We now avoid Cassandra timeouts caused by running unbounded CQL range scans during sweep.
           In order to assign a bound, we prefetch row keys using thrift, and use these bounds to page internally through rows.
           This issue affected tables configured to use THOROUGH sweep strategy — which could accumulate many rows entirely made up of tombstones —
           when Cassandra is configured as the backing store.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2732>`__)

    *    - |improved|
         - Applications can now easily determine whether their AtlasDB cluster is healthy by querying ``TransactionManager.getKeyValueServiceStatus().isHealthy()``.
           This returns true only if all key value service nodes are up; applications that have sweep and scrub disabled and do not perform schema mutations
           can also treat ``KeyValueServiceStatus.HEALTHY_BUT_NO_SCHEMA_MUTATIONS_OR_DELETES`` as a healthy state.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2678>`__)

    *    - |improved| |devbreak|
         - AtlasDB will now consistently throw an ``AtlasDbDependencyException`` when requests fail due to TimeLock being unavailable.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2677>`__)

    *    - |fixed|
         - ``Throwables.createPalantirRuntimeException`` once again throws ``PalantirInterruptedException`` if the original exception was either ``InterruptedException`` or ``InterruptedIOException``.
           This reverts behaviour introduced in 0.67.0, where we instead threw ``PalantirRuntimeException``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2702>`__)

    *    - |improved|
         - Sweep now waits 1 day after generating a large number of tombstones before sweeping a table again. This behavior only applies when using Cassandra.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2733>`__)

    *    - |fixed| |logs|
         - ``CassandraKeyValueServiceImpl.compactInternally`` no longer logs an error when no compaction manager is configured.
           This message is instead logged once, when the CKVS is instantiated.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2727>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.68.0
=======

16 November 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - HTTP clients for endpoints relating to the Paxos protocols (``PingableLeader``, ``PaxosAcceptor`` and ``PaxosLearner``) now reset themselves after 500 million requests have been executed.
           This was implemented as a workaround for `OkHttp #3670 <https://github.com/square/okhttp/issues/3670>`__ where HTTP/2 connections managed in OkHttp would fail after just over a billion requests owing to an unexpected integer overflow.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2680>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.67.0
=======

15 November 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |new|
         - AtlasDB clients are now able to live reload TimeLock URLs.
           This is required for internal work on running services in Kubernetes.
           We still require that clients are configured to use TimeLock (as opposed to a leader, remote timestamp/lock or embedded service) at install time.
           Note that this change does not affect TimeLock Server, which still requires knowledge of the entire cluster as well.
           Please consult the :ref:`documentation <timelock-client-configuration>` for more detail regarding the config changes needed.
           (`Pull Request 1 <https://github.com/palantir/atlasdb/pull/2621>`__ and
           `Pull Request 2 <https://github.com/palantir/atlasdb/pull/2622>`__)

    *    - |deprecated|
         - The ``servers`` block within an AtlasDB ``timelock`` block is now deprecated.
           Please use the live-reloadable ``servers`` block within the ``timelockRuntime`` block of the runtime configuration instead.
           (`Pull Request 2 <https://github.com/palantir/atlasdb/pull/2622>`__)

    *    - |improved|
         - AtlasDB clients using TimeLock can now start up with knowledge of zero TimeLock nodes.
           Requests to TimeLock will throw ``ServiceUnavailableException`` until the config is live reloaded with one or more nodes.
           If live reloading causes the number of nodes to fall to zero, we also fail gracefully; ``ServiceUnavailableException`` will be thrown until the config is live reloaded with one or more nodes.
           Note that this does not affect remote timestamp, lock or leader configurations; those still require at least one server.
           Also, note that if one is using TimeLock without async initialization, then one still needs to provide information about the TimeLock cluster on startup.
           (`Pull Request 3 <https://github.com/palantir/atlasdb/pull/2647>`__)

    *    - |improved| |devbreak|
         - ``ServerListConfig`` can now be created with zero servers, as part of work supporting Atlas clients starting up without knowing TimeLock nodes.
           This is strictly more permissive, but may affect developers that use ``ServerListConfig`` directly, especially if it is being serialized.
           (`Pull Request 3 <https://github.com/palantir/atlasdb/pull/2647>`__)

    *    - |improved| |logs|
         - kvs-slow-log was added on all Cassandra calls. As with the original kvs-slow-log logs, the added logs have the ``kvs-slow-log`` origin.
           To see the exact log messages, check the ``ProfilingCassandraClient`` class.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2673>`__)

    *    - |new| |metrics|
         - Metrics were added on all Cassandra calls. The ``CassandraClient`` interface was Tritium instrumented.
           The following metrics have been added, with the common prefix (package) ``com.palantir.atlasdb.keyvalue.cassandra.``:

              - ``CassandraClient.multiget_slice``
              - ``CassandraClient.get_range_slices``
              - ``CassandraClient.batch_mutate``
              - ``CassandraClient.get``
              - ``CassandraClient.cas``
              - ``CassandraClient.execute_cql3_query``

           Note that the table calls mainly use the first three metrics of the above list.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2673>`__)

    *    - |new| |metrics|
         - Metrics recording the number of Cassandra requests, and the amount of bytes read and written from and to Cassandra were added:
           The following metrics have been added, with the common prefix (package) ``com.palantir.atlasdb.keyvalue.cassandra.``:

              - ``QosMetrics.numReadRequests``
              - ``QosMetrics.numWriteRequests``
              - ``QosMetrics.bytesRead``
              - ``QosMetrics.bytesWritten``

           (`Pull Request <https://github.com/palantir/atlasdb/pull/2679>`__)

    *    - |new| |metrics|
         - Added metrics for cells read.
           The read cells can be post-filtered at the CassandraKVS layer, when there are multiple versions of the same cell.
           The filtered cells are recorded in the following metrics have been added, with the common prefix (package) ``com.palantir.atlasdb.keyvalue.cassandra.``:

              - ``TimestampExtractor.notLatestVisibleValueCellFilterCount``
              - ``ValueExtractor.notLatestVisibleValueCellFilterCount``

           The cells returned from the KVS layer are then recorded at the metric with the prefix (package) ``com.palantir.atlasdb.transaction.impl.``:

              - ``SnapshotTransaction.numCellsRead``

           Such cells can also be filtered out at the transaction layer, due to the Transaction Protocol. The filtered out cells are recorded in the metrics:

              - ``SnapshotTransaction.commitTsGreaterThatTxTsCellFilterCount``
              - ``SnapshotTransaction.invalidStartTsTsCellFilterCount``
              - ``SnapshotTransaction.invalidCommitTsCellFilterCount``
              - ``SnapshotTransaction.emptyValuesCellFilterCount``

           At last, the metric that record the number of cells actually returned to the AtlasDB client is:

              - ``SnapshotTransaction.numCellsReturnedAfterFiltering``

           (`Pull Request <https://github.com/palantir/atlasdb/pull/2671>`__)

    *    - |new| |metrics|
         - Added metrics for written bytes at the Transaction layer:

              - ``com.palantir.atlasdb.transaction.impl.SnapshotTransaction.bytesWritten``

           (`Pull Request <https://github.com/palantir/atlasdb/pull/2671>`__)

    *    - |new| |metrics|
         - A metric was added for the cases where a large read was made:

              - ``com.palantir.atlasdb.transaction.impl.SnapshotTransaction.tooManyBytesRead``

           Note that we also log a warning in these cases, with the message "A single get had quite a few bytes...".
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2671>`__)

    *    - |improved| |devbreak|
         - AtlasDB will now consistently throw a ``InsufficientConsistencyException`` if Cassandra reports an ``UnavailableException``.
           Also, all exceptions thrown at the KVS layer, as ``KeyAlreadyExists`` or ``TTransportException`` and ``NotInitializedException``
           were wrapped in ``AtlasDbDependencyException`` in the interest of consistent exceptions.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2558>`__)

    *    - |improved| |logs|
         - ``SweeperServiceImpl`` now logs when it starts sweeping and makes it clear if it is running full sweep or not
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2618>`__)

    *    - |fixed| |metrics|
         - ``MetricsManager`` now logs failures to register metrics at ``WARN`` instead of ``ERROR``, as failure to do so is not necessarily a systemic failure.
           Also, we now log the name of the metric as a Safe argument (previously it was logged as Unsafe).
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2636>`__)

    *    - |fixed|
         - ``SweepBatchConfig`` values are now decayed correctly when there's an error.
           ``SweepBatchConfig`` should be decreased until sweep succeeds, however the config actually oscillated between values, these were normally small but could be larger than the original config.  This was caused by us fixing one of the values at 1.
           ``SweepBatchConfig`` values will now be halved with each failure until they reach 1 (previously they only went to about 30% due to another bug).  This ensures we fully backoff and gives us the best possible chance of success.  Values will slowly increase with each successful run until they are back to their default level.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2630>`__)

    *    - |improved|
         - AtlasDB now depends on Tritium 0.8.4, which depends on the same version of ``com.palantir.remoting3`` and ``HdrHistogram`` as AtlasDB.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2662>`__)

    *    - |fixed|
         - Check that immutable timestamp is locked on write transactions with no writes.
           This could cause long-running readers to read an incorrect empty value when the table had the ``Sweep.THOROUGH`` strategy.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2406>`__)

    *    - |fixed|
         - Paxos value information is now correctly being logged when applicable leader events are happening.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2674>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.66.0
=======

7 November 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - AtlasDB now depends on Tritium 0.8.3, allowing products to upgrade Tritium without running into ``NoClassDefFound`` and ``NoSuchField`` errors.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2642>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

===========
v0.66.0-rc2
===========

6 November 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - AtlasDB now depends on Tritium 0.8.1.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2639>`__)

    *    - |improved|
         - AtlasDB can now tag RC releases.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2641>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

===========
v0.66.0-rc1
===========

This version was skipped due to issues on release. No artifacts with this version were ever published.

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.65.2
=======

6 November 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Reverted the Cassandra KVS executor PR (`Pull Request <https://github.com/palantir/atlasdb/pull/2534>`__) that caused a performance regression.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2637>`__)

    *    - |fixed|
         - ``CassandraTimestampBackupRunner`` now logs the backup bound correctly when performing a backup as part of TimeLock migration.
           Previously, the bound logged would have been logged as ``null`` or as a relatively arbitrary byte array, depending on the content of the timestamp table when performing migration.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2585>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.65.1
=======

4 November 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - AtlasDB now depends on Tritium 0.8.0, allowing products to upgrade Tritium without running into ``NoClassDefFound`` errors.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2628>`__)

    *    - |improved|
         - Sweep is now more efficient and less susceptible to OOMs on Cassandra.
           Also, the default value for the sweep batch config parameter ``candidateBatchSize`` has been bumped up from ``1`` from ``1024``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2546>`__)
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2610>`__)

    *    - |fixed|
         - Fixed cursor leak when sweeping on oracle/postgres.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2609>`__)

    *    - |improved|
         - Sweep progress is now persisted as a blob and uses a KVS level table.
           This allows us to use check and set to avoid versioning the entries in the sweep progress table.
           As a result, loading of the persisted SweepResult which was previously linear in the size of the table being swept can be done in constant time.
           No migration is necessary as the data is persisted to a new table ``_sweep_progress2``, however, sweep will ignore any previously persisted sweep progress.
           Note that this in particular means that any in-progress sweep will be abandoned and background sweep will choose a new table to sweep.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2569>`__)

    *    - |improved| |logs|
         - AtlasDB tables will now be logged as ``ns.tablename`` instead of ``map[namespace:map[name:ns] tablename:tablename]``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2606>`__)

    *    - |fixed|
         - TracingKVS now has spans with safe names.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2643>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.65.0
=======

This version was skipped due to issues on release. No artifacts with this version were ever published.

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.64.0
=======

1 November 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - UUIDs can now be used in schemas again.
           Previously, schemas generated with UUIDs would reference the ``java.util.UUID`` class without importing it.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2589>`__)

    *    - |improved|
         - The executor used by the Cassandra KVS is now allowed to grow larger so that we can better delegate blocking to the underlying Cassandra client pools.
           Please note that for Cassandra users this may result in increased Atlas thread counts when receiving spikes in requests.
           The underlying throttling is the same, however, so Cassandra load shouldn't be impacted.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2534>`__)

    *    - |improved| |metrics|
         - ``BackgroundSweeperImpl`` now records additional metrics on how each run of sweep went.
           Metrics report the number of occurrences of each outcome in the 1 minute prior to the metrics being gathered.
           We may change this duration in the future.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2531>`__)

    *    - |improved| |logs|
         - Log host names in Cassandra* classes.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2592>`__)

    *    - |fixed|
         - The executors used when async initializing objects are never shutdown anymore.
           You should be affected by this bug only if you had ``AtlasDbConfig.initializeAsync = true``.
           Previously, we would shut down the executors after a successful initialization, which could lead to a race condition with the submission of a ``cancelInitialization`` task.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2547>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.63.0
=======

27 October 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Fixed the deprecated ``TransactionManagers.create`` methods, by specifying a default user agent if none was provided.
           Previously, ``TransactionManager`` creation would have failed at runtime.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2582>`__)

    *    - |improved| |metrics|
         - Metrics are now recorded for put/get operations around commit timestamps.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2561>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.62.1
=======

27 October 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Updated our dependency on ``http-remoting`` to version ``3.5.1``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2574>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.62.0
=======

26 October 2017

Improvements

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - ``getRange`` is now more efficient when scanning over rows with many updates in Cassandra if just a single column is requested.
           Previously, a range request in Cassandra would always retrieve all columns and all historical versions of each column, regardless of which columns were requested.
           Now, we only request the latest version of the specific column requested, if only one column is requested. Requesting multiple columns still results in the previous behavior, however this will also be optimized in a future release.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2480>`__)

    *    - |deprecated| |improved|
         - ``SerializableTransactionManager`` is now created via an immutable builder instead of a long list of individual arguments. Use ``TransactionManagers.builder()``
           to get the builder and once completely configured, build the transaction manager via the builder's ``.buildSerializable()`` method.
           The existing ``create`` methods are deprecated and will be removed by November 15th, 2017.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2459>`__)

    *    - |devbreak| |improved|
         - ``TransactionManagers.builder()`` no longer has a ``callingClass(..)`` method and now requires the consumer to directly specify their user agent via the previously optional method ``userAgent(..)``.
           All of the ``TransactionManagers.create(..)`` methods are still deprecated, and can be used to specify an empty user-agent.
           We use the user-agent on logs and metrics, so specifying it helps us to diagnose issues in the future.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2542>`__)

    *    - |improved|
         - The duration between attempts of whitelist Cassandra nodes was reduced from 5 minutes to 2 minutes, and the minimum period a node is blacklisted for was reduced from 2 minutes to 30 seconds.
           This means we check the health of a blacklisted Cassandra node and whitelist it faster than before.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2543>`__)

    *    - |devbreak| |improved|
         - The size of the transaction cache is now configurable. It is not anticipated end users will need to touch this;
           it is more likely that this will be configured via per-service overrides for the services for whom the
           current cache size is inadequate. If needed, configuring this parameter is available under the AtlasDbRuntimeConfig with the name `timestampCacheSize`.

           This is a small API change for users manually constructing a TransactionManager, which now requires a transaction cache size parameter.
           Please add it from the AtlasDbRuntimeConfig, or instead of manually creating a TransactionManager,
           utilize the builder in TransactionManagers to have this done for you.

           Note that even if the config is changed at runtime, the size of the cache doesn't change dynamically until `2565 <https://github.com/palantir/atlasdb/issues/2565>`__ is resolved.
           (`Pull Request 1 <https://github.com/palantir/atlasdb/pull/2496>`__)
           (`Pull Request 2 <https://github.com/palantir/atlasdb/pull/2554>`__)

    *    - |improved|
         - Exposes another version of ``getRanges`` that uses a configurable concurrency level when not explicitly
           provided a value. This defaults to 8 and can be configured with the ``KeyValueServiceConfig#defaultGetRangesConcurrency`` parameter.
           Check the full configuration docs `here <https://palantir.github.io/atlasdb/html/configuration/key_value_service_configs/index.html>`__.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2484>`__)

    *    - |devbreak| |improved|
         - Simplify and annotate the constructors for ``SerializableTransactionManager``. This should make the code of that class more maintainable.
           If you used one of the deleted or deprecated constructors, use the static ``create`` method.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2549>`__)

Logs and Metrics

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved| |metrics|
         - ``SweepMetrics`` are now updated at the end of every batch rather than cumulative metrics at the end of every table.
           This will provide more accurate metrics for when sweep is doing something.
           Sweeping through the sweep endpoint will now also contribute to these metrics — before it didn't update any metrics which again distorted the view of what work sweep was doing on the DB.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2535>`__)

    *    - |new| |metrics|
         - AtlasDB clients now emit metrics that track the immutable timestamp, unreadable timestamp, and current timestamp.
           These metrics should help in performing diagnosis of issues concerning Sweep and/or the lock service.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2467>`__)

    *    - |fixed| |metrics|
         - Timelock server no longer appends client names to metrics. Instead, each metric is aggregated across all clients.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2501>`__)

    *    - |new| |metrics|
         - We now report metrics for Transaction conflicts.
           The metrics are a meter reported under the name ``SerializableTransaction.SerializableTransactionConflict``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2540>`__)

    *    - |improved| |logs|
         - Specified which logs from Cassandra* classes were Safe or Unsafe for collection, improving the data that we can collect for debugging purposes.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2537>`__)

    *    - |improved| |userbreak| |logs|
         - The ``ProfilingKeyValueService`` now reports its multipart log lines as a single line.
           This should improve log readability in log ingestion tools when AtlasDB is run in multithreaded environments.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2474>`__)

    *    - |fixed| |logs|
         - TimeLock Server's ``ClockSkewMonitor`` now attempts to contact all other nodes in the TimeLock cluster, even in the presence of remoting exceptions or clock skews.
           Previously, we would stop querying nodes once we encountered a remoting exception or detected clock skew.
           Also, the log line ``ClockSkewMonitor threw an exception`` which was previously logged every second when a TimeLock node was down or otherwise uncontactable is now restricted to once every 10 minutes.
           Note that the ``clock.monitor-exception`` metric is still incremented on every call, even if we do not log.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2456>`__)

    *    - |fixed| |logs|
         - ``ProfilingKeyValueService`` now logs correctly when logging a message for ``getRange``, ``getRangeOfTimestamps`` and ``deleteRange``.
           Previously, the table reference was omitted, such that one might receive lines of the form ``Call to KVS.getRange on table RangeRequest{reverse=false} with range 1504 took {} ms.``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2474>`__)

    *    - |fixed| |metrics|
         - ``MetricsManager`` now supports de-registration of metrics for a given prefix.
           Previously, this would crash with a ``ConcurrentModificationException`` if metrics were actually being removed.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2467>`__)

Bug fixes

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *   - |fixed|
        - ``ProfilingKeyValueService`` now logs correctly when logging a message for ``getRange``, ``getRangeOfTimestamps`` and ``DeleteRange``.
          Previously, the table reference was omitted, such that one might receive lines of the form ``Call to KVS.getRange on table RangeRequest{reverse=false} with range 1504 took {} ms.``.
          (`Pull Request <https://github.com/palantir/atlasdb/pull/2474>`__)

    *    - |fixed|
         - When AtlasDB thinks all Cassandra nodes are non-healthy, it logs a message containing "There are no known live hosts in the connection pool ... We're choosing one at random ...".
           The level of this log was reduced from ERROR to WARN, as it was spammy in periods of a Cassandra outage.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2543>`__)

    *    - |fixed|
         - Timelock server will try to gain leadership synchronously when the first time a new client namespace is requested. Previously, the first request would always return 503.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2503>`__)

    *    - |fixed|
         - ``SerializableErrorDecoder`` will decode errors properly instead of throwing ``NullPointerException``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2446>`__)

    *    - |fixed|
         - Async Initialization now works with TimeLock Server.
           Previously, for Cassandra we would attempt to immediately migrate the timestamp bound from Cassandra to TimeLock on startup, which would fail if either of them was unavailable.
           For DBKVS or other key-value services, we would attempt to ping TimeLock on startup, which would fail if TimeLock was unavailable (though the KVS need not be available).
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2520>`__)

    *    - |fixed|
         - ``AsyncInitializer`` now shuts down its executor after initialization has completed.
           Previously, the executor service wasn't shut down, which could lead to the initializer thread hanging around unnecessarily.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2520>`__)

    *    - |fixed|
         - Fixed an issue where a ``waitForLocks`` request could retry unnecessarily.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2491>`__)

    *    - |fixed|
         - ``InMemoryAtlasDbConfig`` now has an empty namespace, instead of "test".
           This means that internal products will no longer have to set their root-level namespace to "test" in order to use ``InMemoryKeyValueService`` for testing.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2541>`__)

    *    - |devbreak| |fixed|
         - Move ``@CancelableServerCall`` to a more fitting package that matches internal codebase.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2562>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.61.1
=======

19 October 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - Reverted the Sweep rewrite for Cassandra as it would unnecessarily load values into memory which could
           cause Cassandra to OOM if the values are large enough.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2521>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.61.0
=======

18 October 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - Sweep is now more efficient on Postgres and Oracle.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2436>`__)

    *    - |improved|
         - The ``SweeperService`` endpoint registered on all clients will now sweeps the full table by default, rather than a single batch.
           It also now returns information about how much data was swept.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2409>`__)

    *    - |fixed| |logs|
         - Sweep candidate batches are now logged correctly.
           Previously, we would log a ``SafeArg`` for these batches that had no content.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2475>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.60.1
=======

16 October 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |new| |improved|
         - AtlasDB now supports asynchronous initialization, where ``TransactionManagers.create()`` creates a ``SerializableTransactionManager`` even when initialization fails, for instance because the KVS is not up yet.

           To enable asynchronous initialization, a new config option ``initializeAsync`` was added to AtlasDbConfig.
           If this option is set to true, ``TransactionManagers.create()`` first attempts to create a ``SerializableTransactionManager`` synchronously, i.e., consistent with current behaviour.
           If this fails, it returns a ``SerializableTransactionManager`` for which the necessary initialization is scheduled in the background and which throws a ``NotInitializedException`` on any method call until the initialization completes - this is, until the backing store becomes available.

           While waiting for AtlasDB to be ready, clients can poll ``isInitialized()`` on the returned ``SerializableTransactionManager``.

           The default value for the config  is ``false`` in order to preserve previous behaviour.
           (`Pull Request 1 <https://github.com/palantir/atlasdb/pull/2390>`__ and
           `Pull Request 2 <https://github.com/palantir/atlasdb/pull/2476>`__)

    *    - |new|
         - Timelock server can now be configured to persist the timestamp bound in the database, specifically in Cassandra/Postgres/Oracle.
           We recommend this to be configured only for cases where you absolutely need to persist all state in the database, for example,
           in special cases where backups are simply database dumps and do not have any mechanism for storing timestamps.
           This will help support large internal product's usage of the Timelock server.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2364>`__)

    *    - |devbreak| |improved|
         - In order to limit the access to inner methods, and to make the implementation of asynchronous initialization feasible, we've extracted interfaces and renamed the following classes:

              - ``CassandraClientPool``
              - ``CassandraKeyValueService``
              - ``LockStore``
              - ``PersistentTimestampService``

           Now the factory methods for the above classes return the interfaces. The actual implementation of such classes was moved to their corresponding \*Impl files.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2390>`__)

    *    - |devbreak| |improved|
         - ``LockRefreshingTimelockService`` has been moved to the ``lock-api`` project under the package name ``com.palantir.lock.client``, and now implements
           ``AutoCloseable``, shutting down its internal executor service.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2451>`__)

    *    - |fixed|
         - ``PersistentLockManager`` can now reacquire the persistent lock if another process unilaterally clears the lock.
           Previously in this case, sweep would continually fail to acquire the lock until the service restarts.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2419>`__)

    *    - |fixed|
         - ``CassandraClientPool`` no longer logs stack traces twice for every failed attempt to connect to Cassandra.
           Instead, the exception is logged once only, when we run out of retries.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2432>`__)

    *    - |fixed|
         - The Sweep endpoint and CLI now accept start rows regardless of the case these are presented in.
           Previously, giving a start row with hex characters in lower case e.g. ``deadbeef`` would result in an ``IllegalArgumentException`` being thrown.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2468>`__)

    *    - |devbreak|
         - Removed the following unnecessary classes related to wrapping KVSes:

           - ``NamespacedKeyValueService``
           - ``NamespaceMappingKeyValueService``
           - ``NamespacedKeyValueServices``
           - ``StaticTableMappingService``

           (`Pull Request <https://github.com/palantir/atlasdb/pull/2448>`__)

    *    - |fixed|
         - Lock state logging will dump ``expiresIn`` of refreshed token, instead of original, which was negative after refreshing.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2469>`__)

    *    - |fixed|
         - When using the TimeLock block and either the timestamp or the lock service threw an exception, we were throwing InvocationTargetException instead.
           We now throw the actual cause for the invocation exception.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2460>`__)


.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.60.0
=======

This version was skipped due to issues on release. No artifacts with this version were ever published.


.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.59.1
=======

04 October 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - Allow passing a `ProxyConfiguration <https://github.com/palantir/conjure-java-runtime-api/blob/2.3.0/service-config/src/main/java/com/palantir/conjure/java/api/config/service/ProxyConfiguration.java>`__ to allow setting custom proxy on the TimeLock clients.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2393>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.59.0
=======

04 October 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - Timestamp batching has now been enabled by default.
           Please see :ref:`Timestamp Client Options <timestamp-client-config>` for details.
           This should improve throughput and latency, especially if load is heavy and/or clients are communicating with a TimeLock cluster which is used by many services.
           Note that there may be an increase in latency under light load (e.g. 2-4 threads).
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2392>`__)

    *    - |new|
         - AtlasDB now offers a simplified version of the schema API by setting the ``enableV2Table()`` flag in your TableDefinition.
           This would generate an additional table class with some easy to use functions such as ``putColumn(key, value)``, ``getColumn(key)``, ``deleteColumn(key)``.
           We only provide these methods for named columns, and don't currently support dynamic columns.
           You can add this to your current Schema, and use the new simplified APIs by using the V2 generated table.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2401>`__)

    *    - |new|
         - AtlasDB now offers specifying ``hashFirstNRowComponents(n)`` in Table and Index definitions.
           This prevents hotspotting by prepending the hashed concatenation of the row components to the row key.
           When using with prefix range requests, the hashed components must also be specified in the prefix.
           Adding this to an existing Schema is not supported, as that would require a data migration.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2384>`__)

    *    - |new|
         - AtlasDB now offers specifying ``hashRowComponents()`` in StreamStore definitions.
           This prevents hotspotting in Cassandra by prepending the hashed concatenation of the ``streamId`` and ``blockId`` to the row key.
           Adding this to an existing StreamStore is not supported, as that would require a data migration.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2384>`__)

    *    - |fixed|
         - The ``lock/log-current-state`` endpoint now correctly logs the number of outstanding lock requests.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2396>`__)

    *    - |fixed|
         - Fixed migration from JDBC KeyValueService by adding a missing dependency to the CLI distribution.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2397>`__)

    *    - |fixed|
         - Oracle auto-shrink is now disabled by default.
           This is an experimental feature allowing Oracle non-EE users to compact automatically.
           We decided to turn it off by default since we have observed timeouts for large amounts of data, until we find a better retry mechanism for shrink failures.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2405>`__)

    *    - |logs| |userbreak|
         - AtlasDB no longer tries to register Cassandra metrics for each pool with the same names.
           We now add `poolN` to the metric name in CassandraClientPoolingContainer, where N is the pool number.
           This will prevent spurious stacktraces in logs due to failure in registering metrics with the same name.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2415>`__)

    *    - |devbreak| |fixed|
         - Adjusted the remoting-api library version to match the version used by remoting3.
           Developers may need to check your dependencies, but no other actions should be required.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2399>`__)

    *    - |fixed|
         - Adjusted optimizer hints for getRange() to prevent Oracle from picking a bad query plan.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2421>`__)


.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.58.0
=======

22 September 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |logs|
         - AtlasDB now logs slow queries CQL queries (via ``kvs-slow-log``) used for sweep
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2363>`__)

    *    - |devbreak| |fixed|
         - AtlasDB now depends on okhttp 3.8.1. This is expected to fix an issue where connections would constantly throw "shutdown" exceptions, which was likely due to a documented bug in okhttp 3.4.1.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2343>`__)

    *    - |devbreak| |improved|
         - Upgraded all uses of `http-remoting <https://github.com/palantir/http-remoting>`__ from remoting2 to remoting3, except for serialization of errors (preserved for backwards wire compatibility).
           Developers may need to check their dependencies, as well as update instantiation of their calls to ``TransactionManagers.create()`` to use the remoting3 API.
           Note that *users* of AtlasDB clients are not affected, in that the wire format of configuration files has not changed.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2348>`__)

    *    - |fixed|
         - KVS migration no longer fails when the old ``_scrub`` table is present.
           This unblocks KVS migrations for users who have data in ``_scrub`` but have not migrated from ``_scrub`` to ``_scrub2`` yet.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2362>`__)

    *    - |fixed|
         - Path and query parameters for TimeLock endpoints have now been marked as safe.
           Several logging parameters in TimeLock (e.g. in ``PaxosTimestampBoundStore`` and ``PaxosSynchronizer``) have also been marked as safe.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2375>`__)

    *    - |improved|
         - The ``LockServiceImpl`` now, in addition to lock tokens and grants (which are unsafe for logging), also logs token and grant IDs (which are big-integer IDs) as safe.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2375>`__)

    *    - |fixed|
         - Sweep log priority has been increased to INFO for logs of when a table 1. is starting to be swept, 2. will be swept with another batch, and 3. has just been completely swept.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2378>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.57.0
=======

19 September 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |metrics| |changed|
         - From this version onwards, AtlasDB's metrics no longer have unbounded multiplicity.
           This means that AtlasDB can be whitelisted in the internal metrics aggregator tool.

    *    - |metrics| |userbreak|
         - AtlasDB no longer embeds Cassandra host names in its metrics.
           Aggregate metrics are retained in both CassandraClientPool and CassandraClientPoolingContainer.
           This was necessary for compatibility with an internal log-ingestion tool.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2324>`__)

    *    - |metrics| |userbreak|
         - AtlasDB no longer embeds table names in Sweep metrics.
           Sweep aggregate metrics continue to be reported.
           This was necessary for compatibility with an internal log-ingestion tool.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2324>`__)

    *    - |devbreak| |fixed|
         - AtlasDB now depends on okhttp 3.8.1. This is expected to fix an issue where connections would constantly throw "shutdown" exceptions, which was likely due to a documented bug in okhttp 3.4.1.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2343>`__)

    *    - |devbreak| |fixed|
         - The ``ConcurrentStreams`` class has been deleted and replaced with calls to ``MoreStreams.blockingStreamWithParallelism``, from `streams <https://github.com/palantir/streams>`__.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2361/files>`__)

    *    - |devbreak| |improved|
         - TimeLockAgent's constructor now accepts a Supplier instead of an RxJava Observable.
           This reduces the size of the TimeLock Agent jar, and removes the need for a dependency on RxJava.
           To convert an RxJava Observable to a Supplier that always returns the most recent value, consider the method `blockingMostRecent` as implemented `here <https://github.com/palantir/atlasdb/blob/0.56.0/timelock-agent/src/main/java/com/palantir/timelock/Observables.java#L39-L48>`__.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2339>`__)

    *    - |improved|
         - ``BatchingVisitableView`` methods ``immutableCopy``, ``immutableSetCopy``, and ``copyInto`` use the default batch hint of 1000, instead of a batch hint of 100,000.
           We previously defaulted to the higher value because the result set was assumed to be small; however, in practice this has turned out not to be the case, leading to timeouts and OOMs in the field.
           To use a custom batch hint, set the ``batchHint`` property for your ``RangeRequest``.
           Alternatively, call ``BatchingVisitableView.hintBatchSize(int)`` before making a copy.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2347>`__)

    *    - |improved|
         - AtlasDB table definitions now support specifying log safety without having to also specify value byte order for row components.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2349>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.56.1
=======

14 September 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - The new concurrent version of `Transaction#getRanges` did not correctly guarantee ordering of the results returned in its stream.
           We now make sure the resulting ordering matches that of the input RangeRequests.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2337>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.56.0
=======

12 September 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |new|
         - TimelockServer now exposes the `LockService` instead of the `RemoteLockService` if using the synchronous lock service.
           This will provide a more comprehensive API which is required by the large internal products.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2284>`__)

    *    - |userbreak| |new|
         - Timelock clients now report tritium metrics for the lock requests with the prefix ``LockService`` instead of ``RemoteLockService``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2284>`__)

    *    - |devbreak|
         - LockAwareTransactionManager now returns a `LockService` instead of a `RemoteLockService` in order to expose the new API.
           Any products that extend this class will have to change their class definition.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2284>`__)

    *    - |new|
         - Added two new methods to Transaction, getRangesLazy and a concurrent version of getRanges, which are also exposed in the Table API.
           If you expect to only use a small amount of the rows in the provided ranges, it is often advisable to use the new getRangesLazy method and serially iterate over the results.
           Otherwise, you should use the new version of getRanges that allows explicitly operating on the resulting visitables in parallel.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2281>`__)

    *    - |deprecated|
         - The existing getRanges method has been deprecated as it would eagerly load the first page of all ranges, potentially concurrently.
           This often caused more data to be fetched than necessary or higher concurrency than expected.
           Recommended alternative is to use getRanges with a specified concurrency level, or getRangesLazy.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2281>`__)

    *    - |userbreak| |fixed|
         - AtlasDB no longer embeds user-agents in metric names.
           This affects both AtlasDB clients as well as TimeLock Server.
           All metrics are still available; however, metrics which previously included a user-agent component will no longer do so.
           For example, the timer ``com.palantir.timestamp.TimestampService.myUserAgent_version.getFreshTimestamp`` is now named ``com.palantir.timestamp.TimestampService.getFreshTimestamp``.
           This was necessary for compatibility with an internal log-ingestion tool.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2322>`__)

    *    - |improved|
         - LockServerOptions now provides a builder, which means constructing one should not require overriding methods.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2284>`__)

    *    - |new|
         - Oracle will now validate connections by running the test query when getting a new connection from the HikariPool.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2301>`__)

    *    - |improved|
         - Cassandra range concurrency defaults lowered from 64x to 32x, to reflect default connection pool sizes
           that have shrank over time, and to be more appropriate for fairly common smaller 3-node clusters.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2386>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.55.0
=======

01 September 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |userbreak|
         - If AtlasDB is used with TimeLock, and the TimeLock client name is different than either the Cassandra ``keyspace``, Postgres ``dbName``, or Oracle ``sid``, *AtlasDB will fail to start*.
           This was done to avoid the risk of data corruption if these are accidentally changed independently.
           If the above parameters contradict, please contact the AtlasDB team to change the TimeLock client name. Changing it in config without additional action may result in *severe data corruption*.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2263>`__)

    *    - |new|
         - AtlasDB introduces a top-level ``namespace`` configuration parameter, which is used to set the ``keyspace`` in the keyValueService when using Cassandra and the ``client`` in TimeLock.
           Following the previous change, we unify both the configs that cannot be changed separately in one single config. Therefore it is suggested that AtlasDB users follow and use the new parameter to specify both the deprecated ones.
           Note that if the new ``namespace`` config contradicts with either the Cassandra ``keyspace`` and/or the TimeLock ``client`` configs, *AtlasDB will fail to start*.
           Please consult the documentation for :ref:`AtlasDB Configuration <atlas-config>` for details on how to set this up.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2263>`__)

    *    - |deprecated|
         - As a followup of the ``namespace`` change, the Cassandra ``keyspace`` and TimeLock ``client`` configs were deprecated.
           As said previously, please use the ``namespace`` root level config to specify both of these parameters.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2263>`__)

    *    - |new|
         - Oracle SE will now automatically trigger table data shrinking to recover space after sweeping a table.
           You can disable the compaction by setting ``enableShrinkOnOracleStandardEdition`` to ``false`` in the Oracle DDL config.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2286>`__)

    *    - |fixed|
         - Fixed an issue where sweep logs would get rolled over sooner than expected. The number of log files stored on disk was increased from 10 to 90 before rolling over.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2295>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.54.0
=======

25 August 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |new|
         - Timelock clients now report tritium metrics for the ``TimestampService`` even if they are using the request batching service.
           Note when setting up metric graphs, the timestamp service metrics are named with ``...Timelock.<getFreshTimestamp/getFreshTimestamps>`` when not using request batching,
           but as ``...Timestamp.<getFreshTimestamp/getFreshTimestamps>`` if using request batching.
           The lock service metrics are always reported as ``...Timelock.<lock/unlock/etc>`` for timelock clients.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2270>`__)

    *    - |fixed|
         - AtlasDB clients now report tritium metrics for the ``TimestampService`` and ``LockService`` endpoints just once instead of twice.
           In the past, every request would be reported twice leading to number bloat and more load on the metric collector service.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2270>`__)

    *    - |fixed|
         - ``kvs-slow-log`` now uses ``logsafe`` to support sls-compatible logging.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2222>`__)

    *    - |fixed|
         - The scrubber queue no longer grows without bound if the same cell is overwritten multiple times by hard delete transactions.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2232>`__)

    *    - |improved|
         - If ``enableOracleEnterpriseFeatures`` is configured to be false, you will now see warnings asking you to run Oracle compaction manually.
           This will help make non-EE Oracle users aware of potential database bloat.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2277>`__)

    *    - |fixed|
         - Fixed a case where logging an expection suppressing itself would cause a stack overflow.
           See `LOGBACK-1027 <https://jira.qos.ch/browse/LOGBACK-1027>`__.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2242>`__)

    *    - |new|
         - AtlasDB now produces a new artifact, ``timelock-agent``.
           Users who wish to run TimeLock Server outside of a Dropwizard environment should now be able to do so more easily, by supplying the TimeLock Agent with a *registrar* that knows how to register Java resources and expose suitable HTTP endpoints.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2247>`__)

    *    - |improved|
         - TimeLock now creates client namespaces the first time they are requested, rather than requiring them to be specified in config.
           This means that specifying a list of clients in Timelock configuration will no longer have any effect. Further, a new configuration property called ``max-number-of-clients`` has been introduced in ``TimeLockRuntimeConfiguration``. This can be used to limit the number of clients that will be created dynamically, since each distinct client has some memory, disk space, and CPU overhead.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2252>`__)

    *    - |deprecated|
         - ``putUnlessExists`` methods in schema generated code have been marked as deprecated as the naming can be misleading, leading to accidental value overwrites. The recommended alternative is doing a separate read and write in a single transaction.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2271>`__)

    *    - |fixed|
         - CharacterLimitType now has fields marked as final.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2259>`__)

    *    - |changed|
         - The ``RangeMigrator`` interface now contains an additional method ``logStatus(int numRangeBoundaries)``.
           This method is used to log the state of migration for each table when starting or resuming a KVS migration.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2241>`__)

    *    - |changed|
         - Updated our dependency on ``sls-packaging`` from 2.3.1 to 2.4.0.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2268>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.53.0
=======

9 August 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - KVS migrations will no longer verify equality between the from and to KVSes for the sweep priority and progress tables.
           Note that these tables are still *migrated* across, as they provide heuristics for timely sweeping of tables.
           However, these tables may change during the migration, without affecting correctness (e.g. the from-kvs could be swept).
           Previously, we would attempt to check that the sweep tables were equal on both KVSes, leading to spurious validation failures.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2244>`__)

    *    - |new|
         - AtlasDB now supports specifying the safety of table names as well as row and column component names following the `palantir/safe-logging <https://github.com/palantir/safe-logging>`__ library.
           Please consult the documentation for :ref:`Tables and Indices <tables-and-indices>` for details on how to set this up.
           As AtlasDB regenerates its metadata on startup, changes will take effect after restarting your AtlasDB client (in particular, you do NOT need to rerender your schemas.)
           Previously, all table names, row component names and column names were always treated as unsafe.
           (`Pull Request 1 <https://github.com/palantir/atlasdb/pull/1988>`__,
           `Pull Request 2 <https://github.com/palantir/atlasdb/pull/2000>`__ and
           `Pull Request 3 <https://github.com/palantir/atlasdb/pull/2172>`__)

    *    - |improved|
         - The ``ProfilingKeyValueService`` and ``SpecificTableSweeper`` now log table names as safe arguments, if and only if these have been specified as safe in one's schemas.
           Previously, these were always logged as unsafe.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2172>`__)

    *    - |devbreak|
         - AtlasDB now throws an error during schema code generation stage if table length exceeds KVS limits.
           To override this, please specify ``ignoreTableNameLengthChecks()`` on your schema.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2220>`__)

    *    - |devbreak|
         - ``NameComponentDescription`` is now a ``final`` class and has a builder instead of constructors.
           This will affect any products which have subclassed ``NameComponentDescription``, although we are not aware of any.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2238>`__)

    *    - |devbreak|
         - IteratorUtils.forEach removed; it's not needed in a Java 8 codebase.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2207>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.52.0
=======

1 August 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - Fixed a critical bug in Oracle that limits the number of writes with values greater than 2000 bytes to ``Integer.MAX_VALUE``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2224>`__)

    *    - |fixed|
         - Change schemas in the codebase so that they use JAVA8 Optionals instead of Guava.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2210>`__)

    *    - |devbreak|
         - Removed unused classes on AtlasDB.

              - ``FutureClosableIteratorTask``
              - ``ClosableMergedIterator``
              - ``ThrowingKeyValueService``

           If any issues arise from this change, please contact the development team.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1933>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.51.0
=======

28 July 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - For DbKvs, the ``actualValues`` field is now populated when a ``CheckAndSetException`` is thrown.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2196>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.50.0
=======

27 July 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed| |userbreak|
         - TimeLock Server, if configured to use the async lock service, will now throw if a client attempts to start a transaction via the sync lock service.

           Previously, users which have clients (for the same namespace) running both pre- and post-0.49.0 versions of AtlasDB were able to run transactions against the sync and async lock services concurrently, thus breaking the guarantees of the lock service.
           AtlasDB does not support having clients (for the same namespace) running both pre- and post-0.49.0 versions.

           Note that TimeLock users which have clients (for different namespaces) running both pre- and post-0.49.0 versions will need to turn this feature off for clients on pre-0.49.0 versions to continue working with TimeLock, and should exercise caution in ensuring that, for each namespace, clients use only pre- or post-0.49.0 versions of AtlasDB.
           Please see :ref:`Async Lock Service Configuration <async-lock-service>` for documentation.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2189>`__)

    *    - |userbreak|
         - TimeLock Server has moved its parameter ``useAsyncLockService`` to be within an ``asyncLock`` block.
           This was done as we wanted to keep the configuration options for the async lock service together.
           The parameter remains optional, and users not configuring this parameter are unaffected.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2189>`__)

    *    - |improved|
         - ``gc_grace_seconds`` will now be automatically updated for services running against CassandraKVS on startup.

           We reduced ``gc_grace_seconds`` from four days to one hour in 0.42.0 but that is enforced for new tables and not the existing ones.
           Updating ``gc_grace_seconds`` can be an expensive operation and users should expect the service to block for a while on startup.
           However, this shouldn't be a concern unless the count of tables is in the order of 100s. If you think this will be an issue,
           please configure the ``gcGraceSeconds`` parameter in Cassandra KVS config to 4 days (``4 * 24 * 60 * 60``), which was the previous default.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2129>`__)

    *    - |fixed|
         - ``RequestBatchingTimestampService`` now works for AtlasDB clients using TimeLock Server once again.
           Previously in 0.49.0, clients using TimeLock Server and request batching would still request timestamps one at a time from the TimeLock Server.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2182>`__)

    *    - |fixed|
         - ``PaxosQuorumChecker`` will now interrupt outstanding requests after a quorum response has been collected. This prevents the number of paxos request threads from growing without bound.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2193>`__)

    *    - |improved| |devbreak|
         - OkHttp clients (created with ``FeignOkHttpClients``) will no longer silently retry connections.
           We have already implemented retries, including retries from connection failures, at the Feign level in ``FailoverFeignTarget``.
           If you require silent retry, please contact the AtlasDB team.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2197>`__)

    *    - |improved| |userbreak|
         - AtlasConsole database mutation commands (namely ``put()`` and ``delete()``) are now disabled by default.
           To enable them, run AtlasConsole with the ``--mutations_enabled`` flag
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2155>`__)

    *    - |fixed|
         - Fixed a bug in AtlasConsole that caused valid table names not to be recognized.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2192>`__)

    *    - |new|
         - TimeLock Server now supports a ``NonBlockingFileAppenderFactory`` which prevents requests from blocking if the request log queue is full.
           To use this appender, the ``type`` property should be set to ``non-blocking-file`` in the logging appender configuration. Note that using this appender may result in request logs being dropped.
           (:ref:`Docs <non-blocking-appender>`)
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2198>`__)

    *    - |fixed|
         - Fixed a potential deadlock in ``PersistentLockManager`` that could prevent clients from shutting down if the persistent backup lock could not be acquired.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2208>`__)

    *    - |new|
         - New metrics have been added for tracking Cassandra's approximate pool size, number of idle connections, and number of active connections. (:ref:`Docs <dropwizard-metrics>`)
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2214>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.49.0
=======

18 July 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - TimeLock Server now can process lock requests using async Jetty servlets, rather than blocking request threads. This leads to more stability and higher throughput during periods of heavy lock contention.
           To enable this behavior, use the ``useAsyncLockService`` option to switch between the new and old lock service implementation. This option defaults to ``true``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2084>`__)

    *    - |devbreak| |improved|
         - The maximum time that a transaction will block while waiting for commit locks is now configurable, and defaults to 1 minute. This can be configured via the ``transaction.lockAcquireTimeoutMillis`` option in ``AtlasDbRuntimeConfig``.
           This differs from the previous behavior, which was to block indefinitely. However, the previous behavior can be effectively restored by configuring a large timeout.
           If creating a ``SerializableTransactionManager`` directly, use the new constructor which accepts a timeout parameter.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2158>`__)

    *    - |devbreak|
         - ``randomBitCount`` and ``maxAllowedBlockingDuration`` are deprecated and no longer configurable in ``LockServerOptions``. If specified, they will be silently ignored.
           If your service relies on either of these configuration options, please contact the AtlasDB team.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2161>`__)

    *    - |userbreak|
         - This version of the AtlasDB client will **require** a version of Timelock server that exposes the new ``/timelock`` endpoints.
           Note that this only applies if running against Timelock server; clients running with embedded leader mode are not affected.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2135>`__)

    *    - |userbreak|
         - The timestamp batching functionality introduced in 0.48.0 is temporarily no longer supported when running with Timelock server. We will re-enable support for this in a future release.

    *    - |fixed|
         - Fixed the broken ``put()`` command in AtlasConsole. You should now be able to insert and update data using Console.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2140>`__)

    *    - |fixed|
         - Fixed an issue that could cause AtlasConsole to print unnecessary amounts of input when commands were run.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2130>`__)

    *    - |userbreak|
         - Remove Cassandra config option ``safetyDisabled``;
           users should instead move to a more specific config for their situation, which are:
           ``ignoreNodeTopologyChecks``, ``ignoreInconsistentRingChecks``, ``ignoreDatacenterConfigurationChecks``, ``ignorePartitionerChecks``
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2024>`__)

    *    - |fixed|
         - ``commons-executors`` now excludes the ``safe-logging`` Java8 jar to support Java 6 clients.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2160>`__)

    *    - |new|
         - ``TransactionManagers`` exposes a method in which it is possible to specify the user agent to be used.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2162>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.48.0
=======

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - If sweep configs are specified in the AtlasDbConfig block, they will be ignored, but AtlasDB will no longer fail to start.
           This effectively fixes the Sweep-related user break change of version ``0.47.0``.
           Note that users of products that upgraded from ``0.45.0`` to ``0.48.0`` will need to move configuration overrides from the regular ``atlasdb`` config to the ``atlasdb-runtime`` config for them to continue taking effect.
           Please reference the Sweep :ref:`configuration docs <sweep_tunable_parameters>` for more details.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2134>`__)

    *    - |new|
         - AtlasDB now supports batching of timestamp requests on the client-side; see :ref:`Timestamp Client Options <timestamp-client-config>` for details.
           On internal benchmarks, the AtlasDB team has obtained an almost 2x improvement in timestamp throughput and latency under modest load (32 threads), and an over 10x improvement under heavy load (8,192 threads).
           There may be a very small increase in latency under extremely light load (e.g. 2-4 threads).
           Note that this is not enabled by default.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2083>`__)

    *    - |devbreak|
         - The ``RateLimitingTimestampService`` in ``timestamp-impl`` has been renamed to ``RequestBatchingTimestampService``, to better reflect what the service does and avoid confusion with the ``ThreadPooledLockService`` (which performs resource-limiting).
           Products that do not use this class directly are not affected.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2083>`__)

    *    - |fixed| |devbreak|
         - ``TransactionManager.close()`` now closes the lock service (provided it is closeable), and also shuts down the Background Sweeper.
           Previously, the lock service's background threads as well as background sweeper would continue to run (potentially indefinitely) even after a transaction manager was closed.
           Note that services that relied on the lock service being available even after a transaction manager was shut down may no longer behave properly, and should ensure that the transaction manager is not shut down while the lock service is still needed.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2102>`__)

    *    - |fixed|
         - ``commons-executors`` now uses Java 6 when compiling from source and generates classes targeting Java 6.
           Java 6 support was removed in AtlasDB ``0.41.0`` and blocks certain internal products from upgrading to subsequent versions.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2122>`__)

    *    - |fixed|
         - ``LockServiceImpl.close()`` is now idempotent.
           Previously, calling the referred method more than once could fail an assertion and throw an exception.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2144>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.47.0
=======

11 July 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - ErrorProne is enabled and not ignored on all AtlasDB projects. This means that AtlasDB can be whitelisted in the internal logging aggregator tool from this version ownards.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1889>`__)
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1901>`__)
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1902>`__)
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2055>`__)

    *    - |new| |improved|
         - Background Sweep is enabled by default on AtlasDB. To understand what Background Sweep is, please check the :ref:`sweep docs<sweep>`, in particular, the :ref:`background sweep docs<background-sweep>`.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2104>`__)

    *    - |userbreak| |devbreak| |improved|
         - Added support for live-reloading sweep configurations.
           ``TransactionManagers.create()`` methods now accept a Supplier of ``AtlasDbRuntimeConfig`` in addition to an ``AtlasDbConfig``.
           If needed, the helper method ``defaultRuntimeConfig()`` can be used to create a runtime config with the default values.
           As part of this improvement, we made the sweep options of ``AtlasDbConfig`` unavailable.
           The following options now **may not** be specified in the install config and must instead be specified in the runtime config:

            +-----------------------------------+---------------------------+
            | ``AtlasDbConfig``                 | ``AtlasDbRuntimeConfig``  |
            +===================================+===========================+
            | :strike:`enableSweep`             | **enabled**               |
            +-----------------------------------+---------------------------+
            | :strike:`sweepPauseMillis`        | **pauseMillis**           |
            +-----------------------------------+---------------------------+
            | :strike:`sweepReadLimit`          | **readLimit**             |
            +-----------------------------------+---------------------------+
            | :strike:`sweepCandidateBatchHint` | **candidateBatchHint**    |
            +-----------------------------------+---------------------------+
            | :strike:`sweepDeleteBatchHint`    | **deleteBatchHint**       |
            +-----------------------------------+---------------------------+

           Specifying any of the above install options will result in **AtlasDB failing to start**. Check the full configuration docs `here <http://palantir.github.io/atlasdb/html/configuration/index.html>`__.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1976>`__)

    *    - |fixed|
         - Fixed a bug that caused AtlasDB internal tables (e.g. the Transactions table or the Punch table) to be **wiped when read from the AtlasDB Console**.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2106>`__)

    *    - |userbreak|
         - The Atomix algorithm implementation for the TimeLock server and the corresponding configurations have been removed.
           The default algorithm for ``TimeLockServer`` has been changed to Paxos.
           This should not affect users as Atomix should not have been used due to known bugs in the implementation.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2082>`__)

    *    - |userbreak|
         - The previously deprecated RocksDBKVS has been removed.
           Developers that relied on RocksDB for testing should move to H2 on JdbcKvs.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1966>`__)

    *    - |fixed| |improved|
         - Sweep now dynamically adjusts the number of (cell, ts) pairs across runs:

           - On a failure run, sweep halves the number of pairs to read and to delete on subsequent runs.
           - On a success run, sweep slowly increases the number of (cell, ts) pairs to read and to delete on subsequent runs, up to a configurable maximum.

           This should fix the issue where we were unable to sweep cells with a high number of mutations.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2060>`__)

    *    - |improved|
         - Default configs which tune sweep runs were lowered, to ensure that sweep works in any situation. For more information, please check the :ref:`sweep docs<sweep>`.
           Please delete any config overrides regarding sweep and use the default values, to ensure a sane run of sweep.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2104>`__)

    *    - |new|
         - AtlasDB now instruments embedded timestamp and lock services when no leader block is present in the config, to expose aggregate response time and service call metrics.
           Note that this may cause a minor performance hit.
           If that is a concern, the instrumentation can be turned off by setting the tritium system properties ``instrument.com.palantir.timestamp.TimestampService`` and
           ``instrument.com.palantir.lock.RemoteLockService`` to false and restarting the service.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2073>`__)

    *    - |new|
         - AtlasDB now adds endpoints for sweeping a specific table, with options for startRow and batch config parameters.
           This should be used in place of the deprecated sweep CLIs. Check the endpoints documentation `here <http://palantir.github.io/atlasdb/html/cluster_management/sweep/sweep-endpoints.html>`__.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2040>`__)

    *    - |improved|
         - Improved performance of timestamp and lock requests on clusters with a leader block and a single node.
           If a single leader is configured, timestamp and lock requests will no longer use HTTPS/Jetty.
           In addition to the minor perf improvement, this fixes an issue causing livelock/deadlock when the leader is under heavy load.
           We recommend HA clusters under heavy load switch to using a standalone timestamp service, as they may also be vulnerable to this failure mode.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2091>`__)

    *    - |improved| |devbreak|
         - The dropwizard independent implementation of the TimeLock server has been separated into a new project, ``timelock-impl``.
           This should not affect users directly, unless they depended on classes from within the TimeLock server.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2081>`__)

    *    - |fixed|
         - JDBC KVS now batches cells in put/delete operations via the config parameter ``batchSizeForMutations``.
           This will prevent the driver from throwing due to many parameters in the resulting SQL select query. Also,
           the batch size for getRows is now controlled by a config parameter ``rowBatchSize``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2093>`__)

    *    - |fixed|
         - AtlasDB clients now retry lock requests if the server loses leadership while the request is blocking.
           In the past, this scenario would cause the server to return 500 responses that were not retried by the client.
           Now the server returns 503 responses, which triggers the correct retry logic.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2098>`__)

    *    - |fixed|
         - AtlasDB now generates Maven POM files for shadowed jars correctly.
           Previously, we would regenerate the XML for shadow dependencies by creating a node with corresponding groupId, artifactId, scope and version tags *only*, which is incorrect because it loses information about, for example, specific or transitive exclusions.
           We now respect these additional tags.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2092>`__)

    *    - |fixed|
         - Fixed a bug where a timelock server instance could get stuck in an infinite loop if cutoff from the other nodes and failed to achieve a quorum.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1983>`__)

    *    - |improved| |userbreak|
         - Improved the way rows and named columns are outputted in AtlasConsole to be more intuitive and easier to use. Note that this may break existing AtlasConsole scripts.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2067>`__)

    *    - |fixed|
         - Added backwards compatibility for the changes introduced in `#2067 <https://github.com/palantir/atlasdb/pull/2067>`__, in particular, for passing row values into AtlasConsole functions.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2080>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.46.0
=======

This version was skipped due to issues on release. No artifacts with this version were ever published.

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.45.0
=======

19 June 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |devbreak| |improved|
         - Upgraded all usages of http-remoting to remoting2.
           Previously, depending on the use case, AtlasDB would use http-remoting, remoting1 and remoting2.
           Developers may need to check their dependencies, as well as update instantiation of their calls to ``TransactionManagers.create()`` to use the remoting2 API.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1999>`__)

    *    - |devbreak| |improved|
         - AtlasDB has updated Feign to 8.17.0 and OkHttp to 3.4.1, following remoting2 in the `palantir/http-remoting <https://github.com/palantir/http-remoting>`__ library.
           We previously used Feign 8.6.1 and OkHttp 2.5.0.
           Developers may need to check their dependencies, especially if they relied on AtlasDB to pull in Feign and OkHttp as there were package name changes.
           (`Pull Request 1 <https://github.com/palantir/atlasdb/pull/2006>`__) and
           (`Pull Request 2 <https://github.com/palantir/atlasdb/pull/2061>`__)

    *    - |devbreak| |improved|
         - AtlasDB now shades Feign and Okio (same as `palantir/http-remoting <https://github.com/palantir/http-remoting>`__).
           This was done to enable us to synchronize with remoting2 while limiting breaks for users of older versions of Feign, especially given an API break in Feign 8.16.
           Users who previously relied on AtlasDB to pull in these libraries may experience a compile break, and should consider explicitly depending on them.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2061>`__)

    *    - |devbreak| |improved|
         - Converted all compile time uses of Guava's ``com.google.common.base.Optional`` class to the Java8 equivalent ``java.util.Optional``.
           This change should not directly affect users as there is no change to `json` or `yml` representations of AtlasDB configurations.
           This is a relatively straightforward compile time break for products consuming AtlasDB libraries.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2016>`__)

    *    - |deprecated| |improved|
         - `AssertUtils` logging methods will now ask for an SLF4J logger to log to, instead of using a default logger.
           This should make log events from AssertUtils easier to filter.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2015>`__)

    *    - |fixed|
         - JDBC KVS now batches cells in get operations via the config parameter ``batchSizeForReads``.
           This will prevent the driver from throwing due to many parameters in the resulting SQL select query.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2063>`__)

    *    - |fixed|
         - The CLI distribution can now be run against JDBC with hikari connection pools.
           In the past, it would fail to resolve the configuration due to a missing runtime dependency.
           Note: this is not a problem if running with the dropwizard bundle.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2063>`__)

    *    - |fixed|
         - Fixed an issue where the lock service was not properly shut down after losing leadership, which could result in threads blocking unnecessarily.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2014>`__)

    *    - |fixed|
         - Lock refresh requests are no longer restricted by lock service threadpool limiting.
           This allows transactions to make progress even when the threadpool is full.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2025>`__)

    *    - |fixed|
         - Lock service now ensures that locks are reaped in a more timely manner.
           Previously the lock service could allow locks to be held past expiration, if they had a timeout shorter than the longest timeout in the expiration queue.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2041>`__)

    *    - |new|
         - Added a getRow() command to AtlasConsole for retrieving a single row.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1992>`__)

    *    - |new|
         - Added a rowComponents() function to the AtlasConsole table() command to allow you to easily view the fields that make up a row key.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2037>`__)

    *    - |new|
         - The default lock timeout is now configurable.
           Currently, the default lock timeout is 2 minutes.
           This can cause a large delay if a lock requester's connection has died at the time it receives the lock.
           Since TransactionManagers#create provides an auto-refreshing lock service, it is safe to lower the default timeout to reduce the delay that happens in this case.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2026>`__)

    *    - |improved|
         - The priority of logging on background sweep was increased from debug to info or warn.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2031>`__)

    *    - |improved|
         - The lock service state logger now has a reduced memory footprint.
           It also now logs the locking mode for each lock.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1891>`__)

    *    - |improved|
         - Reduced the logging level of some messages relating to check-and-set operations in ``CassandraTimestampBoundStore`` to reduce noise in the logs.
           These were designed to help debugging the ``MultipleRunningTimestampServicesException`` issues but we no longer require them to log all the time.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2048>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.44.0
=======

8 June 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - ``TimestampService`` now uses atomic variables rather than locking, and refreshes the bound synchronously rather than asynchronously.
           This should improve performance somewhat under heavy load, although there will be a short pause in responses when the bound needs to be refreshed (currently, once every 1 million timestamps).
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1910>`__)

    *    - |improved|
         - Added new meter metrics for cells swept/deleted and failures to acquire persistent lock.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1946>`__)

    *    - |improved|
         - Cassandra thrift driver has been bumped to version 3.10.
           This will fix a bug (#1654) that caused Atlas probing downed Cassandra nodes every few minutes to see if they were up and working yet to eventually take out the entire cluster by steadily building up leaked connections, due to a bug in the underlying driver.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1970>`__)

    *    - |improved|
         - Read-only transactions will no longer make a remote call to fetch a timestamp, if no work is done on the transaction.
           This will benefit services that execute read-only transactions around in-memory cache operations, and frequently never fall through to perform a read.
           (`Pull Request <https://github.com/palantir/1996>`__)

    *    - |improved|
         - Timelock service now includes user agents for all inter-node requests.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1971>`__)

    *    - |new|
         - Timelock now tracks metrics for leadership elections, including leadership gains, losses, and proposals.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1971>`__)

    *    - |fixed|
         - Fixed a severe performance regression in getRange() on Oracle caused by an inadequate query plan being chosen sometimes.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1989>`__)

    *    - |fixed|
         - Fixed a potential out-of-memory issue by limiting the number of rows getRange() can request from Postgres at once.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2003>`__)

    *    - |fixed|
         - KVS migration CLI will now clear the checkpoint tables that are required while the migration is in progress but not after the migration is complete.
           The tables were previously left hanging and the user had to delete/truncate them.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1927>`__)

    *    - |devbreak|
         - Some downstream projects were using empty table metadata for dev-laziness reasons in their tests.
           This is no longer permitted, as it leads to many (unsolved) questions about how to deal with such a table.
           If this breaks your tests, you can fix it with making real schema for tests or by switching to AtlasDbConstants.GENERIC_TABLE_METADATA
           (`Pull Request <https://github.com/palantir/1925>`__)

    *    - |userbreak| |fixed|
         - Fixed a bug that caused Cassandra to always use the minimum compression block size of 4KB instead of the requested compression block size.
           Users must explicitly rewrite table metadata for any tables that requested explicit compression, as any tables that were created previously will not respect the compression block size in the schema.
           This can have a very large performance impact (both positive and negative in different cases), so users may need to remove the explicit compression request from their schema if this causes a performance regression.
           Users that previously attempted to set a compression block size that was not a power of 2 will also need to update their schema because Cassandra only allows this value to be a power of 2.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1995>`__)

    *    - |fixed|
         - Fixed a potential out-of-memory issue by limiting the number of rows getRange() can request from Postgres at once.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2003>`__)


.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.43.0
=======

25 May 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - For requests that fail due to to networking or other IOException, the AtlasDB client now backs off before retrying.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1958>`__)

    *    - |userbreak| |improved|
         - The ``acquire-backup-lock`` endpoint of ``PersistentLockService`` now returns a 400 response instead of a 500 response when no reason for acquiring the lock is provided.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1909>`__)

    *    - |fixed|
         - ``PaxosTimestampBoundStore`` now throws ``NotCurrentLeaderException``, invalidating the timestamp store, if a bound update fails because another timestamp service on the same node proposed a smaller bound for the same sequence number.
           This was added to address a very specific race condition leading to an infinite loop that would saturate the TimeLock cluster with spurious Paxos messages; see `issue 1941 <https://github.com/palantir/atlasdb/issues/1941>`__ for more detail.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1942>`__)

    *    - |deprecated|
         - The FastForwardTimestamp and FetchTimestamp CLIs have been deprecated.
           Please use the ``timestamp-management/fast-forward`` and ``timestamp/fresh-timestamp`` endpoints instead.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1936>`__)

    *    - |improved|
         - Sweep now batches delete calls before executing them.
           This should improve performance on relatively clean tables by deleting more cells at a time, leading to fewer DB operations and taking out the backup lock less frequently.
           The new configuration parameter ``sweepDeleteBatchHint`` determines the approximate number of (cell, timestamp) pairs deleted in a single batch.
           Please refer to the :ref:`documentation <sweep_tunable_parameters>` for details of how to configure this.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1911>`__)

    *    - |changed|
         - :ref:`Sweep metrics <dropwizard-metrics>` now record counts of cell-timestamp pairs examined rather than the count of entire cells examined. This provides more accurate insight on the work done by the sweeper.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1911>`__)

    *    - |deprecated|
         - The Sweep CLI configuration parameters ``--batch-size`` and ``--cell-batch-size`` have been deprecated, as we now batch on cell-timestamp pairs rather than by rows and cells.
           Please use the ``--candidate-batch-hint`` (batching on cells) instead of ``--batch-hint`` (batching on rows), and ``--read-limit`` instead of ``--cell-batch-size`` (:ref:`docs <sweep_tunable_parameters>`).
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1962>`__)

    *    - |deprecated|
         - The background sweep configuration parameters ``sweepBatchSize`` (which used to batch on rows) and ``sweepCellBatchSize`` have been deprecated in favour of ``sweepCandidateBatchHint`` (which now batches on cells) and ``sweepReadLimit`` respectively.
           If your application configures either of these values, please look at more details in the :ref:`docs <sweep_tunable_parameters>`.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1945>`__)

    *    - |fixed|
         - After the Pull Request `#1808 <https://github.com/palantir/atlasdb/pull/1808>`__ the TimeLock Server did not gate the lock service behind the ``AwaitingLeadershipProxy``. This could lead to data corruption in very rare scenarios. The affected TimeLock server versions are not distributed anymore internally.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1955>`__)

    *    - |fixed|
         - ``TimestampAllocationFailures`` now correctly propagates ``ServiceNotAvailableException`` if thrown from the timestamp bound store.
           Previously, a ``NotCurrentLeaderException`` that was thrown from the timestamp store would be wrapped in ``RuntimeException`` before being thrown out, meaning that TimeLock clients saw 500s instead of the intended 503s. This could lead to inneficient retry logic.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1954>`__)

    *    - |devbreak|
         - New ``KeyValueService`` method ``getCandidateCellsForSweeping()`` that should eventually replace ``getRangeOfTimestamps()``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1943>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>


=======
v0.42.2
=======

25 May 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - ``PaxosTimestampBoundStore`` now throws ``TerminalTimestampStoreException`` if a bound update fails because another timestamp service on the same node proposed a smaller bound, or if another node proposed a bound update we were not expecting.
           Previously, a ``NotCurrentLeaderException`` that was thrown from the timestamp store would be wrapped in ``RuntimeException`` before being thrown out, meaning that TimeLock clients saw 500s instead of the intended 503s.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/TBC>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>


=======
v0.42.1
=======

24 May 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - ``PaxosTimestampBoundStore`` now throws ``NotCurrentLeaderException``, invalidating the timestamp store, if a bound update fails because another timestamp service on the same node proposed a smaller bound for the same sequence number.
           This was added to address a very specific race condition leading to an infinite loop that would saturate the TimeLock cluster with spurious Paxos messages; see `issue 1941 <https://github.com/palantir/atlasdb/issues/1941>`__ for more detail.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1942>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>


=======
v0.42.0
=======

23 May 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |fixed|
         - ``PaxosTimestampBoundStore``, the bound store for Timelock, will now throw ``NotCurrentLeaderException`` instead of ``MultipleRunningTimestampServiceError`` when a bound update fails.
           The cases where this can happen are explained by a race condition that can occur after leadership change, and it is safe to let requests be retried on another server.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1934>`__)

    *    - |fixed|
         - A 500 ms backoff has been added to the our retry logic when the client has queried all the servers of a cluster and received a ``NotCurrentLeaderException``.
           Previously in this case, our retry logic would dictate infinitely many retries with a 1 ms backoff.
           The new backoff should reduce contention during leadership elections, when all nodes throw ``NotCurrentLeaderException``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1939>`__)

    *    - |improved|
         - Timelock server can now start with an empty clients list.
           Note that you currently need to restart timelock when adding clients to the configuration.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1907>`__)

    *    - |improved|
         - Default ``gc_grace_seconds`` set by AtlasDB for Cassandra tables has been changed from four days to one hour, allowing Cassandra to start cleaning up swept data sooner after sweeping.

           This parameter is set at table creation time, and it will only apply for new tables.
           Existing customers can update the ``gc_grace_seconds`` of existing tables to be one hour if they would like to receive this benefit now. We will also be adding functionality to auto-update this for existing tables in a future release.
           There is no issue with having tables with different values for ``gc_grace_seconds``, and this can be updated at any time.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1726>`__)

    *    - |improved|
         - ``ProfilingKeyValueService`` now has some additional logging mechanisms for logging long-running operations on WARN level, enabled by default.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1801>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

=======
v0.41.0
=======

17 May 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |userbreak| |changed|
         - Projects ``atlasdb-commons``, ``commons-annotations``, ``commons-api``, ``commons-executors``, ``commons-proxy``, and ``lock-api`` no longer force Java 6 compatibility.
           This eliminates the need for a Java 6 compiler to compile AtlasDB.
           However, users can no longer compile against AtlasDB artifacts using Java 6 or 7; they must use Java 8 if depending on these AtlasDB projects.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1887>`__)

    *    - |devbreak| |improved|
         - The format of serialized exceptions occurring on a remote host has been brought in line with that of the `palantir/http-remoting <https://github.com/palantir/http-remoting>`__ library.
           This should generally improve readability and also allows for more meaningful messages to be sent; we would previously return message bodies with no content for some exceptions (such as ``NotCurrentLeaderException``).
           In particular, the assumption that a status code of 503 definitively means that the node being contacted is not the leader is no longer valid.
           That said, existing AtlasDB clients will still behave correctly even with a new TimeLock.
           (`Pull Request 1 <https://github.com/palantir/atlasdb/pull/1831>`__,
           `Pull Request 2 <https://github.com/palantir/atlasdb/pull/1808>`__)

    *    - |new|
         - Timelock server now has jar publication in addition to dist publication.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1898>`__)

    *    - |new| |fixed|
         - TimeLock clients may now receive an HTTP response with status code 503, encapsulating a ``BlockingTimeoutException``.
           This response is returned if a client makes a lock request that blocks for long enough that the server's idle timeout expires; clients may (immediately) retry the request.
           Previously, these requests would be failed with a HTTP-level exception that the stream was closed.
           We have rewritten clients constructed via ``AtlasDbHttpClients`` to account for this new behaviour, but custom clients directly accessing the lock service may be affected.
           This feature is disabled by default, but can be enabled following the TimeLock server configuration :ref:`docs <timelock-server-time-limiting>`.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1808>`__)

    *    - |fixed|
         - ``DbKvs.getRangeOfTimestamps()`` now returns the entire range of timestamps requested for.
           Previously, this would only return a range corresponding to the first page of results.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1872>`__)

    *    - |fixed|
         - AtlasDB Console no longer errors on range requests that used a column selection and had more than one batch of results.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1876>`__)

    *    - |improved|
         - The ``PaxosQuorumChecker`` thread pool which is used to dispatch requests to other nodes during leadership elections is now instrumented with Dropwizard metrics.
           This will be useful for debugging `PaxosQuorumChecker can leave hanging threads <https://github.com/palantir/atlasdb/issues/1823>`__.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1849>`__)

    *    - |fixed|
         - Import ordering and license generation in generated IntelliJ project files now respect Baseline conventions.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1893>`__)

    *    - |fixed| |improved|
         - Cassandra thrift depedencies have been bumped to newer versions; should fix a bug (#1654) that caused Atlas probing downed Cassandra nodes every few minutes to see if they were up and working yet to eventually take out the entire cluster by steadily building up leaked connections, due to a bug in the underlying driver.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1896>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>


=======
v0.40.1
=======

4 May 2017

This release contains (almost) exclusively baseline-related changes.

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |devbreak|
         - The Lock Descriptor classes (``AtlasCellLockDescriptor`` etc.), static factories (e.g. ``LockCollections``) and ``LockClient`` have been made final.
           If this is a concern, please contact the AtlasDB team.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1857>`__)

    *    - |devbreak|
         - Removed package ``atlasdb-exec``. If you require this package, please file a ticket to have it reinstated.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1861>`__)

    *    - |changed|
         - Our dependency on immutables was bumped from 2.2.4 to 2.4.0, in order to fix an issue with static code analysis reporting errors in generated code.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1853>`__)

    *    - |devbreak|
         - Renamed the following classes to match baseline rules. In each case, acronyms were lowercased, e.g. ``CQL`` becomes ``Cql``.

              - ``CqlExpiringKeyValueService``
              - ``CqlKeyValueService``
              - ``CqlKeyValueServices``
              - ``CqlStatementCache``
              - ``KvTableMappingService``
              - ``TransactionKvsWrapper``

           (`Pull Request <https://github.com/palantir/atlasdb/pull/1853>`__)

    *    - |devbreak|
         - Relax the signature of KeyValueService.addGarbageCollectionSentinelValues() to take an Iterable instead of a Set.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1843>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>


=======
v0.40.0
=======

28 Apr 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |userbreak|
         - AtlasDB will refuse to start if backed by Postgres 9.5.0 or 9.5.1.
           These versions contain a known bug that causes incorrect results to be returned for certain queries.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1820>`__)

    *    - |userbreak| |improved|
         - The lock server now will dump all held locks and outstanding lock requests in YAML file, when logging state requested, for easy readability and further processing.
           This will make debuging lock congestions easier. Lock descriptors are changed with places holders and can be decoded using descriptors file,
           which will be written in the folder. Information like requesting clients, requesting threads and other details can be found in the YAML.
           Note that this change modifies serialization of lock tokens by adding the name of the requesting thread to the lock token; thus, TimeLock Servers are no longer compatible with AtlasDB clients from preceding versions.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1792>`__)

    *    - |devbreak| |fixed|
         - Correct ``TransactionManagers.createInMemory(...)`` to conform with the rest of the api by accepting a ``Set<Schema>`` object.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1859>`__)

    *    - |new|
         - The lock server can now limit the number of concurrent open lock requests from the same client.
           This behavior can be enabled with the flag ``useClientRequestLimit``. It is disabled by default.
           For more information, see the :ref:`docs <timelock-server-further-config>`.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1785>`__)

    *    - |devbreak| |new|
         - The ``TransactionManager`` and ``KeyValueService`` interfaces have new methods that must be implemented by applications that have custom implementations of those interfaces.
           These new methods are ``TransactionManager.getKeyValueServiceStatus()`` and ``KeyValueService.getClusterAvailabilityStatus()``.

           Applications can now call ``TransactionManager.getKeyValueServiceStatus()`` to determine the health of the underlying KVS.
           This is designed for applications to implement their availability status taking into account the :ref:`kvs health <kvs-status-check>`.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1832>`__)

    *    - |improved|
         - On graceful shutdown, the background sweeper will now release the backup lock if it holds it.
           This should reduce the need for users to manually reset the ``_persisted_locks`` table in the event that they restarted a service while it was holding the lock.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1847>`__)

    *    - |improved|
         - Improved performance of ``getRange()`` on DbKvs. Range requests are now done with a single round trip to the database.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1805>`__)

    *    - |devbreak|
         - ``atlasdb-config`` now pulls in two more dependencies - the Jackson JDK 8 and JSR 310 modules (``jackson-datatype-jdk8`` and ``jackson-datatype-jsr310``).
           These are required by the `palantir/http-remoting <https://github.com/palantir/http-remoting>`__ library.
           This behaviour is consistent with our existing behaviour for Jackson modules (JDK 7, Guava and Joda Time).
           If you do encounter breaks due to this addition, please contact the AtlasDB team for support.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1810>`__)

    *    - |deprecated|
         - ``ConflictDetectionManagers.createDefault(KeyValueService)`` has been deprecated.
           If you use this method, please replace it with ``ConflictDetectionManagers.create(KeyValueService)``.
           (`Pull Request 1 <https://github.com/palantir/atlasdb/pull/1822>`__) and (`Pull Request 2 <https://github.com/palantir/atlasdb/pull/1850>`__)


.. <<<<------------------------------------------------------------------------------------------------------------->>>>


=======
v0.39.0
=======

19 Apr 2017

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |improved|
         - Refactored ``AvailableTimestamps`` reducing overzealous synchronization.
           Giving out timestamps is no longer blocking on refreshing the timestamp bound if there are enough timestamps to give out with the current bound.
           This improves latency of timestamp requests under heavy load; we have seen an approximately 30 percent improvement on internal benchmarks.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1783>`__)

    *    - |new|
         - The lock server now has a ``SlowLockLogger``, which logs at INFO in the service logs if a lock request receives a response after at least a given amount of time (10 seconds by default).
           This is likely to be useful for debugging issues with long-running locks in production.

           Specifically, the timelock server has a configuration parameter ``slowLockLogTriggerMillis`` which defaults to ``10000``.
           Setting this parameter to zero (or any negative number) will disable the new logger.
           If not using timelock, an application can modify the trigger value through ``LockServerOptions`` during initialization in ``TransactionManagers.create``.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1791>`__)

    *    - |deprecated|
         - Deprecated ``InMemoryAtlasDbFactory#createInMemoryTransactionManager``, please instead use the supported ``TransactionManagers.createInMemory(...)`` for your testing.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1784>`__)

    *    - |fixed|
         - Proxies created via ``AtlasDbHttpClients`` now parse ``Retry-After`` headers correctly.
           This manifests as Timelock clients failing over and trying other nodes when receiving a 503 with a ``Retry-After`` header from a remote (e.g. from a TimeLock non-leader).
           Previously, these proxies would immediately retry the connection on the node with a 503 two times (for a total of three attempts) before failing over.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1782>`__)

    *    - |new|
         - The ``atlasdb-config`` project now shadows the ``error-handling`` and ``jackson-support`` libraries from `http-remoting <https://github.com/palantir/http-remoting>`__.
           This will be used to handle exceptions in a future release, and was done in this way to avoid causing dependency issues in upstream products.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/1796>`__)

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

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

.. <<<<------------------------------------------------------------------------------------------------------------->>>>

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
         - AtlasDB :ref:`CLIs <clis>` run via the Dropwizard bundle can now work with a Timelock block, and will contact the relevant Timelock server for timestamps or locks in this case.
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
           This prevented CLIs deployed via the Dropwizard bundle from loading configuration properly.
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
