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
.. role:: strike
    :class: strike

.. |userbreak| replace:: :changetype-breaking:`USER BREAK`
.. |devbreak| replace:: :changetype-breaking:`DEV BREAK`
.. |new| replace:: :changetype-new:`NEW`
.. |fixed| replace:: :changetype-fixed:`FIXED`
.. |changed| replace:: :changetype-changed:`CHANGED`
.. |improved| replace:: :changetype-improved:`IMPROVED`
.. |deprecated| replace:: :changetype-deprecated:`DEPRECATED`

.. toctree::
  :hidden:

=======
develop
=======

.. replace this with the release date

.. list-table::
    :widths: 5 40
    :header-rows: 1

    *    - Type
         - Change

    *    - |userbreak|
         - If AtlasDB is used with TimeLock, and the TimeLock client name is different than either the Cassandra ``keyspace``, Postgres ``dbName``, or Oracle ``sid``, *AtlasDB will fail to start*.
           This was done to avoid risks of data corruption if these are accidentally changed independently.
           If the above parameters contradict, please contact the AtlasDB team to change the TimeLock client name. Changing it in config without additional action may result in *severe data corruption*.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2263>`__)

    *    - |new|
         - AtlasDB introduces a top-level ``namespace`` configuration parameter, which is used to set the ``keyspace`` in Cassandra and the ``client`` in TimeLock.
           Following the previous change, we unify both the configs that cannot be changed separately in one single config. Therefore it is suggested that AtlasDB users follow and use the new parameter to specify both the deprecated ones.
           Note that if the new ``namespace`` config contradicts with either the Cassandra ``keyspace`` and/or the TimeLock ``client`` configs, *AtlasDB will fail to start*.
           Please consult the documentation for :ref:`AtlasDB Configuration <atlas-config>` for details on how to set this up.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2263>`__)

    *    - |deprecated|
         - As a followup of the ``namespace`` change, the Cassandra ``keyspace`` and TimeLock ``client`` configs were deprecated.
           As said previously, please use the ``namespace`` root level config to specify both of these parameters.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2263>`__)

    *    - |improved|
         - If ``enableOracleEnterpriseFeatures`` if configured to be false, you will now see warnings asking you to run Oracle compaction manually.
           This will help make non-EE Oracle users aware of potential database bloat.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2277>`__)

    *    - |fixed|
         - Fixed a case where logging an expection suppressing itself would cause a stack overflow.
           See https://jira.qos.ch/browse/LOGBACK-1027.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2242>`__)

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

    *    - |new|
         - AtlasDB now produces a new artifact, ``timelock-agent``.
           Users who wish to run TimeLock Server outside of a Dropwizard environment should now be able to do so more easily, by supplying the TimeLock Agent with a *registrar* that knows how to register Java resources and expose suitable HTTP endpoints.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2247>`__)

    *    - |improved|
         - Timelock now creates client namespaces the first time they are requested, rather than requiring them to be specified in config.
           This means that specifying a list of clients in Timelock configuration will no longer have any effect. Further, a new configuration property called ``max-number-of-clients`` has been introduced in ``TimeLockRuntimeConfiguration``. This can be used to limit the number of clients that will be created dynamically, since each distinct client has some memory, disk space, and CPU overhead.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2252>`__)

    *    - |deprecated|
         - ``putUnlessExists`` methods in schema generated code have been marked as deprecated as the naming can be misleading, leading to accidental value overwrites. The recommended alternative is doing a separate read and write in a single transaction.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2271>`__)

    *    - |fixed|
         - CharacterLimitType now has fields marked as final.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2259>`__)

    *    - |fixed|
         - ``kvs-slow-log`` now uses ``logsafe`` to support sls-compatible logging.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2222>`__)

    *    - |fixed|
         - The scrubber can no longer get backed up if the same cell is overwritten multiple times by hard delete transactions.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2232>`__)

    *    - |changed|
         - The ``RangeMigrator`` interface now contains an additional method ``logStatus(int numRangeBoundaries)``.
           This method is used to log the state of migration for each table when starting or resuming a KVS migration.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2241>`__)

    *    - |changed|
         - Updated our dependency on ``sls-packaging`` from 2.3.1 to 2.4.0.
           (`Pull Request <https://github.com/palantir/atlasdb/pull/2268>`__)

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
