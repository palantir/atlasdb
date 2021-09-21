.. _standard-sweep:

Standard Sweep
==============

Standard sweep scans a table to find rows with multiple versions, then deletes the older versions from the database.

Ways to use Standard Sweep
--------------------------

- :ref:`Background sweep process<background-sweep>` scheduled periodically:
  Under normal use, the sweep job is intended to run at a constant interval as a background sweep process that does not consume significant system resources.
  Background sweep is enabled by default. If needed, background sweep can be disabled without bouncing the host service.

- :ref:`Sweep Endpoint<atlasdb-sweep-endpoints>` triggered manually:
  You may trigger the sweep job on demand via an HTTP request. This could be useful in order to address any one-off performance issues.
  Long-running AtlasDB instances with large data scale may want to manually sweep specific tables before enabling the background sweeper.
  Given that the background sweep job chooses which tables it sweeps, you can ensure that a one-off large sweep job occurs during non-peak usage hours.

Tracking Sweep Progress
-----------------------

In order to track sweep's general workflow, and which log lines you should expect, see :ref:`Sweep Logs<sweep-logs>`.

.. _sweep_tunable_parameters:

Tunable Configuration Options
-----------------------------

The following optional parameters can be tuned to optimize Sweep performance for a specific AtlasDB instance.
You may set them as part of your :ref:`AtlasDB runtime configuration <atlas-config>`, or as a CLI option if you are running sweep using the CLI.
Note that some of these parameters are just used as a hint. Sweep dynamically modifies these parameters according to successful or failed runs.

.. csv-table::
   :header: "AtlasDB Runtime Config", "Endpoint Option", "Default", "Description"
   :widths: 20, 20, 40, 200

   ``enabled``, "Only specified in config", "true", "Whether the background sweeper should run."
   ``sweepThreads``, "Only specified in config", "1", "The number of threads to run sweep with. Changes require a restart of any service node to take effect. Not recommended for Cassandra KVS. Note that threads will contend with each other for the backup lock on deletes."
   ``readLimit``, ``maxCellTsPairsToExamine``, "128", "Target number of (cell, timestamp) pairs to examine in a batch of sweep."
   ``candidateBatchHint``, ``candidateBatchSize``, "128", "Target number of candidate (cell, timestamp) pairs to load at once. Decrease this if sweep fails to complete (for example if the sweep job or the underlying KVS runs out of memory). Increasing it may improve sweep performance."
   ``deleteBatchHint``, ``deleteBatchSize``, "128", "Target number of (cell, timestamp) pairs to delete in a single batch. Decrease if sweep cannot progress pass a large row or a large cell. Increasing it may improve sweep performance."
   ``pauseMillis``, "Only specified in config", "5000 ms", "Wait time between row batches. Set this if you want to use less shared DB resources, for example if you run sweep during user-facing hours."
   ``sweepPriorityOverrides``, "Only specified in config", "No overrides", "Fully-qualified names of tables that Sweep should prioritise particularly highly, or ignore completely. Please see `Priority Overrides`_ for more details."

Batch Parameters
~~~~~~~~~~~~~~~~

Following is more information about when each of the batching parameters is useful.
In short, the recommendation is:

- Decrease ``candidateBatchHint`` and ``readLimit`` if there is memory pressure on the client.
- Decrease ``candidateBatchHint`` if there is memory pressure on the KVS.

Memory pressure on AtlasDB client: decrease candidateBatchHint and readLimit
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``candidateBatchHint`` specifies the number of values to load from the KVS on each round-trip. ``readLimit`` specifies the number of values that should be swept in a batch, potentially fetching them through multiple KVS round-trips.
If each batch takes little time, it's advisable to increase the ``readLimit`` and ``deleteBatchHint``, to make sweep's batches bigger.
If the client is OOMing, it's advisable to decrease the ``readLimit``, to have a lower number of values per batches.
Since each batch contains at least ``candidateBatchHint`` values, if ``readLimit`` reaches the same value as ``candidateBatchHint`` you should reduce the ``candidateBatchSize`` config.
Note that since sweep still needs to sweep at least a full row on every batch, it might be the case that the client is OOMing because the row it's trying to sweep is very large. Please contact the AtlasDB team if you think you're hitting this issue.

Memory pressure in the backing KVS: decrease candidateBatchHint
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The sweep job works by requesting ``candidateBatchHint`` values from the KVS in each round-trip.
In some rare cases, the Cassandra KVS will not be able to fetch ``candidateBatchHint`` values, likely due to specific cells being overwritten many times and/or containing very large values.
If that happens, KVS requests from AtlasDB will fail and sweep will reduce ``candidateBatchHint`` automatically, trying to fetch fewer values in the following round-trips to the KVS.
If subsequent KVS round-trips are successful, the value of ``candidateBatchHint`` is slowly increased back to the configured value, which is also the maximum it reaches. Therefore, if the configured value is too high, failures will happen again.

You can check the sweep logs to verify if this is happening frequently — and if this is the case — reduce this config to a value that the load on the KVS doesn't trigger failures and sweep is able to run.

.. _sweep-priority-overrides:

Priority Overrides
~~~~~~~~~~~~~~~~~~

.. warning::
   Specifying ``priorityTables`` can be useful for influencing sweep's behaviour in the short run.
   However, if any tables are specified as ``priorityTables``, and the number of priority tables is at least ``sweepThreads``, then no other tables will ever be swept, meaning that old versions of cells for those tables will accumulate.
   It is not intended for priority tables to be specified in a steady state, generally speaking.

There may be situations in which the background sweeper's heuristics for selecting tables to sweep may not satisfy one's requirements.
One can influence the selection process by configuring the ``sweep.sweepPriorityOverrides`` parameter in runtime configuration.

.. csv-table::
   :header: "AtlasDB Runtime Config", "Default", "Description"
   :widths: 20, 40, 200

   ``blacklistTables``, "[]", "List of fully qualified table names (e.g. ``namespace.tablename``) which the background sweeper should not sweep."
   ``priorityTables``, "[]", "List of fully qualified table names which the background sweeper should prioritise for sweep."

Note that the conditions for table selection are only checked between iterations of sweep. The algorithm for deciding
whether a table that is currently being swept should continue to be swept is as follows:

1. If the table being swept is a priority table, continue sweeping it.
2. If there are any priority tables and the table currently being swept is not a priority table, stop sweeping it and
   switch to a random priority table.
3. If the table being swept is not blacklisted, continue sweeping it.
4. There are no priority tables, and the table being swept is blacklisted; select another table using standard sweep
   heuristics.

Note that if blacklist/priority settings are subsequently removed, sweep will continue sweeping the table that those
settings have made it select (in particular, it will not immediately return to the original table it was sweeping).

Also, as we don't know the data distribution of the original table we were sweeping, we don't know how much of the
table we have swept; we thus don't have enough information to update the sweep priority table with our results while
registering that we have only done a partial sweep. In practice, this would tend to mean that the probability the
original table being swept will be picked again for sweeping after overrides have been removed may be a little higher
than if we accounted for the partial sweep.

One possible use of having blacklist tables is for specifying tables which you don't want the background sweeper to
sweep, but you still want to be able to carry out a manual sweep. Note that specifying tables as
``SweepStrategy.NOTHING`` will mean that even manual sweeps will not do anything.

.. toctree::
    :maxdepth: 1
    :hidden:

    standard/background-sweep
    standard/sweep-endpoints
    standard/sweep-cli
    standard/sweep-logs
