.. _sweep:

AtlasDB Sweep Job
=================

.. warning::

   If you're using sweep through either a) the deprecated Sweep CLI or b) the background sweeper on AtlasDB < 0.32.0, it can cause **SEVERE DATA CORRUPTION** to any ongoing backups.

Under normal usage, the data written to the key value service backing AtlasDB is never updated or deleted.
When a row in AtlasDB is "updated", a new version of the row with a higher timestamp is written.
The old row is left untouched. When that row is later read, only the version with the higher timestamp is returned.
Deletions in AtlasDB are similar; in practice they are the same as updates with an empty value.

To prevent an AtlasDB database from growing in size indefinitely, old versions of rows can be deleted through a process called sweeping.
At a high level, this works by periodically scanning the database for rows with multiple versions and deleting the older versions from the database.

Reasons to Sweep
----------------

1. Freeing up Disk Space
    - Putting large amounts of data into cells that are updated creates tables that hold onto data that isn't used. These tables are good candidates to sweep if you want to reclaim disk space.

2. Improving Performance
    - Making many edits to the same row will leave behind many tombstoned entries, so it's advantageous to sweep these tables to increase performance.

3. Improving Stability
    - We have seen situations in the field where reading rows with many historical values caused the underlying Cassandra KVS to run out of memory.
      This situation could have been mitigated by sweeping the table in question.

Ways to Sweep
-------------

- :ref:`Background sweep process<background-sweep>` scheduled periodically:
  Under normal use, the sweep job is intended to run at a constant interval as a background sweep process that does not consume significant system resources.
  Background sweep is enabled by default. If needed, background sweep can be disabled without bouncing the host service.

- :ref:`Sweep Endpoint<atlasdb-sweep-endpoints>` triggered manually:
  You may trigger the sweep job on demand via an HTTP request. This could be useful in order to address any one-off performance issues.
  Long-running AtlasDB instances with large data scale may want to manually sweep specific tables before enabling the background sweeper.
  Given that the background sweep job chooses which tables it sweeps, you can ensure that a one-off large sweep job occurs during non-peak usage hours.

Tracking if Sweep Progress
--------------------------

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
   ``readLimit``, ``maxCellTsPairsToExamine``, "128", "Target number of (cell, timestamp) pairs to examine in a batch of sweep."
   ``candidateBatchHint``, ``candidateBatchSize``, "128", "Target number of candidate (cell, timestamp) pairs to load at once. Decrease this if sweep fails to complete (for example if the sweep job or the underlying KVS runs out of memory). Increasing it may improve sweep performance."
   ``deleteBatchHint``, ``deleteBatchSize``, "128", "Target number of (cell, timestamp) pairs to delete in a single batch. Decrease if sweep cannot progress pass a large row or a large cell. Increasing it may improve sweep performance."
   ``pauseMillis``, "Only specified in config", "5000 ms", "Wait time between row batches. Set this if you want to use less shared DB resources, for example if you run sweep during user-facing hours."

Following is more information about when each of the batching parameters is useful.
In short, the recommendation is:

- Decrease ``candidateBatchHint`` and ``readLimit`` if there is memory pressure on the client.
- Decrease ``candidateBatchHint`` if there is memory pressure on the KVS.

Memory pressure on AtlasDB client: decrease candidateBatchHint and readLimit
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``candidateBatchHint`` specifies the number of values to load from the KVS on each round-trip. ``readLimit`` specifies the number of values to read from the KVS in a batch.
If each batch takes little time, it's advisable to increase the ``readLimit`` and ``deleteBatchHint``, to make sweep's batches bigger.
If the client is OOMing, it's advisable to decrease the ``readLimit``, to have a lower number of values per batches.
Since each batch contains at least ``candidateBatchHint`` values, if ``readLimit`` reaches the same value as ``candidateBatchHint`` you should reduce the ``candidateBatchSize`` config.
Note that since sweep still needs to sweep at least a full row on every batch, it might be the case that the client is OOMing because the row it's trying to sweep is very large. Please contact the AtlasDB team if you think you're hitting this issue.

Memory pressure in the backing KVS: decrease candidateBatchHint
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The sweep job works by requesting ``candidateBatchHint`` values from the KVS in each round-trip.
In some rare cases, the Cassandra KVS will not be able to fetch ``candidateBatchHint`` values, likely due to specific cells being overwritten many times and/or containing very large values.
If that happens, KVS calls will timeout and sweep will reduce ``candidateBatchHint`` automatically, trying to fetch fewer values in the following round-trips to the KVS.
If subsequent KVS round-trips are successful, the value of ``candidateBatchHint`` is slowly increased back to the configured value, which is also the maximum it reaches. Therefore, if the maximum is too high, failures will happen again.

You can check the sweep logs to verify if this is happening frequently — and if this is the case — reduce this config to a value that the load on the KVS doesn't trigger failures and sweep is able to run.

.. toctree::
    :maxdepth: 1
    :hidden:

    sweep/background-sweep
    sweep/sweep-endpoints
    sweep/sweep-cli
    sweep/sweep-logs
