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

.. _sweep_tunable_parameters:

Tunable Configuration Options
-----------------------------

The following optional parameters can be tuned to optimize Sweep performance for a specific AtlasDB instance.
You may set them as part of your :ref:`AtlasDB runtime configuration <atlas-config>`, or as a CLI option if you are running sweep using the CLI.
Note that some of these parameters are just used as a hint. Sweep dynamically modifies these parameters according to successful or failed runs.

.. csv-table::
   :header: "AtlasDB Runtime Config", "Endpoint Option", "Default", "Description"
   :widths: 20, 20, 40, 200

   ``enabled``, "Only specified in config", "1,000", "Target number of (cell, timestamp) pairs to examine in a single run."
   ``readLimit``, ``maxCellTsPairsToExamine``, "1,000", "Target number of (cell, timestamp) pairs to examine in a single run."
   ``candidateBatchHint``, ``candidateBatchSize``, "1", "Target number of candidate (cell, timestamp) pairs to load at once. Decrease this if sweep fails to complete (for example if the sweep job or the underlying KVS runs out of memory). Increasing it may improve sweep performance."
   ``deleteBatchHint``, ``deleteBatchSize``, "1,000", "Target number of (cell, timestamp) pairs to delete in a single batch. Decrease if sweep cannot progress pass a large row or a large cell. Increasing it may improve sweep performance."
   ``pauseMillis``, "Only specified in config", "5000 ms", "Wait time between row batches. Set this if you want to use less shared DB resources, for example if you run sweep during user-facing hours."

.. csv-table::
   :header: "CasandraKeyValueService Config", "Endpoint Option", "Default", "Description"
   :widths: 20, 20, 40, 200

   ``timestampsGetterBatchSize``, "Only specified in config", "1,000", "Specify a limit on the maximum number of columns to fetch in a single database query. Set this to a number fewer than your number of columns if your Cassandra OOMs when attempting to run sweep with even a small row batch size. This parameter should be used when tuning Sweep for cells with many historical versions."

Following is more information about when each of the batching parameters is useful.
In short, the recommendation is:

- Decrease ``candidateBatchHint`` and ``readLimit`` if there is memory pressure on the client.
- Decrease ``timestampsGetterBatchSize`` if there is memory pressure on Cassandra even after setting ``candidateBatchHint`` down to 1.

Memory pressure on AtlasDB client: decrease candidateBatchHint and readLimit
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Since the number of columns per row can vary widely between tables, only setting a row batch size can lead to sweep batches containing different numbers of cells; setting ``candidateBatchHint`` can even this out.
For example, consider a database that has one table with 1000 rows and 10 columns, a second table with 1000 rows and 100 columns, and that performs best when sweeping at most 10,000 cells at a time.
In this setup, limiting ``candidateBatchHint`` to 100 would ensure that at most 10,000 cells are swept at a time, but would split the first table in 10 batches when only 1 was necessary.
Instead, setting ``readLimit`` to 10,000 would allow both tables to be swept in optimal batches.

Memory pressure in Cassandra: decrease timestampsGetterBatchSize
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The sweep job works by requesting at least one full row at a time from the KVS, including all historical versions of each cell in the row.
In some rare cases, the Cassandra KVS will not be able to construct a single full row in memory, likely due to specific cells being overwritten many times and/or containing very large values.
This situation will manifest with Cassandra OOMing during sweep even if ``sweepBatchSize`` is set to 1.
If that happens, you can use ``timestampsGetterBatchSize`` to instruct Cassandra to read a smaller number of columns at a time before aggregating them into a single row metadata to pass on to the sweeper.
If ``timestampsGetterBatchSize`` is set, Cassandra will read at most one row at a time.
The other batch parameters are still respected, but their values are unlikely to make a difference because the execution time will be dominated by the single-row reads.

.. toctree::
    :maxdepth: 1
    :hidden:

    sweep/background-sweep
    sweep/sweep-endpoints
    sweep/sweep-cli
