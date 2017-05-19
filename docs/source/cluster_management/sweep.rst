.. _sweep:

AtlasDB Sweep Job
=================

.. warning::

   Running the Sweep Job while taking a Backup can cause **SEVERE DATA CORRUPTION** to your Backup.

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

- :ref:`Sweep CLI<atlasdb-sweep-cli>` triggered manually:
  You may trigger the sweep job on demand via the sweep CLI. This could be useful in order to address any one-off performance issues.
  Long-running AtlasDB instances with large data scale may want to manually sweep specific tables before enabling the background sweeper.
  Given that the background sweep job chooses which tables it sweeps, you can ensure that a one-off large sweep job occurs during non-peak usage hours.

.. _sweep_tunable_parameters:

Tunable Configuration Options
-----------------------------

The following optional parameters can be tuned to optimize Sweep performance for a specific AtlasDB instance.
You may set them as part of your :ref:`AtlasDB configuration <atlas-config>`, or as a CLI option if you are running sweep using the CLI.

.. csv-table::
   :header: "AtlasDB Config", "CLI Option", "Default", "Description"
   :widths: 20, 20, 40, 200

   (DEPRECATED) ``sweepBatchSize``, (REMOVED), "100", "Maximum number of rows to sweep at once. Decrease this if sweep fails to complete (for example if the sweep job or the underlying KVS runs out of memory). Increasing it may improve sweep performance. This parameter should be replaced by ``--candidate-batch-hint``."
   (DEPRECATED) ``sweepCellBatchSize``, (REMOVED), "10,000", "Maximum number of cells to sweep at once. Similar to ``sweepBatchSize`` but provides finer control if the row widths vary greatly."
   ``sweepReadLimit``, ``--read-limit``, "1,000,000", "Target number of (cell, timestamp) pairs to examine in a single run."
   ``sweepCandidateBatchHint``, ``--candidate-batch-hint``, "100", "Approximate number of candidate (cell, timestamp) pairs to load at once. Decrease this if sweep fails to complete (for example if the sweep job or the underlying KVS runs out of memory). Increasing it may improve sweep performance."
   ``sweepDeleteBatchHint``, ``--delete-batch-hint``, "1,000", "Target number of (cell, timestamp) pairs to delete in a single batch."
   ``sweepPauseMillis``, ``--sleep``, "5000 ms", "Wait time between row batches. Set this if you want to use less shared DB resources, for example if you run sweep during user-facing hours."
   "``timestampsGetterBatchSize`` (Cassandra KVS only, see :ref:`Cassandra KVS config <cassandra-configuration>`)", "Only specified in config", "Fetch all columns", "Specify a limit on the maximum number of columns to fetch in a single database query. Set this to a number fewer than your number of columns if your Cassandra OOMs when attempting to run sweep with even a small row batch size. This parameter should be used when tuning Sweep for cells with many historical versions."

Following is more information about when each of the batching parameters is useful.
In short, the recommendation is:

- Decrease ``sweepCellBatchSize`` and ``sweepBatchSize`` if there is memory pressure on the client.
- Decrease ``timestampsGetterBatchSize`` if there is memory pressure on Cassandra even after setting ``sweepBatchSize`` down to 1.

Memory pressure on AtlasDB client: decrease sweepCellBatchSize and sweepBatchSize
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Since the number of columns per row can vary widely between tables, only setting a row batch size can lead to sweep batches containing different numbers of cells; setting ``sweepCellBatchSize`` can even this out.
For example, consider a database that has one table with 1000 rows and 10 columns, a second table with 1000 rows and 100 columns, and that performs best when sweeping at most 10,000 cells at a time.
In this setup, limiting ``sweepBatchSize`` to 100 would ensure that at most 10,000 cells are swept at a time, but would split the first table in 10 batches when only 1 was necessary.
Instead, setting ``sweepCellBatchSize`` to 10,000 would allow both tables to be swept in optimal batches.
In the future, we plan to deprecate ``sweepBatchSize`` in preference for ``sweepCellBatchSize``.

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
    sweep/sweep-cli
