.. _sweep:

AtlasDB Sweep Job
=================

.. warning::

   AtlasDB sweep is currently an experimental feature, and is disabled by default. If you are interested in testing it, please contact the AtlasDB team.

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


.. _sweep_tunable_parameters:

Tunable Configuration Options
-----------------------------

The following optional parameters can be tuned to optimize Sweep performance for a specific AtlasDB instance.
You may set them as part of your :ref:`AtlasDB configuration <atlas_config>`, or as a CLI option if you are running sweep using the CLI.

.. csv-table::
   :header: "AtlasDB Config", "CLI Option", "Default", "Description"
   :widths: 20, 20, 40, 200

   ``sweepBatchSize``, ``--batch-size``, "1,000", "Maximum number of rows to sweep at once. Decrease this if sweep fails to complete (for example if the sweep job or the underlying KVS runs out of memory). Increasing it may improve sweep performance."
   ``sweepCellBatchSize``, ``--cell-batch-size``, "10,000", "Maximum number of cells to sweep at once. Similar to ``maxBatchSize`` but provides finer control if the row widths vary greatly."
   ``sweepPauseMillis``, ``--sleep``, "0 ms", "Wait time between row batches. Set this if you want to use less shared DB resources, for example if you run sweep during user-facing hours."
   "``timestampsGetterBatchSize`` under ``keyValueService`` (see :ref:`Cassandra KVS config <cassandra-configuration>`)", "Not available, the CLI will pick up the value from the config", "Absent (fetch all columns)", "(Cassandra KVS only): Specify a limit on the maximum number of columns to fetch in a single database query. Set this if your Cassandra OOMs when attempting to run sweep with even a small row batch size."


Ways to Sweep
-------------

- :ref:`Background sweep process<background_sweep>` scheduled periodically:
  Under normal use, the sweep job is intended to run at a constant interval as a background sweep process that does not consume significant system resources.

- :ref:`Sweep CLI<atlas-sweep-cli>` triggered manually:
  You may trigger the sweep job on demand via the sweep CLI. This could be useful in order to address any one-off performance issues.
  Also, long-running AtlasDB instances with high data scale and months/years of user activity that want to enable the background sweep job
  may benefit from manually sweeping specific tables first, to reduce the number of unused cells that have accumulated over time.

.. toctree::
    :maxdepth: 1
    :hidden:

    sweep/background-sweep
    sweep/sweep-cli
