.. _sweep:

AtlasDB Sweep
=============

.. warning::

   If you're using sweep through either a) the deprecated Sweep CLI or b) the background sweeper on AtlasDB < 0.32.0, it can cause **SEVERE DATA CORRUPTION** to any ongoing backups.

Under normal usage, the data written to the key value service backing AtlasDB is never updated or deleted.
When a row in AtlasDB is "updated", a new version of the row with a higher timestamp is written.
The old row is left untouched. When that row is later read, only the version with the higher timestamp is returned.
Deletions in AtlasDB are similar; in practice they are the same as updates with an empty value.

To prevent an AtlasDB database from growing in size indefinitely, old versions of rows can be deleted through a process called sweeping.

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

- :ref:`Standard sweep<standard-sweep>`:
  Standard sweep scans a table to find rows with multiple versions, then deletes the older versions from the database.

- :ref:`Targeted sweep<targeted-sweep>`:
  Targeted sweep persists the information about each transactional write as it is performed, and then uses this information to delete all older versions of each of the cells using a single ranged tombstone.

.. toctree::
    :maxdepth: 1
    :hidden:

    standard-sweep
    targeted-sweep
