.. _targeted-sweep:

Targeted Sweep
==============

Targeted sweep persists the information about each transactional write as it is performed, and then uses this information to delete all older versions of each of the cells using a single ranged tombstone.

Persisting to the Sweep Queue
-----------------------------

As long as ``enableSweepQueueWrites`` is enabled, whenever a transaction is about to commit, the necessary information for deleting stale versions of cells is persisted to the sweep queue.

Running Background Targeted Sweep
---------------------------------

Background targeted sweep can be enabled in the live reloadable runtime targeted sweep configuration.
As long as targeted sweep is enabled, background threads will read the information persisted in the sweep queue, and will use ranged tombstones to delete any stale versions of the cells that have been written to.

.. warning::

   While sweep queue writes are disabled, nothing will be persisted to the sweep queue.
   This can result in targeted sweep being unable to delete older versions of cells written to while sweep queue writes are disabled even if they are later enabled again.
   In this case, running standard sweep may be necessary, to delete any stale versions that targeted sweep has missed.

Configuration Options
---------------------

Targeted sweep uses a combination of :ref:`install and runtime AtlasDB configurations <atlas-config>`.
In both cases, these configuration options are specified within a ``targetedSweep`` context.

.. csv-table::
   :header: "AtlasDB Install Config", "Default", "Description"
   :widths: 80, 40, 200

   ``enableSweepQueueWrites``, "true", "Whether information about writes should be persisted to the sweep queue. If set to false, the targeted sweep runtime configurations will be ignored."
   ``conservativeThreads``, "1", "Number of threads to use for targeted sweep of tables with sweep strategy conservative. Maximum supported value is 256."
   ``thoroughThreads``, "1", "Number of threads to use for targeted sweep of tables with sweep strategy thorough. Maximum supported value is 256."

.. csv-table::
   :header: "AtlasDB Runtime Config", "Default", "Description"
   :widths: 80, 40, 200

   ``enabled``, "true", "Whether targeted sweep should be run by background threads. Note that enableSweepQueueWrites must be set to true before targeted sweep can be run."
   ``shards``, "1", "Number of shards to use for persisting information to the sweep queue, enabling better parallelization of targeted sweep. The number of shards should be greater than or equal to the number of threads used for background targeted sweep. Note that this number must be monotonically increasing, and attempts to lower may be ignored. Maximum supported value is 256."

For example, to configure targeted sweep with three conservative threads, one thorough
thread (which is the default) and 8 shards, one should add the following blocks to their configuration:

.. code-block:: yaml

    atlasdb:
      targetedSweep:
        conservativeThreads: 3

    atlasdb-runtime:
      targetedSweep:
        shards: 8

Also note that threads perform targeted sweep serially within the context of a shard, so configuring more threads
in an attempt to increase parallelism will only work if the number of shards is also increased.

Changing Sweep Strategy for a Table
-----------------------------------

.. danger::

   Consult with the AtlasDB team before changing the sweep strategy of a table. Doing this incorrectly can invalidate
   AtlasDB's correctness guarantees.

.. danger::

   Throughout this section, when distinct sequential steps are indicated to roll the cluster from one version to
   another, it is imperative that each roll is complete before the next roll begins, and also that rolls that skip any
   version in the sequence are not allowed.

Whenever targeted sweep enqueues a write into the sweep queue, it does so using the latest known sweep strategy for the
table. If the sweep strategy for that table later changes, **targeted sweep does not recheck the strategy**, and will
therefore sweep those entries using the old strategy.

In a multinode setting, changing the sweep strategy for a table using a rolling upgrade strategy can cause some nodes to
run read-only transactions on tables that are swept using the THOROUGH strategy by other nodes. Even with a shutdown
upgrade, if the new strategy is CONSERVATIVE, we may still have old entries in the thorough sweep queue, and therefore
sweep those cells using the THOROUGH strategy. Both of these cases have correctness implications for read-only
transactions.

At its schema layer, AtlasDB also supports a third sweep strategy, THOROUGH_MIGRATION. While this strategy is enabled,
entries are written to the CONSERVATIVE sweep queue, but read transactions are also required to check that they still
hold the immutable timestamp lock (as if we were using the THOROUGH sweep strategy). This avoids the aforementioned
correctness implications: any read-only transactions that may ever run concurrently with targeted sweep will also check
the immutable timestamp lock.

CONSERVATIVE to THOROUGH
~~~~~~~~~~~~~~~~~~~~~~~~

.. warning::

   You should not change user transactions from read-only to read-write and the sweep strategy from conservative to
   thorough in the same release. In this case, read-only transactions on old nodes may fail during any state where
   both old and new nodes are simultaneously operating (though there are no correctness implications).

Thus, a way of changing sweep strategy from CONSERVATIVE to THOROUGH while avoiding downtime is as follows:
  1. Roll service nodes from a version which uses read-only transactions (e.g. ``runTaskReadOnly``) to one that uses
     only read-write transactions (e.g. ``runTaskReadWrite``). Both versions should still use the CONSERVATIVE sweep
     strategy.
  2. Roll service nodes from a version that uses CONSERVATIVE sweep strategy AND only uses read-write transactions to
     one with THOROUGH_MIGRATION.
  3. Roll service nodes from a version with THOROUGH_MIGRATION sweep strategy to one with THOROUGH.
     During this roll, the queue to which cell references are enqueued will vary depending on the individual node.
     However, all read transactions will check the immutable timestamp lock, so it's okay for some values to be
     THOROUGH swept.

This process may also be performed as a shutdown upgrade from CONSERVATIVE to THOROUGH (where ALL nodes are shut down
before any is restarted with the new table metadata). In this case, it is also permissible that this upgrade covers
moving away from the use of read-only transactions.

THOROUGH to CONSERVATIVE
~~~~~~~~~~~~~~~~~~~~~~~~

The process here needs to account for the existence of old entries written to the THOROUGH sweep queue.
  1. Roll service nodes from a version with THOROUGH sweep strategy to one with THOROUGH_MIGRATION.
     This is safe; see step 2 above.
  2. Wait until targeted sweep for strategy THOROUGH has caught up to after the upgrade. This can be verified by
     consulting the ``millisSinceLastSweptTs`` targeted sweep metric.
  3. Roll service nodes from a version with THOROUGH_MIGRATION sweep strategy to one with CONSERVATIVE. If desired,
     the CONSERVATIVE product version may immediately begin using read-only transactions.

This process may also be performed as a single shutdown upgrade from THOROUGH to CONSERVATIVE:
  1. Shut down all the nodes.
  2. Start AtlasDB with the new table metadata, but **do not use read-only transactions on the table yet**.
  3. Wait until targeted sweep for strategy THOROUGH has caught up to after the upgrade. This can be verified by
     consulting the ``millisSinceLastSweptTs`` targeted sweep metric.
  4. We are now guaranteed to perform no more thorough sweeps on the table and can run read-only transactions.
