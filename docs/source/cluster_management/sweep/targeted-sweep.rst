.. _targeted-sweep:

Targeted Sweep
==============

.. warning::

   Targeted sweep is still under active development and some of its functionality is not implemented yet.
   Consult with the AtlasDB team if you wish to use targeted sweep in addition to, or instead of, standard sweep.

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

Targeted sweep uses a combination of :ref:`install and runtime AtlasDB configurations <atlas-config>`, as follows:

.. csv-table::
   :header: "AtlasDB Install Config", "Default", "Description"
   :widths: 80, 40, 200

   ``enableSweepQueueWrites``, "false", "Whether information about writes should be persisted to the sweep queue. If set to false, the targeted sweep runtime configurations will be ignored."
   ``conservativeThreads``, "1", "Number of threads to use for targeted sweep of tables with sweep strategy conservative. Maximum supported value is 256."
   ``thoroughThreads``, "1", "Number of threads to use for targeted sweep of tables with sweep strategy thorough. Maximum supported value is 256."

.. csv-table::
   :header: "AtlasDB Runtime Config", "Default", "Description"
   :widths: 80, 40, 200

   ``enabled``, "false", "Whether targeted sweep should be run by background threads. Note that enableSweepQueueWrites must be set to true before targeted sweep can be run."
   ``shards``, "1", "Number of shards to use for persisting information to the sweep queue, enabling better parallelization of targeted sweep. The number of shards should be greater than or equal to the number of threads used for background targeted sweep. Note that this number must be monotonically increasing, and attempts to lower may be ignored. Maximum supported value is 256."

Changing Sweep Strategy for a Table
-----------------------------------

.. danger::

   Consult with the AtlasDB team before changing the sweep strategy of a table. Doing this incorrectly can invalidate
   AtlasDB's correctness guarantees.

Whenever targeted sweep enqueues a write into the sweep queue, it does so using the latest known sweep strategy for the
table. If the sweep strategy for that table later changes, **targeted sweep does not recheck the strategy**, and will
therefore sweep those entries using the old strategy.

In a multinode setting, changing the sweep strategy for a table using a rolling upgrade strategy can cause some nodes to
run read-only transactions on tables that are swept using the THOROUGH strategy by other nodes. Even with a shutdown
upgrade, if the new strategy is CONSERVATIVE, we may still have old entries in the thorough sweep queue, and therefore
sweep those cells using the THOROUGH strategy. Both of these cases have correctness implications for read-only
transactions.

The safe way to change the sweep strategy from THOROUGH to CONSERVATIVE:
  1. Shut down all the nodes.
  2. Start AtlasDB with the new table metadata, but **do not use read-only transactions on the table yet**.
  3. Wait until targeted sweep for strategy THOROUGH has caught up to after the upgrade. This can be verified by
     consulting the ``millisSinceLastSweptTs`` targeted sweep metric.
  4. We are now guaranteed to perform no more thorough sweeps on the table and can run read-only transactions.

The safe way to change sweep strategy from CONSERVATIVE to THOROUGH:
   1. Shut down all the nodes.
   2. Start AtlasDB with the new table metadata.