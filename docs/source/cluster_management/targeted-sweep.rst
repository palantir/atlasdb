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
   In this case, running standard sweep may be necessary, to delete any stale versions that targeted sweep may have missed.

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
