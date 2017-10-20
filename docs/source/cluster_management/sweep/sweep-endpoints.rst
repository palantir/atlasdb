.. _atlasdb-sweep-endpoints:

AtlasDB Sweep Endpoints
=======================

If you ever need to force a particular table to be swept immediately, you can run sweep by hitting any of the following endpoints from the service API.

 +-----------------------+-------------------------------------------------+---------------------------+---------------------------------------------------------------------------------+
 | Endpoint              | Query Parameters                                | Required/Optional         | Description                                                                	 |
 +=======================+=================================================+===========================+=================================================================================+
 |``/sweep/sweep-table`` | ``tablename``: fully qualified table name       | Required                  |Sweep a particular table from EMPTY startRow with default ``SweepBatchConfig``.  |
 +                       +-------------------------------------------------+---------------------------+                                                                                 |
 |                       | ``startRow``: base16 encoded start row          | Optional                  |                                                                                 |
 +                       +-------------------------------------------------+---------------------------+                                                                                 |
 |                       | ``fullSweep``: sweep full table or single batch | Optional(default: true)   |                                                                                 |
 |                       +-------------------------------------------------+---------------------------+                                                                                 |
 +                       | ``maxCellTsPairsToExamine``: cells to examine   | Optional                  |                                                                                 |
 |                       +-------------------------------------------------+---------------------------+                                                                                 |
 +                       | ``candidateBatchSize``: cells to read per batch | Optional                  |                                                                                 |
 |                       +-------------------------------------------------+---------------------------+                                                                                 |
 +                       | ``deleteBatchSize``: cells to delete per batch  | Optional                  |                                                                                 |
 +-----------------------+-------------------------------------------------+---------------------------+---------------------------------------------------------------------------------+

Note that using the sweep endpoints to manually kick off sweep will *not* update the Background Sweeper's sweep priority table.
