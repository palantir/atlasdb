.. _atlasdb-sweep-endpoints:

AtlasDB Sweep Endpoints
=======================

If you ever need to force a particular table to be swept immediately, you can run sweep by hitting any of the following endpoints from the service API.

 +------------------------------------------+-------------------------------------------------+--------------------------------------------------------------------------------------------------+
 | Endpoint                                 | Query Parameters                                | Description                                                                                      |
 +==========================================+=================================================+==================================================================================================+
 |``/sweep/sweep-table``                    | ``tablename``: fully qualified table name       | Sweep a particular table from EMPTY startRow with default ``SweepBatchConfig``.                  |
 +------------------------------------------+-------------------------------------------------+--------------------------------------------------------------------------------------------------+
 |``/sweep/sweep-table-from-row``           | ``tablename``: fully qualified table name       | Sweep a particular table from specified startRow with default ``SweepBatchConfig``.              |
 +                                          +-------------------------------------------------+                                                                                                  |
 |                                          | ``startRow``: base16 encoded start row          |                                                                                                  |
 +------------------------------------------+-------------------------------------------------+--------------------------------------------------------------------------------------------------+
 |``/sweep/sweep-table-from-row-with-batch``| ``tablename``: fully qualified table name       | Sweep a particular table from specified startRow with specified ``SweepBatchConfig`` parameters. |
 +                                          +-------------------------------------------------+                                                                                                  |
 |                                          | ``startRow``: base16 encoded start row          |                                                                                                  |
 +                                          +-------------------------------------------------+                                                                                                  |
 |                                          | ``maxCellTsPairsToExamine``: cells to examine   |                                                                                                  |
 +                                          +-------------------------------------------------+                                                                                                  |
 |                                          | ``candidateBatchSize``: cells to read per batch |                                                                                                  |
 +                                          +-------------------------------------------------+                                                                                                  |
 |                                          | ``deleteBatchSize``: cells to delete per batch  |                                                                                                  |
 +------------------------------------------+-------------------------------------------------+--------------------------------------------------------------------------------------------------+

Note that using the sweep endpoints to manually kick off sweep will *not* update the Background Sweeper's sweep priority table.
