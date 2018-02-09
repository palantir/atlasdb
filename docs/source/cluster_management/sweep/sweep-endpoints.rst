.. _atlasdb-sweep-endpoints:

AtlasDB Sweep Endpoints
=======================

If you ever need to force a particular table to be swept immediately, you can run sweep by making a POST request to the
``/sweep/sweep-table`` endpoint. Sweep runs initiated via the endpoint are configurable via standard HTTP query
parameters.

.. list-table::
   :header-rows: 1

   * - Parameter Name
     - Description
     - Required/Optional
   * - ``tablename``
     - Fully qualified table name (inclusive of namespace; e.g. ``namespace.actualTableName``)
     - Required
   * - ``startRow``
     - Base16 encoded start row.
     - Optional (default: empty - i.e. first row of table)
   * - ``fullSweep``
     - Whether to sweep a full table. If set to ``false``, we will sweep just one batch.
     - Optional (default: true)
   * - ``maxCellTsPairsToExamine``
     - Total number of cell-timestamp pairs to examine before a batch completes. Note that Sweep may actually read more
       than this value if that is required to complete reading all cell-timestamp pairs in a given row.
     - Optional (default: 128)*
   * - ``candidateBatchSize``
     - Number of cell-timestamp pairs to query the key value service for at a time.
     - Optional (default: 128)*
   * - ``deleteBatchSize``
     - Number of cell-timestamp pairs to delete from the key-value service at a time. Note that Sweep may attempt
       slightly larger deletes than this value, but it is guaranteed that it does not exceed twice this.
     - Optional (default: 128)*

\* Note the the sweep config might be lower if previous sweep runs have failed.

.. warning::

    If using curl from a shell to hit the sweep endpoint, please remember to escape ampersands and/or quote the
    URL provided to curl; otherwise, the ampersand will end the command and run curl in the background with the first
    parameter provided, disregarding all other parameters.

    A correct query could look like this. If the URL was not quoted, this would sweep the table from the beginning.

    .. code::

        $ curl -k -XPOST 'https://palantir:9999/sweep/sweep-table?tablename=foo.bar&startRow=3141592654'

Using the sweep endpoints to manually kick off sweep will *not* update the Background Sweeper's sweep priority table.
This will publish some metrics (such as the number of cell-timestamp pairs examined and deleted), but not all
(in particular, sweep outcome metrics are not published).
