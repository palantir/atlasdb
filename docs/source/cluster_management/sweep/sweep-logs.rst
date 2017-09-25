.. _sweep-logs:

Background Sweep Inner Workings and Logs
========================================

Sweep, at a high level, the background sweep process works as follows:

1. Selects a table to sweep;
2. Reads a batch of values from the table;
3. Determines which values can be swept;
4. Deletes the sweepable values from the table;
5. If the table still has values, read another batch from the table and execute step 3.
6. If the table has no new values, execute step 1.

In order to know if sweep is working, or it's current progress, look for the following:

- ``Starting background sweeper.``

Logged when the service has started, to indicate that background sweeper is running.

- ``Now starting to sweep next table: {table name}.``

Logged when a new table has been selected to be swept, after 1.

- ``Sweeping another batch of table: {}. Batch starts on row {}``

Logged when a new batch of the same table is going to be swept, after 5.

- ``Swept successfully.``

Logged when a batch of values has been successfully swept, after 4. Its parameters also contain the number of
(cell + ts) pairs analyzed and deleted on this batch.

- ``Finished sweeping``

Logged when a table has been successfully swept, after 6. Its parameters also contain the number of (cell + ts) pairs
analyzed and deleted when sweeping this table.
