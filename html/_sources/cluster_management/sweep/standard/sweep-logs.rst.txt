.. _sweep-logs:

Background Sweep Inner Workings and Logs
========================================

At a high level, the background sweep thread works as follows:

1. Select a table to sweep;
2. Read a batch of values from the table;
3. Determine which values from the batch can be deleted;
4. Delete the sweepable values from the table;
5. If the table still has values, go to step 2.
6. If the table has no new values, go to step 1.

In order to know if sweep is working, or its current progress, look for the following:

- ``Starting background sweeper.``

Logged when the service has started, to indicate that background sweeper thread is running.

- ``Beginning iteration of sweep for table {} starting at row {}``

Logged before we begin an iteration of sweep, after 1 or 5. (note that this could be triggered by background sweep, the sweep CLI, or the ``SweeperService`` endpoints)

- ``Analyzed {number of values read} cell+timestamp pairs from table {table name} ...``

Logged when a batch of values has been successfully swept, after 4. Its parameters also contain the number of
(cell + ts) pairs read and deleted on this batch.

- ``Finished sweeping table {table name}. ...``

Logged when a table has been successfully swept, after 6. Its parameters also contain the number of (cell + ts) pairs
read and deleted when sweeping this table.
