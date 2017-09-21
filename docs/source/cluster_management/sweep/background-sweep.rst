.. _background-sweep:

Background Sweep
================

How Background Sweep Works
--------------------------

The Background Sweep Job works by sweeping one table at a time.
The Background Sweep Job determines which table to sweep by estimating which would be most beneficial based on I/O activity and frequency, considering the following criteria:

- The number of cells written to a table since it was last swept.
- The number of cells that were deleted the last time a table was swept.
- The number of cells that were not deleted the last time a table was swept.
- The amount of time that has passed since the it was last swept.

Configuration
-------------

The background sweeper can be disabled by setting the ``enabled`` property in the `Sweep Config` block in the :ref:`AtlasDB Runtime Config<atlas-config>` block to false.

Metrics
-------

We now expose Dropwizard metrics to allow easier tracking of the background sweeper's actions.
For more information, see :ref:`Dropwizard Metrics<dropwizard-metrics>`.

Additional logging for Background Sweep
---------------------------------------

By default, the background sweeper only logs errors. If you'd like to watch the background sweeper's progress, add the following in ``atlasdb.log.properties``:

  ::

    #--------------------------------------------------------------------------------
    # Sweep Logging
    #--------------------------------------------------------------------------------

    # enable background sweep logging by setting this to 'debug'; for less verbose logging, use 'error'.
    log4j.logger.com.palantir.atlasdb.sweep.BackgroundSweeperImpl=debug, sweepAppend
    log4j.logger.com.palantir.atlasdb.sweep.SpecificTableSweeper=debug, sweepAppend
    log4j.logger.com.palantir.atlasdb.sweep.SweepTaskRunner=debug, sweepAppend
    log4j.logger.com.palantir.atlasdb.sweep.CellSweeper=debug, sweepAppend

    # set additivity to false to make these logs only show up in background-sweeper.log
    log4j.additivity.com.palantir.atlasdb.sweep.BackgroundSweeperImpl=false

    # configure a basic file appender
    log4j.appender.sweepAppend.layout=com.palantir.monitoring.logging.log4j.PalantirPatternLayout
    log4j.appender.sweepAppend.layout.ConversionPattern=%m%n
    log4j.appender.sweepAppend=com.palantir.util.logging.ArchivedDailyRollingFileAppender
    log4j.appender.sweepAppend.threshold=debug
    log4j.appender.sweepAppend.file=log/background-sweeper.log
    log4j.appender.sweepAppend.datePattern='.'yyyy-MM-dd
    log4j.appender.sweepAppend.MaxRollFileCount=90

This will create a log file ``log/background-sweeper.log`` where sweep information will be logged.

Querying the Sweep Metadata Tables
----------------------------------

You can also query the Atlas table ``sweep.progress`` using Atlas Console.
``sweep.progress`` contains at most a single row detailing information for the current table the background sweeper is sweeping.
You can also query ``sweep.priority`` to get a breakdown per table of:

- ``write_count`` - Approximate number of writes to this table since the last time it was swept.

- ``last_sweep_time`` - Wall clock time the last time this table was swept.

- ``cells_deleted`` - The numbers of stale values deleted last time this table was swept.

- ``cells_examined`` - The number of cell-timestamp pairs in the table total the last time this table was swept.

.. note::

   If one investigates the ``sweep.priority`` table, one *may* find information regarding the number of cell-timestamp
   pairs examined or deleted on earlier runs as well. However, the ``sweep.priority`` table itself can be swept, thus
   the table *may* not contain information concerning all historical runs of sweep.
