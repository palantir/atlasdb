.. _atlas-sweep-cli:

AtlasDB Sweep Cli
=================

If you ever need to force a particular table or namespace to be swept immediately, you can run the CLI ``./bin/atlasdb sweep``, which has the following arguments:

.. csv-table::
   :header: "Short option", "Long option", "Description"
   :widths: 20, 40, 200

   ``-a``, ``--all``, "Sweep all tables, in all AtlasDB namespaces."
   ``-n``, ``--namespace <namespace name>``, "A namespace name to sweep, for instance ``-n product``"
   ``-t``, ``--table <table name>``, "A fully qualified table name to sweep. For example, to sweep the accounts table in the bank namespace, you would use ``-t bank.accounts``."
   ``-r``, ``--row <row name>``, "A row name encoded in hexadecimal to start sweeping from. The CLI prints out row names as it runs, so you can use this to easily resume a manual sweep job without unnecessarily processing rows that have already been recently swept. If this option is omitted, sweeping will process all rows of the table."

You must specify exactly one of ``-a``, ``-n``, and ``-t``. If you are sweeping a specific table with `-t`, you may additionally specify the start row with ``-r``. This is useful for resuming failed jobs.

Configuration options
---------------------

The following options are set as part of your :ref:`AtlasDB configuration <atlas_config>`.

- ``sweepBatchSize``
  If you have extremely large rows, you may want to set a lower batch size. If you have extremely small rows, you may want to up the batch size. (Default: 1000)

- ``sweepCellBatchSize``
  Provides finer control of the maximum number of cells to sweep at a time, independent of how large rows are. (Default: 10,000)

- ``sweepPauseMillis``
  If you want to use less shared DB resources during user-facing hours, you can specify a wait time in between batches. (Default: 0 ms)

- ``keyValueService/timestampsGetterBatchSize``
  (EXPERIMENTAL - Cassandra KVS only): For really, really large rows, you can set a batch size for the number of columns to fetch in a single database query. See :ref:`Cassandra KVS config <cassandra-configuration>`. (absent by default, meaning fetch all columns per row)

Be aware that manual sweeping will ignore all conditions that factor into determining whether background sweepers should run, and that the background sweeper will also be affected by system property changes.

