.. _enabling-cassandra-tracing:

==========================
Enabling Cassandra Tracing
==========================

Overview
========

Sometimes in order to do deeper performance analysis of queries hitting
your Cassandra cluster, you'll want to enable tracing.  To learn more
about tracing itself check out DataStax's `Request tracing in Cassandra
1.2 <http://www.datastax.com/dev/blog/tracing-in-cassandra-1-2>`__ and `TRACING <https://docs.datastax.com/en/cql/3.3/cql/cql_reference/tracing_r.html>`__.

To enable tracing you need to provide a configuration file called
``atlas_tracing.prefs`` and place it in the ``user.dir`` directory of your
Java process, which is the directory from which java was executed.

When a query is traced, you will see the following line your log file:
``Traced a call to <table-name> that took <duration> ms. It will appear in system_traces with UUID=<session-id-of-trace>``

.. warning::

   Trace logging is meant to be a temporary tool for performance analysis.
   Leaving tracing enabled on a production instance is not recommended due to performance implications.
   Ensure you disable tracing after your performance profiling is completed.

The Prefs File
==============

The ``atlas_tracing.prefs`` is a standard java properties file with
the following parameters:

.. code:: properties

    # enables tracing
    # this will cause a significant performance hit while enabled
    # ensure you disable tracing after your performance profiling is completed
    tracing_enabled: true

    # the probability we trace an eligible query
    # this is a pre-filter and a good tool to use to ensure you're not tracing
    # frequently enough to incur performance degradation
    trace_probability: 1.0

    # a comma-separated list of tables whose queries are eligible for tracing
    # for namespaced tables the table entry must be <namespace>.<table>
    # like "trace_probability", this is also a pre-filter
    tables_to_trace: _transactions,namespaceOne.table_7,namespaceTwo.table_3

    # the minimum amount of time a traced query has to take to actually be logged
    # this is a post-filter and so the trace is still done (and thus still incurs
    # a performance hit) even if you do not log it
    min_duration_to_log_ms: 1000

Understanding Namespaced Tables in Cassandra
============================================
In Cassandra, an AtlasDB namespaced table will look like ``<keyspace>.<namespace>__<table-name>``.
As described in the prefs file's comment, to trace said table you would want to
write it in the form of ``<namespace>.<table-name>`` in the comma-separated list.
