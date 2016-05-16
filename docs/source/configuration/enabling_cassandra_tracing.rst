==========================
Enabling Cassandra Tracing
==========================

Overview
========

Sometimes in order to do deeper performance analysis of queries hitting
your cassandra cluster, you'll want to enable tracing.  To learn more 
about tracing itself check out datastax's `Request tracing in Cassandra 
1.2 <http://www.datastax.com/dev/blog/tracing-in-cassandra-1-2>`__ and `TRACING <https://docs.datastax.com/en/cql/3.3/cql/cql_reference/tracing_r.html>`__.

To enable tracing you need to provide a configuration file called 
``atlasdb_tracing.prefs`` and place it in the user.dir directory of your 
java process.

When a query is traced you will see the following line your log file:
``Traced a call to <table-name> that took <duration> ms. It will appear in system_traces with UUID=<session-id-of-trace>``

The Prefs File
==============

The ``atlasdb_tracing.prefs`` is a standard java properties file with 
the following parameters:

.. code:: properties

    tracing_enabled: true           # self explanatory
    trace_probability: 1.0          # the probability we trace an eligible query
    min_duration_to_log_ms: 1000    # the minimum amount of time a traced query has to take to actually be logged

    # a comma separated list of tables whose queries are eligible for tracing
    # for namespaced tables the table entry must be <namespace>.<table>
    tables_to_trace: _transactions,namespaceOne.table_7,namespaceTwo.table_3    

