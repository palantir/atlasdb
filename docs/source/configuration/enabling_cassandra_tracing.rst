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

To enable tracing you need to set the key ``tracing.enabled`` to ``true``
in the Cassandra KVS runtime config block.

When a query is traced, you will see the following line your log file:
``Traced a call to <table-name> that took <duration> ms. It will appear in system_traces with UUID=<session-id-of-trace>``

.. warning::

   Trace logging is meant to be a temporary tool for performance analysis.
   Leaving tracing enabled on a production instance is not recommended due to performance implications.
   Ensure you disable tracing after your performance profiling is completed.

Example Cassandra Config with Tracing Enabled
=============================================

Consult the documentation for your service for where the Cassandra runtime
config is located. Assuming the Atlas runtime config is located at
``conf.runtime.atlas``, you can control tracing with the following config:

.. code:: yaml

   conf:
     runtime:
       atlas:
         keyValueService:
           type: cassandra
           tracing:
             enabled: true
             trace-probability: 1.0
             min-duration-to-log: "0ms"
             tables-to-trace:
               - "_transactions2"
               - "namespaceOne.table_7"
               - "namespaceTwo.table_3"

Tracing config paramters:

``tracing.enabled`` - enables tracing. This will cause a significant performace
hit while enabled. Ensure you disable tracing after your performance profiling
is completed.

``tracing.trace-probability`` - the probability to trace an eligible query.
This is a pre-filter and a good tool to use to ensure you're not tracing
frequently enough to incur performance degradation.

``tracing.tables-to-trace`` - a list of tables whose queries are eligible for
tracing. For namespaced tables the table entry must be ``<namespace>.<table>``.
Like ``trace-probability``, this is also a pre-filter. If the list is empty,
all tables are eligible.

``tracing.min-duration-to-log`` - the minimum amount of time a traced query
has to take to actually be logged. This is a post-filter and so the trace is
still done (and thus still incurs a performance hit) even if you do not log it.


Understanding Namespaced Tables in Cassandra
============================================
In Cassandra, an AtlasDB namespaced table will look like ``<keyspace>.<namespace>__<table-name>``.
As described above, to trace said table you would want to write it in the form
of ``<namespace>.<table-name>`` in the ``tables-to-trace`` list.
