===============
Dynamic Columns
===============

Overview
--------

AtlasDB offers support for *dynamic columns* - these are rows that can have an arbitrary number of columns, each with
a (typed) column name. A table with dynamic columns may be reasoned about as a
``Map<RowKey, SortedMap<Tuple<Col1, Col2... ColN>, Value>>``. For example, a simple schema using dynamic columns may be
defined as follows:

.. code:: java

    schema.addTableDefinition("todo", new TableDefinition() {{
                rowName();
                    rowComponent("person", ValueType.STRING);
                dynamicColumns();
                    columnComponent("priority", ValueType.VAR_LONG);
                    columnComponent("subunit", ValueType.VAR_LONG);
                    value(ValueType.STRING);
            }});

Note that dynamic column components must be primitive ``ValueType``s which support partitioning and ordering.
Also, key-value pairs in an individual row will be returned in *lexicographically* sorted order of the key.

Dynamic Columns are useful for avoiding KVS-level range scans, especially in key-value services where range scans
are expensive (like Cassandra - its caching optimisations are rendered ineffectual for range scans). For example,
for the schema defined above:

1. "Find descriptions of all of John's highest priority todos (smallest number)" can be answered reasonably
   efficiently. Elements with priority 0 will be stored before those with priority 1, before those with priority 2,
   etc., so once we come across an element with non-maximum priority, we do not need to process any more batches.
2. "Find descriptions of all of Tom's todos with priority less than M, limit to N results" is easy, as this can be
   processed with a natural stopping point, as in query 1, and we are iterating in the right order for this query.
3. "Find descriptions of all of Tom's todos with priority P2, limit to N results" can be performed via a range scan
   on the dynamic column *values*. This is also readily supported.
4. "Find descriptions of all of Tom's lowest priority todos" is somewhat more difficult, as reverse range scans aren't
   supported in the dynamic columns API.
5. "Find descriptions of all of Tom's todos with the smallest sub-unit" cannot be answered efficiently; we need to
   retrieve the entire row to do this, because there is no sorting by sub-units.
6. "Find descriptions of all of John's and Jeremy's highest priority todos" is easy for a similar reason to 1, and
   we even provide an API, ``getRowsColumnRange``, to do this efficiently.
7. "Find descriptions of the highest priority todos for everyone whose name begins with J" is NOT easy. While this
   conceivably can be split into two parts (a range scan for J, and then the aforementioned getRowsColumnRange),
   the range scan for J is potentially costly, as it needs to iterate through every todo for people whose names
   do indeed begin with J.

Using Dynamic Columns
---------------------

Storage
=======

TODO jkong

Retrieval
=========

One can use the standard ``getRowsMultimap`` method to retrieve a multimap of rows to column values. However, note
that this method will call the underlying transaction's ``getRows`` method, which eagerly loads the entire row into
memory!

Alternatively, one can use ``getRowsColumnRange``. This takes a collection of rows and key ranges, and returns a map of
row-keys to ``BatchingVisitable``s of column values. These may in turn be traversed

TODO

Data Representation (Cassandra)
-------------------------------

A row with dynamic columns is stored as a single Cassandra-level row.

.. code:: java

    CREATE TABLE atlasete.default__todo (
        key blob,
        column1 blob,
        column2 bigint,
        value blob,
        PRIMARY KEY (key, column1, column2)
    )

``key`` consists of the (encoded) row key; ``column1`` refers to the (encoded) column components.
The clustering order of the table follows the ``ValueByteOrder`` of the relevant column components.