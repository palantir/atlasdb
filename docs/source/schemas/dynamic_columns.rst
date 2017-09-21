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
                    columnComponent("taskSize", ValueType.VAR_LONG);
                    columnComponent("monetaryCost", ValueType.VAR_LONG);
                    value(ValueType.STRING);
            }});

Note that dynamic column components must be primitive ``ValueType``s which support partitioning and ordering.
Also, key-value pairs in an individual row will be returned in *lexicographically* sorted order of the key.

Dynamic Columns are useful for avoiding KVS-level range scans, especially in key-value services where range scans
are expensive (like Cassandra - its caching optimisations are rendered ineffectual for range scans). For example,
for the schema defined above:

1. "Find John's smallest and cheapest todo" can be answered very efficiently.
   Elements are stored in sorted order, so the very first key-value pair as stored in the database is actually the
   correct answer to this query.
2. "Find all of Tom's todos with size up to M, limited to N results" is easy, as this can be
   processed with a natural stopping point, as in query 1, and we are iterating in the right order for this query.
3. "Find all of Tom's todos with size of 10 to 15, limit to N results" can be performed via a range
   scan on the dynamic column *values*. This is also readily supported.
4. "Find all of Tom's largest todos" is somewhat more difficult, as reverse range scans aren't
   supported in the dynamic columns API.
5. "Find all of Tom's todos which cost less than 5" cannot be answered efficiently; we need to
   retrieve the entire row to do this, because there is no sorting by sub-units.
6. "Find all of Tom's todos which have size between 3 and 7, and cost between 5 and 10". This is achievable, though in
   addition to the values we want to see, we also need to scan through all values with size 4-6, and those with size
   3 and cost >10, or 7 and cost <5.
7. "Find all of John's and Jeremy's smallest todos" is easy for a similar reason to 1. We provide an API that allows
   for this to be done seamlessly as well.
8. "Find the highest priority todos for everyone whose name begins with J" is NOT easy. While this
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