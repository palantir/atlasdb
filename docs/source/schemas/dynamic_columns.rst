===============
Dynamic Columns
===============

.. note::

    This document discusses AtlasDB's dynamic columns. This is a separate concept from Cassandra's dynamic columns;
    in fact, AtlasDB's schemas in Cassandra always use two Cassandra dynamic columns, independent of whether
    we are storing a fixed-columns or dynamic columns table! Throughout this document, where "dynamic columns" is
    used unqualified, we refer to AtlasDB dynamic columns.

Overview
--------

AtlasDB offers support for *dynamic columns* - in tables where these are enabled, rows that can have an arbitrary number
of columns, each with a (typed) column name. A table with dynamic columns may be reasoned about as a
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

Note that dynamic column components must be primitive ValueTypes which support partitioning and ordering.
Also, key-value pairs in an individual row will be returned in *lexicographically* sorted order of the key.

Dynamic Columns are useful for avoiding KVS-level range scans, especially in key-value services where range scans
are expensive (like Cassandra - its caching optimisations are rendered ineffectual for range scans). For example,
for the schema defined above:

1. "Find John's smallest and cheapest todo" can be answered very efficiently.
   Elements are stored in sorted order, so the very first key-value pair retrieved from the database is actually the
   correct answer to this query.
2. "Find all of Tom's todos with size up to M, limited to N results" is easy, as this can be
   processed with a natural stopping point, as in query 1, and we are iterating in the right order for this query.
   This can be achieved using the getRowsColumnRange API.
3. "Find all of Tom's todos with size of 10 to 15, limit to N results" can be performed via a range
   scan on the dynamic column *values*. This is also readily supported.
4. "Find all of Tom's largest todos" is somewhat more difficult, as reverse range scans aren't supported by the
   dynamic columns API. If we were more interested in largest todos as opposed to smallest, we could set the
   ``ValueByteOrder`` of the ``taskSize`` column component to decreasing.
5. "Find all of Tom's todos which cost less than 5" will not be efficient; we need to retrieve the entire row to do
   this, because there is no sorting by sub-units.
6. "Find all of Tom's todos which have size between 3 and 7, and cost between 5 and 10". This is achievable, though in
   addition to the values we want to see, we also need to scan through all values with size 4-6, and those with size
   3 and cost >10, or 7 and cost <5.
7. "Find all of John's and Jeremy's smallest todos" is easy for a similar reason to 1. We provide an API that allows
   for this to be done seamlessly as well.
8. "Find the highest priority todos for everyone whose name begins with J" is NOT easy. While this
   conceivably can be split into two parts (a prefix scan for J, and then the aforementioned getRowsColumnRange),
   the range scan for J is potentially costly, as it needs to iterate through every todo for people whose names
   do indeed begin with J.

Using Dynamic Columns
---------------------

Storage
=======

One can use the standard ``put`` method. This can be used to add one or more columns to a row.

.. code:: java

    TodoTable todoTable = TodoSchemaTableFactory.of().getTodoTable(tx);

    todoTable.put(
            TodoTable.TodoRow.of(person),
            TodoTable.TodoColumnValue.of(
                    TodoTable.TodoColumn.of(size1, cost1), description1),
                    TodoTable.TodoColumn.of(size2, cost2), description2));

Note that if the table already consists of a value for a given row-key and dynamic column values, then this put
will overwrite the existing value.

Retrieval
=========

One can use the standard ``getRowsMultimap`` method to retrieve a multimap of rows to column values. However, note
that this method will call the underlying transaction's ``getRows`` method, which eagerly loads the entire row into
memory!

Alternatively, one can use ``getRowsColumnRange``. This takes a collection of rows and key ranges, and returns a map of
row-keys to BatchingVisitables of column values. These may in turn be traversed using the BatchingVisitable
API. For example, the code below is an implementation of query 1 for an arbitrary String ``person``:

.. code:: java

    TodoTable todoTable = TodoSchemaTableFactory.of().getTodoTable(tx);
    Map<TodoTable.TodoRow, BatchingVisitable<TodoTable.TodoColumnValue>> results =
            todoTable.getRowsColumnRange(
                    ImmutableList.of(TodoTable.TodoRow.of(person)),
                    BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1));

    if (!results.isEmpty()) {
        AtomicReference<Todo> element = new AtomicReference<>();
        BatchingVisitable<TodoTable.TodoColumnValue> visitable = Iterables.getOnlyElement(results.values());
        visitable.batchAccept(1, item -> {
            element.set(ImmutableTodo.of(item.get(0).getValue()));
            return false; // Do not want any more, since we know our first batch contains the one.
        });
        return element.get();
    }
    return null;


Data Representation (Cassandra)
-------------------------------

A row with dynamic columns is stored as a single Cassandra-level row. Recall the Atlas schema:

.. code:: java

    CREATE TABLE atlasete.default__todo (
        key blob,
        column1 blob,
        column2 bigint,
        value blob,
        PRIMARY KEY (key, column1, column2)
    )

``key`` consists of the (encoded) row key; ``column1`` refers to the (encoded) column components, and ``column2``
refers to the Atlas timestamp at which a write occurred. The clustering order of the table follows the
``ValueByteOrder`` of the relevant column components.

Conversely, observe that in a conventional (fixed columns) AtlasDB table, ``column1`` identifies which column is being
encoded in the ``value``.

When Not To Use Dynamic Columns
-------------------------------

Dynamic columns create wide rows in Cassandra, because they generate many (Cassandra) dynamic columns.
This may add limits to the scalability of the data, because all data for a single row key will be stored on a
single machine in the cluster.
