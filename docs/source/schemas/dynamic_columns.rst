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
``Map<RowKey, SortedMap<Tuple<Col1, Col2... ColN>, Value>>``. It may also be thought of as a collection of key-value
pairs within a row. For example, a simple schema using dynamic columns may be defined as follows:

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

We call the person the *row key*, the (taskSize, monetaryCost) pair the *dynamic column key*, and the string (which is
intended to be a description of the todo) the *value*.

Query Capabilities
------------------

Dynamic Columns are useful for avoiding row range scans, especially in key-value services where these
are expensive (like Cassandra - its caching optimisations are rendered ineffectual for row range scans). Furthermore,
when considering whether a query without row range scans will be performant, it is worth considering how each row is
laid out in the database. Generally, queries for column ranges that are contiguous are more efficient, though there are
exceptions to this rule (such as query 4).

.. list-table::
    :header-rows: 1

    * - Dynamic Column Key
      - Dynamic Column Value
    * - (1, 3000)
      - "Buy a bitcoin"
    * - (2, 0)
      - "Review pull request"
    * - (2, 1)
      - "Get coffee"
    * - (3, 0)
      - "Write docs for dynamic columns"
    * - (3, 6)
      - "Get lunch"
    * - (5, -1)
      - "Complete online survey"
    * - (5, 0)
      - "Resolve merge conflicts"
    * - (6, 10)
      - "Take a train out of the city"
    * - (7, 2)
      - "Do laundry"
    * - (7, 7)
      - "Visit the supermarket"
    * - (7, 42)
      - "Watch a musical"

For example, for the schema defined above:

1. "Find John's smallest and cheapest todo" can be answered very efficiently.
   Elements are stored in sorted order, so the very first key-value pair retrieved from the database is actually the
   correct answer to this query.
2. "Find all of Tom's todos with size up to M, limited to N results" is easy, as this can be
   processed with a natural stopping point, as in query 1, and we are iterating in the right order for this query.
   This can be achieved using the ``getRowsColumnRange`` API.
3. "Find all of Tom's todos with size of 10 to 15, limit to N results" can be performed via a range
   scan on the dynamic column key. This is also readily supported, and does not require a row range scan.
4. "Find all of Tom's largest todos" is somewhat more difficult, as reverse range scans aren't supported by the
   dynamic columns API. If we were more interested in largest todos as opposed to smallest, we could set the
   ``ValueByteOrder`` of the ``taskSize`` column component to decreasing.
5. "Find all of Tom's todos which cost less than 5" will be less efficient. We need to retrieve the entire row to do
   this, because the map is sorted by task size and then monetary cost. If queries on cost are more common than queries
   on size, flipping the order of the column components could help.
6. "Find all of Tom's todos which have size between 3 and 7, and cost between 5 and 10". We have to visit column ranges
   in contiguous order, so if we write a single query we need to scan from (3, 5) to (7, 10 + 1). However, this range
   also includes values we are not interested in (any values with size 4-6 that don't fit in the cost range, and also
   costly size 3 tasks and cheap size 7 tasks). We can postfilter them out, but we still need to scan through them.
7. "Find all of Nathan's todos that begin with the letter N". Prefix / range scans are not supported on values, so this
   requires us to scan through the entire row.
8. "Find all of John's and Jeremy's smallest todos" is easy - this is doing the query "find a person's smallest todos"
   twice. To answer that query, we can scan through the person's tasks until we find one with a larger size than the
   others we have seen so far. At that point, we know we have seen all the smallest tasks (because it is in sorted
   order).
9. "Find the smallest todos for everyone whose name begins with J" is NOT easy. While this
   conceivably can be split into two parts (a row prefix scan for J, and then the aforementioned
   ``getRowsColumnRange``),
   the range scan for J is potentially costly, as it needs to iterate through every todo for people whose names
   do indeed begin with J.

Notice, though, that all of the queries apart from 9 can be answered without a row range scan.

Using Dynamic Columns
---------------------

Storage
=======

One can use the standard ``put`` method. This can be used to add one or more columns to a row.

.. code:: java

    TodoTable todoTable = TodoSchemaTableFactory.of().getTodoTable(tx);

    todoTable.put(
            TodoTable.TodoRow.of(person),
            TodoTable.TodoColumnValue.of(TodoTable.TodoColumn.of(size1, cost1), description1),
            TodoTable.TodoColumnValue.of(TodoTable.TodoColumn.of(size2, cost2), description2));

Note that if the table already consists of a value for a given row key and dynamic column key, then this put
will logically overwrite the existing value.

Retrieval
=========

Typically, one should use ``getRowsColumnRange``. This takes a collection of rows and column key ranges, and returns a
map of row-keys to ``BatchingVisitables`` of column values. These may in turn be traversed using the
``BatchingVisitable`` API. For example, the code below is an implementation of query 1 for an arbitrary String
``person``:

.. code:: java

    TodoTable todoTable = TodoSchemaTableFactory.of().getTodoTable(tx);
    Map<TodoTable.TodoRow, BatchingVisitable<TodoTable.TodoColumnValue>> results =
            todoTable.getRowsColumnRange(
                    ImmutableList.of(TodoTable.TodoRow.of(person)),
                    BatchColumnRangeSelection.create(
                            PtBytes.EMPTY_BYTE_ARRAY, // Empty byte arrays mean unbounded.
                            PtBytes.EMPTY_BYTE_ARRAY,
                            1)); // The batch hint is 1, because we are only interested in the first result.

    AtomicReference<Todo> element = new AtomicReference<>();
    BatchingVisitable<TodoTable.TodoColumnValue> visitable = Iterables.getOnlyElement(results.values());
    visitable.batchAccept(
            1, // We can consider batches of 1 element at a time, because we are only interested in the first element.
            item -> {
                element.set(ImmutableTodo.of(item.get(0).getValue()));
                return false; // Do not want any more, since we know our first batch contains the one.
            });
    return element.get();

.. note::

    One can also use the ``getRowsMultimap`` method to retrieve a multimap of rows to column values. However, note
    that this method will call the underlying transaction's ``getRows`` method, which eagerly loads the entire row into
    memory!


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

Using (Atlas) dynamic columns creates wide rows in Cassandra, because every dynamic column key and value are stored
with the same row key. This may add limits to the scalability of the data, because all data for a single row key will
be stored on a single machine in the cluster.

Generally, we recommend that row sizes are kept in the tens of MBs at most, and also below one million dynamic column
keys.

Appendix: Sample Query Implementations
--------------------------------------

We present possible implementations of some of the queries (or generalisations of them) described above.
Note that an implementation of query 1 has been included in the "Retrieval" section.

Query 3 (Size Lower to Upper, Limit N)
======================================

Assume the existence of variables ``lower`` and ``upper`` (inclusive bounds for the size range; 10 and 15 in the
sample query), ``person`` (the person to run the query for) and ``limit`` (the maximum number of records we want to
return).

.. code:: java

    TodoTable todoTable = TodoSchemaTableFactory.of().getTodoTable(tx);

    // Note that a column range selection is a start-inclusive end-exclusive range, so using (upper, Long.MAX_VALUE)
    // as the end of the column selection is incorrect, because it excludes that specific dynamic column key.
    BatchColumnRangeSelection selection = BatchColumnRangeSelection.create(
            TodoTable.TodoColumn.of(lower, Long.MIN_VALUE).persistToBytes(),
            TodoTable.TodoColumn.of(upper + 1, Long.MIN_VALUE).persistToBytes(),
            limit)

    Map<TodoTable.TodoRow, BatchingVisitable<TodoTable.TodoColumnValue>> results =
            todoTable.getRowsColumnRange(ImmutableList.of(TodoTable.TodoRow.of(person)), selection);

    List<Todo> result = Lists.newArrayList();
    BatchingVisitable<TodoTable.TodoColumnValue> visitable = Iterables.getOnlyElement(results.values());
    visitable.batchAccept(limit, item -> {
        item.forEach(columnValue -> result.add(ImmutableTodo.of(columnValue.getValue())));
        return result.size() < limit;
    });
    return result.subList(0, limit); // The batch hint may not always be respected exactly.

Query 6 (Size lowerSize to upperSize, Cost lowerCost to upperCost)
==================================================================

Assume the existence of variables ``lowerSize``, ``upperSize``, ``lowerCost`` and ``upperCost`` which are inclusive bounds for the
size and cost ranges respectively. We also assume the existence of ``person`` (the person to run the query for).

.. code:: java

    TodoTable todoTable = TodoSchemaTableFactory.of().getTodoTable(tx);
    Map<TodoTable.TodoRow, BatchingVisitable<TodoTable.TodoColumnValue>> results =
            todoTable.getRowsColumnRange(
                    ImmutableList.of(TodoTable.TodoRow.of(person)),
                    BatchColumnRangeSelection.create(
                            TodoTable.TodoColumn.of(lowerSize, lowerCost).persistToBytes(),
                            TodoTable.TodoColumn.of(upperSize, upperCost + 1).persistToBytes(), // end is exclusive
                            100));

    List<Todo> result = Lists.newArrayList();
    BatchingVisitable<TodoTable.TodoColumnValue> visitable = Iterables.getOnlyElement(results.values());
    visitable.batchAccept(100, item -> {
        item.forEach(columnValue -> {
            // needed to ignore values with an intermediate size that are not in the cost range
            if (columnValue.getColumnName().getMonetaryCost() >= lowerCost &&
                    columnValue.getColumnName().getMonetaryCost() <= upperCost) {
                result.add(ImmutableTodo.of(columnValue.getValue()));
            }
        });
        return true;
    });
    return result;

Query 8 (Smallest for Multiple Row Keys)
========================================

Assume the existence of a Set of Strings, ``personSet``. This corresponds to person identifiers.

.. code:: java

    Map<String, List<Todo>> results = Maps.newConcurrentMap();
    Map<String, Long> smallestSizes = Maps.newConcurrentMap();
    TodoTable todoTable = TodoSchemaTableFactory.of().getTodoTable(tx);
    Map<TodoTable.TodoRow, BatchingVisitable<TodoTable.TodoColumnValue>> visitables =
            todoTable.getRowsColumnRange(
                    personSet.stream()
                            .map(TodoTable.TodoRow::of)
                            .collect(Collectors.toList()),
                    // This MUST be unbounded because each person could have a different size of smallest task
                    BatchColumnRangeSelection.create(
                            PtBytes.EMPTY_BYTE_ARRAY,
                            PtBytes.EMPTY_BYTE_ARRAY,
                            100)); // Magic number; it's hard to prescribe a one-size-fits-all value here.

    visitables.entrySet().forEach(entry -> {
        String person = entry.getKey().getPerson();
        results.put(person, Lists.newArrayList());
        entry.getValue().batchAccept(100, columnValues -> {
            if (columnValues.isEmpty()) {
                return false;
            }

            // Triggers on the first batch.
            if (!smallestSize.containsKey(person)) {
                // This suffices, even if the first batch has multiple task sizes, because dynamic columns are
                // returned in sorted order.
                smallestSize.put(person, columnValues.get(0).getColumnName().getTaskSize());
            }

            long smallestSize = smallestSizes.get(person);
            columnValues.stream()
                    .filter(columnValue -> columnValue.getColumnName().getTaskSize() == smallestSize)
                    .forEach(columnValue -> results.get(person).add(ImmutableTodo.of(columnValue.getValue())));

            // If the last entry doesn't have the smallest size, we must have covered all of the smallest todos
            // in this batch. This works, because dynamic columns are returned in sorted order.
            return columnValues.get(columnValues.size() - 1).getColumnName().getTaskSize() == smallest;
        });
    });
    return results;
