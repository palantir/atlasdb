.. _tables-and-indices:

==================
Tables and Indices
==================

.. contents::
   :local:
   :depth: 1

Tables
======

Tables are the base structure for storing data in AtlasDB. Every table
has a name, a row specification, a column-value specification, an
optional set of constraints on the table, and an optional set of
behavior and performance tuning parameters.

There is one main way to add a table to the schema, along with two
variants.

.. code:: java

    schema.addTableDefinition("table_name_here", new TableDefinition() {{
        javaName("JavaTableName"); //optional
        rowName();
            ...
        columns(); //or dynamicColumns();
            ...
        constraints(); //optional section
            ...
        ... //behavior/perf options
    }});

The ``addTableDefinition`` method takes two arguments: the name of the
table to be used in the key-value store itself, and a table definition.
The table name should be specified in snake\_case. The details of the
TableDefinition initialization will be covered in the "Table/Index
Definition" section.

If there are multiple tables which will have the same definition but
will have different names, the first variation of the table definition
can be used:

.. code:: java

    schema.addDefinitionForTables(ImmutableSet.of("table1_name", "table2_name"), new TableDefinition() {{
        ...
    }});

If the table should not exist for more than a transaction and is
primarily to conserve server memory, then a table can be explicitly
declared a temp table using the second variation:

.. code:: java

    schema.addTempTableDefinition("temp_table_name", new TableDefinition() {{
        ...
    }});

The AtlasDB developers however strongly recommend against usage of this
form, since they have not found it to be particularly useful in making
AtlasDB queries, and thus have never used it themselves, and thus have
never tested to see if it actually works.

Indices
=======

.. role:: strike

A common pattern in database schemas is to define an index table whose
values are derived from and kept in sync with values from a base table.
In standard RDBMS's these are user-defined and db-managed, but AtlasDB
:strike:`is not so full-featured` requires you to think more carefully
about performance.

There are two kinds of indices which can be defined in AtlasDB: additive
and cell-referencing. Both use the dynamic columns layout. For additive
indices, each cell in the index is derived from a unique one row in the
base table. For cell-referencing indices, each cell in the index is
derived from a unique cell (not row of cells) in the base table. For
more complicated index situations (e.g. indices whose rows are derived
from multiple tables) a regular table must be defined for the index, and
synchronization between the base table(s) and index must be done
manually.

.. code:: java

    schema.addIndexDefinition("index_name_here", new IndexDefinition(IndexType.ADDITIVE /* or .CELL_REFERENCING */) {{
        onTable("base_table_name");
        onCondition("source_column", " /* java boolean expression */ _value > 100 "); //optional
        rowName();
            ...
        dynamicColumns(); //or noColumns();
            ...
        ... //behavior/perf options
    }});

Note that, in the case where the index should only get a row from the
base table if some condition is met, the ``onCondition`` clause can be
added to the index definition. The value of the cell with the specified
column is accessed by the ``_value`` term.

If multiple indices should be defined for the same index definition,
then the following variant can be used:

.. code:: java

    schema.addAdditiveIndexesForDefinition(ImmutableSet.of("index1_name", "index2_name"), new IndexDefinition(...) {{
        ...
    }});

The AtlasDB Developers however strongly recommend against usage of this
form, since they have not found it to be particularly useful in making
AtlasDB queries, and thus have never used it themselves, and thus have
never tested to see if it actually works.

Additive
--------

The components of a cell in an additive index can reference any cells
for each row of the base table. Insertions of new rows to the base table
and updates to existing rows in the base table will automatically
trigger updates to the values in the index tables. Deletes to the base
table however are not cascaded to the index tables, and must be done
manually. Manual additions, updates, and deletes to an additive index
table do not trigger actions on the base table. Additive indices will
have a "\_aidx" suffix added to their index names.

Cell-Referencing
----------------

The components of a cell in a cell-referencing index can only reference
cells from a schema-specified column for each row of the base table.
Insertions and updates of cells to the base table with the correct
column will automatically trigger insertions/updates to corresponding
cells in the index table. Deletes to the base table are also cascaded to
the index table. Manual additions, updates, and deletes to the
cell-referencing table do not trigger actions on the base table.
Cell-referencing indices have a "\_idx" suffix added to their index
names.

Note however that this automatic management comes at a performance cost:
Writing to a table performs a read from each cell-referencing index (as
well as a write) to determine what deletions need to be performed on the
index. This read happens synchronously. This can cause writes (which are
otherwise asynchrounous and batched) to be particularly expensive in
tables with cell-referencing indices.

Regular tables
--------------

Technically there exists a third type of index, which is simply a
regular table with data derived from another table. In this case the row
name is a tuple composed of the field on which you're indexing and the
primary key (i.e., row name) of the table from which the index table is
derived. To look something up, the client can simply do a range scan of
rows in the index the first component of which is what it's looking for.
The client then gets back the row names that include that row component,
and from there it can look at the remaining components (typically some
sort of ID) to find what it's looking for.

The disadvantage of creating such an index is that inserts, updates, and
deletes to the table from which the index table is derived all have to
be accounted for manually with extra logic.

However, there is an advantage to indexing this way: The client can page
through the results, whereas additive or cell-referencing indices have
dynamic columns by default, forcing the client to get all of the results
at once. This may or may not be an issue depending on the amount of data
being stored in a given table.

Table/Index Definitions
=======================

The ``TableDefinition`` and ``IndexDefinition`` is often created as an
anonymous class using "double-brace initialization", which allows for
more readable code than a conventional builder. Certain initialization
methods, such as ``javaName()``, can be called at any time in the
method, while others, such as ``column()``, need to be preceded by a
"state transition" command, such as ``columns()``. By convention, such
definitions are broken down into the following sections:

-  **Definition Parameters** such as ``javaName()`` define basic
   properties of tables and indexes and are placed at the beginning.
-  **Row Definitions** such as ``rowComponent()`` define the rows of the
   table. The section is begun with a ``rowName()`` call. A table must
   define at least one row component through these methods to be valid.
-  **Named Column Definitions** such as ``column()`` define the named
   columns of the table. The section is begun with a ``columns()`` call.
   A table can have a named column section or a dynamic column section,
   but not both.
-  **Dynamic Column Definitions** such as ``columnComponent()`` define
   the dynamic columns of the table. The section is begun with a
   ``dynamicColumns()`` call. A table can have a named column section or
   a dynamic column section, but not both.
-  **Enabling the V2 Table API** by setting the ``enableV2Table()`` flag.
   This would generate an additional table class with some easy to use functions such as
   ``putColumn(key, value)``, ``getColumn(key)``, ``deleteColumn(key)``.
   We only provide these methods for named columns, and don't currently support dynamic columns.
-  **Constraint Definitions** such as ``tableConstraint()`` define
   constraints on the table (such as foreign key relations). The section
   is begun with a ``constraints()`` call. This section is optional.
-  **Behavioral Parameters** such as ``conflictHandler()`` define the
   behavior of the table during run-time. This includes allowed queries,
   performance optimizations, and concurrency strategies, among others.
   This section is usually at the end.

Definition Parameters
---------------------

.. code:: java

    public void javaTableName(String name);

This method specifies the name of the table to be used in generated java
code for the schema. It should be specified in CamelCase and be as long
as descriptive as is useful. If this method is not called, the value
will be derived by converting the table's AtlasDB name from snake\_case
to CamelCase.

Logging Parameters
~~~~~~~~~~~~~~~~~~

.. code:: java

    public void tableNameLogSafety(LogSafety logSafety);

If called, this marks the table name as either safe or unsafe
for logging, depending on the argument passed. When AtlasDB logs
a table reference for this table, this will be logged as a ``SafeArg``
or ``UnsafeArg`` respectively, following the Palantir
`safe-logging <https://github.com/palantir/safe-logging>`__ library.

If this is not specified, the table name defaults as UNSAFE.

.. code:: java

    public void namedComponentsSafeByDefault();

If called, then row components and named columns that are subsequently
defined for this table will be assumed to be safe for logging unless
specifically indicated as unsafe. By default, row components and named
columns are assumed unsafe unless specifically indicated as safe.

Note that specifying named components as safe by default does not
also make the table name considered safe.

.. code:: java

    public void allSafeForLoggingByDefault();

If called, this marks the table name as safe, and all named components as
safe unless they are explicitly marked as unsafe. Note that an exception
will be thrown if this method is called alongside ``tableNameLogSafety(LogSafety.UNSAFE)``;
to achieve that effect (table names unsafe, but all row/column components safe),
please use ``namedComponentsSafeByDefault()`` instead.

Index-specific Parameters
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: java

    public void onTable(String name);

This method specifies the AtlasDB name of the table which this index
definition will derive its data from. This method is required for all
IndexDefinitions.

.. code:: java

    public void onCondition(String sourceColumn, String booleanExpression);

Optional parameter. Specifies that only rows which satisfy the specified
boolean expression on the specified source column will be added to the
index. The source column must be a valid component name from the source
table, and the boolean expression must be a valid java expression, with
``_value`` denoting the value of the source column.

Row Definitions
---------------

Each row is uniquely identified by its ``rowName``. Each ``rowName`` is
composed of at least one ``rowComponent``. Therefore each row is
uniquely identified by the permutation of its ``rowComponent`` values.
Order matters. For example,

.. code:: java

    rowName();
        rowComponent("object_id",           ValueType.FIXED_LONG);
        rowComponent("group_id",            ValueType.VAR_LONG); partition(GROUP_PARTITIONER);
        rowComponent("fragment_version_id", ValueType.VAR_LONG);

This means that each row in this table is uniquely identified by a
3-tuple consisting of an object ID, a group ID, and a fragment version
ID.

Only the last ``rowComponent`` of a ``rowName`` can be set to
``ValueType.STRING`` or to ``ValueType.BLOB`` because values of these
types do not explicitly or implicitly track their own size. If you need
a ``rowComponent`` other than the last one to be a string or a byte
array, use ``ValueType.VAR_STRING`` or ``ValueType.SIZED_BLOB`` instead.
See the ValueTypes section for more information

.. code:: java

    public void rowComponent(String componentName, ValueType valueType, ValueByteOrder valueByteOrder = ValueByteOrder.ASCENDING);

By default, all rows are stored in ascending byte order. This means
range results are iterated in ascending order. If you need to access
rows in reverse order, then adding the ``ValueByteOrder.DESCENDING``
argument will store them in descending order instead.

.. code:: java

   public void rowComponent(String componentName, ValueType valueType, ValueByteOrder valueByteOrder, LogSafety logSafety = LogSafety.UNSAFE);

You may also identify a row component as being explicitly safe or unsafe
for logging. (If this is not specified it defaults to unsafe, or safe if
the table was set to default components as being safe.)

.. _tables-and-indices-partitioners:

Partitioners
~~~~~~~~~~~~

Each row component, after being defined, may then have a partitioner
specified on them. The partitioner is responsible for split the space of
possible values for each row component into a number of ranges. Rows are
then partitioned according to the row component ranges; rows which fall
into the same partition are stored in the same server shard. Performance
is optimized by putting rows often accessed together in the same
partition, while spreading rows equally across all partitions.

.. code:: java

    public void partition(RowNamePartitioner... partitioners);

    public ExplicitRowNamePartitioner explicit(String... componentValues);
    public ExplicitRowNamePartitioner explicit(long... componentValues);
    public UniformRowNamePartitioner uniform();

By default, all row components use ``partition(uniform())``. If however,
certain values are certain to be stored/access very often (the group ids
of the objects, in the above example), they can have partitions
explicitly created for them by specifying ``explicit(...)``. Note that
use of ``partition()`` assumes the order storage of rows; if there is no
good way to partition the rows uniformly and range requests are not
needed, then hashing the first (or first-N) row components of
your table would likely be a good idea.

.. warning::
   The most significant component of any
   table is used by the partitioner to distribute data across the cluster.
   To avoid hot-spotting, the type of the first row component should NOT be
   a VAR\_LONG, a VAR\_SIGNED\_LONG, or a SIZED\_BLOB.

For a safe data distribution the usage of ``hashFirstRowComponent()`` is suggested.

.. code:: java

    rowName();
        hashFirstRowComponent();
        rowComponent("secondary_row_component_of_any_type", ValueType.VAR_LONG);

Also, in the event that the first row component may not be sufficient for even
distribution (e.g. it has low cardinality and an uneven distribution, but subsequent
components are more varied), AtlasDB also offers hashing a prefix of the row key, via
``hashFirstNRowComponents(int)``. This is useful, for instance, in stream stores.

.. code:: java

    rowName();
        hashFirstNRowComponents(2);
        rowComponent("first_component_not_evenly_distributed", ValueType.VAR_LONG);
        rowComponent("second_component_fairly_distributed", ValueType.UUID);
        rowComponent("third_component_maybe_expensive_to_hash", ValueType.BLOB);

This will prepend a hash of the first and second components of each row key to
the table.

Table Named Columns
-------------------

For tables using the named columns layout, the column name and value
type referenced by each column is specified by a single command.

.. code:: java

    public void column(String columnName, String shortName, ValueType valueType)
    public void column(String columnName, String shortName, Class<?> protoOrPersistable, Compression compression = Compression.NONE)
    public void column(String columnName, String shortName, Class<?> protoOrPersistable, Compression compression, LogSafety logSafety = LogSafety.UNSAFE)

The column name is the name of the column that will be used in the
generated java code and table metadata. The short name is a one or two
character label which will be the actual name for the column when stored
in AtlasDB. Any ValueType may be used as the value for a column, as well
as any protobuffer class or Persistable. AtlasDB will handle serializing
and deserializing the proto/persistable to and from its byte array
representation, and will optionally also compress the byte array to save
space using the method you specify. Columns can not be overloaded with
multiple types - each ``column()`` call must contain unique column names
and short names.

Also, you may explicitly identify the name of this column to be safe or
unsafe for logging. We don't currently support having different safety
levels for the column name and the short name.
(If this is not specified it defaults to unsafe, or safe if
the table was set to default components as being safe.)

If instead you don't need a row to have multiple columns and all
table information can be encapsulated in the row components, then the
section can instead be specified with ``noColumns()``, which defines the
table to contain a single column "exists" with short name "e" and value
type VAR\_LONG (always zero).

Table Dynamic Columns
---------------------

For dynamic columns, the name-value components that make up the column
and the value type referenced by columns are specified by separate
commands.

.. code:: java

    public void columnComponent(String componentName, ValueType valueType, ValueByteOrder byteOrder = ValueByteOrder.ASCENDING)

Each column component is made up of a component name (specified in
snake\_case), a value type, and optionally a byte ordering. Column
components for dynamic columns must be primitive ValueTypes which can be
partitioned and ordered. The order of the column component calls is the
order in which the components will be stored together. Since dyanmic
columns of a row are retrieved in byte order, this means column
component ordering determines sort ordering for retrieval.

.. code:: java

    public void value(ValueType valueType)
    public void value(Class<? extends GeneratedMessage> proto, Compression compression = Compression.NONE)

Every dynamic column will also have a value associated with it, which
can be a primitive ValueType or protobuf (optionally compressed).

If values are not needed for the table, specifying
``value(ValueType.VAR_LONG)`` and ``maxValueSize(1)`` is conventional.
The max value size command is a performance hint for AtlasDB.

Index Rows and Columns
----------------------

Indices are a little more constrained in their definition than tables,
since their components must be primitive value-types derived from the
pre-existing components of a table. All index definitions also do not
get a choice between named and dynamic column types: If the index is
defined with columns, then it is dynamic; if defined without columns,
this it is named with an implicit ``noColumns()``.

Both the ``rowName()`` and ``dynamicColumns()`` sections use the same
methods to define their components:

.. code:: java

    public void componentFromRow(String componentName,
                                 ValueType valueType,
                                 ValueByteOrder byteOrder = ValueByteOrder.ASCENDING,
                                 String sourceComponentName = componentName);

    public void componentFromColumn(String componentName,
                                    ValueType valueType,
                                    ValueByteOrder byteOrder = ValueByteOrder.ASCENDING,
                                    String sourceColumnName = componentName,
                                    String codeToAccessValue);

    public void componentFromDynamicColumn(String componentName,
                                           ValueType valueType,
                                           ValueByteOrder byteOrder = ValueByteOrder.ASCENDING,
                                           String sourceComponentName = componentName);

All components define a component name, value-type, byte-order, which
defaults to ascending if unspecified, and component name of the source
row/column component, which by default is assumed to be the same as the
component name specified earlier. Note that for cell-referencing
indexes, all index components derived from columns need to reference the
same column.

For components being derived from named columns, an additional "code to
access value" argument is required. This argument allows value-type
index components to be extracted from more complicated protobuf or
serializable column components. The argument must be a valid java source
code expression, where ``_value`` is the value of the table component.

For the row definitions section, each ``componentFromRow`` call can be
succeeded by a ``partitioner()`` call, in the exact same manner as for
table rows. For more information, see the Partitioners subsection of
Table Rows.

.. note::

    Internally, index rows are stored including a reference to the source column,
    but this is stripped out in the generated code before being returned to the user.
    Thus, if one uses a ``List`` of results returned from an index table (e.g. through ``getRowColumns``,
    one may encounter multiple values that appear to be the same). The standard workaround is to use
    a ``Set`` to deduplicate the results.

    Please see discussion on `issue 604 <https://github.com/palantir/atlasdb/issues/604>`__ for more details
    regarding this behaviour.

Constraints
-----------

Sometimes the set of valid values for a table is smaller than the set of
valid values specified by just type information. In these cases, it can
be useful to explicitly express these constraints when defining the
tables to ensure that code written against these tables do not violate
them. The AtlasDB schema allows three different types of constraints to
be defined: Foreign key constraints, table constraints, and row
constraints. Note however, that these constraints are used mostly for
staging and debugging environments only and will usually not be enabled
in production due to their sometimes prohibitive performance costs.

.. code:: java

    public void foreignKeyConstraint(ForeignKeyConstraingMetadata constraint);
    public void tableConstraint(TableConstraint constraint);
    public void rowConstraint(RowConstraintMetadata constraint);

-  **Foreign Key Constraints** reach across tables and specify that
   certain components must have matching values with components from
   another table.
-  **Table Constraints** affect the whole table and at present define
   immutability constraints for those tables. See the javadoc for
   TableConstraint for details.
-  **Row Constraints** define constraints for which each table row must
   satisfy. These operations can be specifying that certain components
   must be nonnegative, or that certain row components and column
   components must contain the same value.

Behavioral Parameters
---------------------

.. code:: java

    public void conflictHandler(ConflictHandler handler = ConflictHandler.RETRY_ON_WRITE_WRITE);

The conflict handler parameter specifies the MVCC transaction semantics
for the table.

.. code:: java

    public void cachePriority(CachePriority priority = CachePriority.WARM);

Specifies the retention policy for caching AtlasDB queries and their
results. Values are **COLDEST, COLD, WARM, HOT, HOTTEST.** The hotter
the setting, the more queries and the longer they are stored.

.. code:: java

    public void dbCompressionRequested();

Cassandra only - specifies whether the table should be stored
compressed.

.. code:: java

    public void rangeScanAllowed();

Specifies whether a table should be allowed to have range scans
conducted on its rows.

.. note::
   If this option is not selected, you will
   not be able to use the **getRange** operation against your table!

.. code:: java

    public void negativeLookups();

Cassandra only - if certain queries are expected to regularly search for
non-existent rows, this will have cassandra create bloom filters on the
rows to speed up the search.

.. code:: java

    public void maxValueSize(int size);

Performance hint - specifies the size in bytes of the largest value
which any given row in the table may hold.
