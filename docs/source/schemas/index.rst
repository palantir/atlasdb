=======
Schemas
=======

.. toctree::
   :maxdepth: 1

   tables_and_indices
   streams
   cleanup_tasks
   physical_cleanup


What to Expect
==============

This guide should give you the basics on:

1. How an AtlasDB schema file is organized.
2. What commands are available.
3. When they can be used and what they do.
4. How they translate to the underlying database.

Background
==========

Data in AtlasDB is stored as a series of key-value pairs, structured
into tables, grouped into namespaces. An AtlasDB **schema** defines a
set of named tables, and for each **table** defines the key-value pairs
which can be stored, the kind of operations which can be performed, and
any relations between tables which should be maintained. A **namespace**
is a named instantiation of a schema in the database. A table whose data
is explicitly derived from another table in the schema is called an
**index**. Indexes defined in a schema can have some of the
synchronization between the parent table and the index managed
automatically.

Each key-value pair in a table is actually specified by a triple: a
**row** part (potentially made up of several components), a **column**
part (potentially made up of several components), and a **value** part
which together form the value. The row and column together make up a
**cell**, which is always associated with a single value, i.e, within a
table, a cell forms a primary key. Puts and Gets on the table are done
in terms of cells; a Put of an existing cell overwrites the existing
cell and can be thought of as an update to that cell. A table is said to
contain a given row if there is at least one cell with the given row.

The definition of a key-value pair as a row-column-value triplet allows
for two different kinds of column layouts for a table, called "named"
columns and "dynamic" columns. In a **named column** layout, the columns
come from a pre-defined set of string labels, and the type of the value
can vary based on the column. In a **dynamic column** layout, the (sole)
column type is a tuple of values with pre-defined types. Every value in
a table with dynamic columns has the same type. One way to compare the
two layouts is that the named layout is akin to a SQL table with
primary-key and multiple columns, while the dynamic layout is akin to a
one-key-to-many-rows SQL map. Another way to compare the two layouts is
that both layout have defined rows and columns, but the named column
layout allows a row to have a subset of the named columns, whereas the
dynamic layout allows a row to have more than one full column set.

There are four main operations which can be conducted against an AtlasDB
table: get, getRange, put, and delete. The **get** operation retrieves
all cell-value pairs with the specified row values. The **getRange**
operation retrieves all cell-value pairs whose row values either fall
between two specified rows or begin with the specified prefix. The
**put** operation adds or overrides cells with new values as specified
by the operation arguments, updating any indexes needed. The **delete**
operation removes the specified cell-value pairs from the table.

Sample Schema File
==================

.. code:: java

    public class BlankSchema {
        private BlankSchema() {
            //private
        }

        private static final Schema SCHEMA = generateSchema();

        private static Schema generateSchema() {
            // Define the prefix and package for generated code
            Schema schema = new Schema("BlankSchema", "com.palantir.atlasdb.blankschema");

            /* Schema definition start */
            schema.addTableDefinition("TableName", new TableDefinition() {{
                ...
            }});

            schema.addIndexDefinition("IndexName", new IndexDefinition() {{
                ..
            }});
            /* Schema definition end */

            schema.validate() // ensure that the schema as constructed is valid.
            return schema;
        }

        public static void main (String[] args) {
            // generate the java classes, and write them to the specified source folder.
            SCHEMA.renderTablesWithNamespace(new File("src"), Namespace.create("pt_met"));
        }
    }

Contrary to the standard SQL format, an AtlasDB schema is written in
java, in a java class, as a sequence of calls to a schema builder and
anonymous inner classes. The java class is then run, which generates
java classes for creating, accessing, and manipulating these tables.

Value Types
-----------

AtlasDB understands three kinds of data: primitive ValueTypes, protocol
buffer GeneratedMessages, and palantir Persistables.

Primitive ValueTypes
~~~~~~~~~~~~~~~~~~~~

For simple data types, AtlasDB represents them by ValueType. Note that a
java primitive type can have multiple ValueTypes associated with it.
Each ValueType represents a different method of storing that type in the
database, and thus affects storage efficiency and search
characteristics, among others. The types are:

+-------------------------------+--------------+---------------+----------------------+----------------+
| Name                          | Java Type    | Format        | Anywhere in table?   | Range Scans?   |
+===============================+==============+===============+======================+================+
| FIXED\_LONG                   | long         | byte[8]       | YES                  | YES            |
+-------------------------------+--------------+---------------+----------------------+----------------+
| FIXED\_LONG\_LITTLE\_ENDIAN   | long         | byte[8]       | YES                  | NO             |
+-------------------------------+--------------+---------------+----------------------+----------------+
| NULLABLE\_FIXED\_LONG         | long         | byte[9]       | YES                  | YES            |
+-------------------------------+--------------+---------------+----------------------+----------------+
| VAR\_LONG                     | long         | byte[len]\*   | YES                  | YES            |
+-------------------------------+--------------+---------------+----------------------+----------------+
| VAR\_SIGNED\_LONG             | long         | byte[len\*]   | YES                  | YES            |
+-------------------------------+--------------+---------------+----------------------+----------------+
| UUID                          | UUID         | byte[16]      | YES                  | YES            |
+-------------------------------+--------------+---------------+----------------------+----------------+
| SHA\_256\_HASH                | Sha256Hash   | byte[32]      | YES                  | YES            |
+-------------------------------+--------------+---------------+----------------------+----------------+
| STRING                        | String       | byte[]        | NO\*\*               | YES            |
+-------------------------------+--------------+---------------+----------------------+----------------+
| VAR\_STRING                   | String       | byte[len]     | YES                  | NO             |
+-------------------------------+--------------+---------------+----------------------+----------------+
| BLOB                          | byte[]       | byte[]        | NO\*\*               | YES            |
+-------------------------------+--------------+---------------+----------------------+----------------+
| SIZED\_BLOB                   | byte[]       | byte[len]     | YES                  | NO             |
+-------------------------------+--------------+---------------+----------------------+----------------+

note: \* var long: the smaller the value, the fewer bytes used. For
signed, the closer to zero, the fewer bytes used. negative values for
VAR\_LONG are always 10 bytes. 0 to 127 are 1 byte for VAR\_LONG. -64 to
63 are 1 byte or SIGNED\_VAR\_LONG

note: \*\* can only be a row or column component if it is the last
component of the component list.

Note that all long data types are signed, but VAR\_SIGNED\_LONG is
encoded in a manner which stores negative numbers more efficiently than
VAR\_LONG. (The latter will always use 10 bytes to encode negative
values, but the former will not.)

Protobufs and Persistables
~~~~~~~~~~~~~~~~~~~~~~~~~~

For protobufs and persistables, AtlasDB will handle persisting and
hydrating objects to and from byte arrays. Otherwise, they function
similarly to ValueType.BLOB. However, for structured data it is
recommended that you store them as protobufs or persistables rather than
BLOBs - this is because AtlasDB can then extract component values from
the structs to create indexes.

Schema Objects
--------------

The AtlasDB schema contains four kinds of top level objects - tables,
indexes, stream stores, and cleanup tasks.

-  **Tables** are the base structure for storing information in AtlasDB.
   Similar to SQL tables in idea, with base layout described in the
   section above.
-  **Indexes** are tables which explicitly base their content off of a
   parent table. They are useful for optimizing access patterns on a
   table.
-  **Stream Stores** store large/oversized binary data. They are similar
   to SQL overflow/LOB stores.
-  **Cleanup Tasks** are useful for cleaning up tables when the data
   they reference is deleted. Similar to ON DELETE CASCADE in SQL.

In this section, we'll give you a overview of what each object does and
how to add it to a schema. Stream stores and CleanupTasks will have all
definitions explained in detail, but table/index definition will be
covered in the following section (Table/Index Definition) as it's a much
more complicated monster.

NOTE: AtlasDB does not try to fully replicate a SQL language - many of
the high-level constructs (cascades, joins, etc.) are not implemented.
This is because we practically don't have the time to replicate every
conceivable feature and philosophically want to be more explicit with
our performance than with SQL queries. Things will not map 1:1 with SQL.
