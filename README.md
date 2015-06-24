# AtlasDB
Atlas is a transactinal layer on top of a key value store.  When designing a
data store to be scalable, transactions are usually the first feature to be
cut.  However they are one of the most useful features for developers.  Atlas
allows any key value store that supports durable writes to have transactions.
Once you have transactions, indexes are also easy to add because updates to
rows can just update the indexes and include thoese wites in a single
tranaction.  Provided are KeyValueService implmentations of LevelDb and
Cassandra, but any data store worth its salt should make a fine storage layer.

#Consistency
Atlas uses classic MVCC and supports Snapshot Isolation and
Serializable Snapshot Isolation.  SI must keep track of the write set in memory
to detect write/write conflicts.  SSI must also keep the read set in memory to
detect read/write conflcits.  This means that write transactions are expected
to be reasonably short lived.  Read-only transactions are allowed to run for
longer and will never conflcit with other transactions.

#Schemas
Atlas can store keys anv values using atlas-api directly, but using
Schemas can get you type safely, more readable code and remove a lot of
boilerplate serialization code.  Atlas contains a java DSL to define schemas
and a proto representation that gets stored in the K/V store that can be used
by other tools to inspect the values.  Schema tables can be rendered to java
classes to easily access tables and keep indexes up to date given the schema.

#Shell
Since everything is now stored efficiently as encoded byte arrays, inspecting
the data becomes hard.  Atlas Shell is a ruby command line that can inspect the
schema for each table and display the values in a readable way.  It can even
parse the stored protobufs values because the proto description for each column
is stored in the schema metadata.
