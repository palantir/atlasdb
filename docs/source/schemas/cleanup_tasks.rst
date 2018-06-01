.. _cleanup_tasks:

=============
Cleanup Tasks
=============

Cleanup tasks allow AtlasDB to essentially implement ON DELETE CASCADE.
If there is an index that marks usage of a shared resource, effectively
reference counting this resource, then a cleanup task can be used to see
if all references are gone and remove the resource if it is no longer
needed. The original intent of clean up tasks was to clean up shared
streams when doing a hard delete of media that used these shared
streams.

.. code:: java

    public void addCleanupTask(String tableName, OnCleanupTask task);
    public void addCleanupTask(String tableName, Supplier<OnCleanupTask> task);

The table name is the (database) name of the table for which the table
should be applied. The OnCleanupTask will rely on generated code, and
therefore should be implemented in a separate class from the schema
class.
